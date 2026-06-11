import pytest
from pathlib import Path
from neurohub.core.bus import MessageBus
from neurohub.core.loader import ModuleLoader
from neurohub.core.schema import BusMessage, MessageType


@pytest.fixture
async def bus():
    b = MessageBus()
    await b.start()
    yield b
    await b.stop()


MODULES_DIR = Path("neurohub/modules")


async def test_load_all_discovers_modules(bus):
    loader = ModuleLoader(bus, MODULES_DIR)
    await loader.load_all()

    assert "echo" in loader.loaded
    assert "kv" in loader.loaded

    await loader.unload_all()


async def test_unload_all_clears_loaded(bus):
    loader = ModuleLoader(bus, MODULES_DIR)
    await loader.load_all()
    await loader.unload_all()

    assert len(loader.loaded) == 0


async def test_echo_module_responds(bus):
    loader = ModuleLoader(bus, MODULES_DIR)
    await loader.load_all()

    msg = BusMessage(source="test", type=MessageType.COMMAND, payload={"x": 1})
    response = await bus.request(msg, timeout=2.0)

    assert response.reply_to == msg.id
    assert response.payload["echo"] == {"x": 1}

    await loader.unload_all()


async def test_kv_set_and_get(bus):
    loader = ModuleLoader(bus, MODULES_DIR)
    await loader.load_all()

    set_msg = BusMessage(
        source="test",
        target="kv",
        type=MessageType.COMMAND,
        payload={"command": "kv.set", "key": "name", "value": "NeuroHub"},
    )
    r = await bus.request(set_msg, timeout=2.0)
    assert r.payload["ok"] is True

    get_msg = BusMessage(
        source="test",
        target="kv",
        type=MessageType.COMMAND,
        payload={"command": "kv.get", "key": "name"},
    )
    r = await bus.request(get_msg, timeout=2.0)
    assert r.payload["ok"] is True
    assert r.payload["data"]["value"] == "NeuroHub"

    await loader.unload_all()


async def test_kv_get_missing_key(bus):
    loader = ModuleLoader(bus, MODULES_DIR)
    await loader.load_all()

    msg = BusMessage(
        source="test",
        target="kv",
        type=MessageType.COMMAND,
        payload={"command": "kv.get", "key": "nonexistent"},
    )
    r = await bus.request(msg, timeout=2.0)
    assert r.payload["ok"] is False
    assert "Ключ не найден" in r.payload["error"]

    await loader.unload_all()


async def test_disabled_module_not_loaded(bus, tmp_path):
    """Module with enabled=false is skipped."""
    mod_dir = tmp_path / "disabled_mod"
    mod_dir.mkdir()
    (mod_dir / "manifest.json").write_text(
        '{"name":"disabled_mod","enabled":false}'
    )

    loader = ModuleLoader(bus, tmp_path)
    await loader.load_all()

    assert "disabled_mod" not in loader.loaded


async def test_invalid_manifest_skipped(bus, tmp_path):
    """Broken manifest.json doesn't crash the loader."""
    mod_dir = tmp_path / "bad_mod"
    mod_dir.mkdir()
    (mod_dir / "manifest.json").write_text("not valid json{{{")

    loader = ModuleLoader(bus, tmp_path)
    await loader.load_all()  # должен не упасть

    assert "bad_mod" not in loader.loaded


async def test_reload_module(bus):
    loader = ModuleLoader(bus, MODULES_DIR)
    await loader.load_all()

    await loader.reload("kv")

    assert "kv" in loader.loaded

    await loader.unload_all()
