"""
neurohub.core.loader - динамический загрузчик модулей.

Сканирует ./modules/*/manifest.json, импортирует класс каждого модуля,
подключает его к шине и управляет жизненным циклом.

Добавление нового модуля:
    1. Создать ./modules/my_module/
    2. Добавить manifest.json (см. ModuleManifest)
    3. Добавить __init__.py с классом, наследующим BaseModule
    4. Перезапустить

Обнаружение модулей НЕ требует изменений в коде ядра.
"""

from __future__ import annotations

import importlib
import json
from pathlib import Path
from typing import TYPE_CHECKING

import structlog
from pydantic import BaseModel, ValidationError

from neurohub.core.module import BaseModule

if TYPE_CHECKING:
    from neurohub.core.bus import MessageBus

log = structlog.get_logger(__name__)


class ModuleManifest(BaseModel):
    """
    Схема manifest.json в папке каждого модуля.


    name:           Уникальный идентификатор модуля. Должен совпадать с BaseModule.name.

    version:        Семантическое версионирование, например "1.0.0".

    subscriptions:  Топики, на которые модуль подписывается при загрузке.
                    Принимает значения MessageType ("COMMAND") и имена модулей ("planner").
                    Wildcard "*" разрешен, но использовать с осторожностью!

    entry:          Путь импорта Python к классу модуля, относительно пакета neurohub.
                    По умолчанию: "modules.<name>" -> класс "<Name>Module".

    enabled:        Поставить false, чтобы пропустить загрузку без удаления папки модуля.
    """

    name: str
    version: str = "1.0.0"
    subscriptions: list[str] = []
    entry: str = ""  # Если пусто, то переопределяется автоматически
    enabled: bool = True


class ModuleLoader:
    """
    Сканирует директорию в поисках модулей, загружает их и управляет жизненным циклом.

    Использование:
        loader = ModuleLoader(bus, modules_dir=Path("neurohub/modules"))
        await loader.load_all()
        ...
        await loader.load("my_module")
        await loader.unload("my_module")
        ...
        await loader.unload_all()
    """

    def __init__(self, bus: MessageBus, modules_dir: Path) -> None:
        self._bus = bus
        self._modules_dir = modules_dir
        # name: загруженный экземпляр модуля
        self._loaded: dict[str, BaseModule] = {}

    # =============
    # Публичное API
    # =============

    async def load_all(self) -> None:
        """Найти и загрузить все включенные модули из modules_dir."""
        manifests = self._discover()
        for manifest in manifests:
            await self._load_one(manifest)

    async def unload_all(self) -> None:
        """Остановить все загруженные модули в порядке, обратном загрузке."""
        for name in reversed(list(self._loaded.keys())):
            await self._unload_one(name)

    async def load(self, name: str) -> None:
        """
        Загружает один подуль по имени.

        Если модуль уже загружен, логгируем предупреждение.
        """
        if name in self._loaded:
            log.warning("loader.already_loaded", name=name)
            return

        manifest_path = self._modules_dir / name / "manifest.json"
        if not manifest_path.exists():
            log.warning("loader.manifest_not_found", name=name)

        manifest = self._parse_manifest(manifest_path)
        if manifest:
            await self._load_one(manifest)

    async def unload(self, name: str) -> None:
        """
        Выгружает один модуль по имени.
        """
        if name not in self._loaded:
            log.warning("loader.not_loaded", name=name)
            return

        await self._unload_one(name)

    async def reload(self, name: str) -> None:
        """Перезапускает модуль (выгружает, а затем заного загружает его)"""
        await self.unload(name)
        await self.load(name)

    @property
    def loaded(self) -> dict[str, BaseModule]:
        """Read-only. Копия словаря загруженных модулей."""
        return dict(self._loaded)

    # ==================
    # Внутренние методы
    # ==================

    def _discover(self) -> list[ModuleManifest]:
        """Сканирует modules_dir на наличие валидных manifest.json"""
        manifests = []

        if not self._modules_dir.exists():
            log.warning(
                "loader.modules_dir_missing", path=str(self._modules_dir)
            )
            return manifests

        for manifests_path in sorted(
            self._modules_dir.glob("*/manifest.json")
        ):
            manifest = self._parse_manifest(manifests_path)
            if manifest:
                manifests.append(manifest)

        log.info("loader.discovered", count=len(manifests))
        return manifests

    def _parse_manifest(self, path: Path) -> ModuleManifest | None:
        """Разобрать и провалидировать manifest.json. Возвращает None при любой ошибке."""
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            manifest = ModuleManifest(**raw)
        except (json.JSONDecodeError, ValidationError, OSError) as e:
            log.error("loader.manifest_invalid", path=str(path), error=str(e))
            return None

        if not manifest.enabled:
            log.debug("loader.module_disabled", name=manifest.name)
            return None

        return manifest

    async def _load_one(self, manifest: ModuleManifest) -> None:
        """Импортировать, создать, подписать и запустить один модуль."""
        if manifest.name in self._loaded:
            log.warning("loader.already_loaded", name=manifest.name)
            return

        entry = manifest.entry or f"neurohub.modules.{manifest.name}"

        try:
            module_pkg = importlib.import_module(entry)
        except ImportError as e:
            log.error(
                "loader.import_failed",
                name=manifest.name,
                entry=entry,
                error=str(e),
            )
            return

        # Найти класс модуля
        cls = self._find_class(module_pkg, manifest.name)
        if cls is None:
            log.error(
                "loader.class_not_found", name=manifest.name, entry=entry
            )
            return

        try:
            instance: BaseModule = cls(self._bus)
        except Exception as e:
            log.error(
                "loader.instantiation_falied", name=manifest.name, error=str(e)
            )
            return

        # Подключить подписки из манифеста
        for topic in manifest.subscriptions:
            instance.subscribe(topic)

        try:
            await instance.on_start()
        except Exception as e:
            log.error(
                "loader.on_start_failed", name=manifest.name, error=str(e)
            )
            # Обязательно отписаться, чтобы не осталось обработчиков-призраков
            for topic in list(instance._subscriptions):
                instance.unsubscribe(topic)
            return

        self._loaded[manifest.name] = instance
        log.info(
            "loader.loaded",
            name=manifest.name,
            version=manifest.version,
            subscriptions=manifest.subscriptions,
        )

    async def _unload_one(self, name: str) -> None:
        """Остановить и удалить один модуль"""
        instance = self._loaded.pop(name, None)
        if instance is None:
            return

        # Отписаться, чтобы не осталось обработчиков-призраков
        for topic in list(instance._subscriptions):
            instance.unsubscribe(topic)

        try:
            await instance.on_stop()
        except Exception as e:
            log.error("loader.on_stop_failed", name=name, error=str(e))

        log.info("loader.unloaded", name=name)

    @staticmethod
    def _find_class(module_pkg, name: str) -> type[BaseModule] | None:
        """Найти наследника BaseModule в загруженном пакете."""
        conventional = name.capitalize() + "Module"
        candidate = getattr(module_pkg, conventional, None)
        if (
            candidate
            and isinstance(candidate, type)
            and issubclass(candidate, BaseModule)
        ):
            return candidate

        for attr_name in dir(module_pkg):
            attr = getattr(module_pkg, attr_name, None)
            if (
                isinstance(attr, type)
                and issubclass(attr, BaseModule)
                and attr is not BaseModule
            ):
                return attr

        return None
