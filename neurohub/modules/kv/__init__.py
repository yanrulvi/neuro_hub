from neurohub.core.module import BaseModule
from neurohub.core.schema import BusMessage, MessageType


class KVModule(BaseModule):
    """
    Key-value хранилище в памяти.

    Слушает target="kv". Поддерживает команды: kv.set, kv.get, kv.delete, kv.list.
    Данные живут только в памяти процесса - будет заменен на NeuroMemory позже.

    Полезная нагрузка команд:
        kv.set:    {"command": "kv.set",    "key": str, "value": any}
        kv.get:    {"command": "kv.get",    "key": str}
        kv.delete: {"command": "kv.delete", "key": str}
        kv.list:   {"command": "kv.list"}
    """

    name = "kv"

    def __init__(self, bus) -> None:
        super().__init__(bus)
        self._store: dict[str, object] = {}

    async def on_message(self, msg: BusMessage) -> None:
        command = msg.payload.get("command", "")
        handler = {
            "kv.set": self._handle_set,
            "kv.get": self._handle_get,
            "kv.delete": self._handle_delete,
            "kv.list": self._handle_list,
        }.get(command)

        if handler is None:
            await self._respond(
                msg, ok=False, error=f"Неизвестная команда: {command!r}"
            )
            return

        await handler(msg)

    async def _handle_set(self, msg: BusMessage) -> None:
        key = msg.payload.get("key")
        if not key:
            await self._respond(msg, ok=False, error="Не указан key")
            return
        self._store[key] = msg.payload.get("value")
        await self._respond(msg, ok=True, data={"key": key})

    async def _handle_get(self, msg: BusMessage) -> None:
        key = msg.payload.get("key")
        if key not in self._store:
            await self._respond(
                msg, ok=False, error=f"Ключ не найден: {key!r}"
            )
            return
        await self._respond(
            msg, ok=True, data={"key": key, "value": self._store[key]}
        )

    async def _handle_delete(self, msg: BusMessage) -> None:
        key = msg.payload.get("key")
        existed = self._store.pop(key, None) is not None
        await self._respond(
            msg, ok=True, data={"key": key, "deleted": existed}
        )

    async def _handle_list(self, msg: BusMessage) -> None:
        await self._respond(
            msg, ok=True, data={"keys": list(self._store.keys())}
        )

    async def _respond(
        self,
        original: BusMessage,
        *,
        ok: bool,
        data: dict | None = None,
        error: str | None = None,
    ) -> None:
        await self.publish(
            BusMessage(
                source=self.name,
                target=original.source,
                type=MessageType.RESPONSE,
                payload={"ok": ok, "data": data, "error": error},
                reply_to=original.id,
                session_id=original.session_id,
            )
        )
