from neurohub.core.module import BaseModule
from neurohub.core.schema import BusMessage, MessageType


class EchoModule(BaseModule):
    """Отображает каждое сообщение обратно как RESPONSE. Используется для тестирования шины"""

    name = "echo"

    async def on_message(self, msg: BusMessage) -> None:
        await self.publish(
            BusMessage(
                source=self.name,
                target=msg.source,
                type=MessageType.RESPONSE,
                payload={"echo": dict(msg.payload)},
                reply_to=msg.id,
                session_id=msg.session_id,
            )
        )
