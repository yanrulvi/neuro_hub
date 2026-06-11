"""
neurohub.core.module - базовый класс для всех модулей NeuroHub.

Каждый модуль наследует BaseModule и прегружает on_message().
Шина связывает все воедино - модули никогда не вызывают друг друга напрямую.

Жизненный цикл:
    loader.load() -> module.__init__() -> module.on_start()
    шина доставляет -> module.on_message()
    loader.upload() -> module.on_stop()
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import structlog

from neurohub.core.schema import BusMessage, MessageType

if TYPE_CHECKING:
    from neurohub.core.bus import MessageBus

log = structlog.get_logger(__name__)


class BaseModule(ABC):
    """
    Абстрактный базовый класс для всех модулей NeuroHub.

    Наследуй этот класс, реализуй on_message(), положи в папку ./modules/.
    Загрузчик сам зарегестрирует модуль и подключит его к шине.

    Пример минимального модуля:
        class EchoModule(BaseModule):
            name = "echo"


            async def on_message(self, msg: BusMessage) -> None:
                await self.publish(BusMessage(
                    source=self.name,
                    target=msg.source,
                    type=MessageType.RESPONSE,
                    payload={"ok": True},
                    reply_to=msg.id,
                ))
    """

    # Должно совпадать с полем "name" в manifest.json
    # Используется для маршрутизации в шине и логирования
    name: str = ""

    def __init__(self, bus: MessageBus) -> None:
        if not self.name:
            raise ValueError(
                f"{type(self).__name__} must define a non-empty 'name'"
            )
        self._bus = bus

        self._subscriptions: list[str] = (
            []
        )  # отслеживать все активные подписки
        self._log = structlog.get_logger(self.name)

    # =================================================================
    # Жизненный цикл модуля, переопределяется модулем при необходимости
    # =================================================================

    async def on_start(self) -> None:
        """Вызывается один раз после загрузки модуля и подключения к шине"""

    async def on_stop(self) -> None:
        """Вызывается один раз перед выгрузкой модуля. Освобождает ресурсы здесь."""

    # ==============================
    # Ядро, обязательно к реализации
    # ==============================

    @abstractmethod
    async def on_message(self, msg: BusMessage) -> None:
        """Обработать входящее сообщение. Вызывается шиной для каждого подписанного топика."""

    # ====================================================
    # Вспомогательные методы, которые можно переопределить
    # ====================================================

    async def publish(self, msg: BusMessage) -> None:
        """Опубликовать сообщение в шину."""
        await self._bus.publish(msg)

    async def request(
        self, msg: BusMessage, timeout: float = 5.0
    ) -> BusMessage:
        """Опубликовать и дождаться RESPONSE. См. MessageBis.request()."""
        return await self._bus.request(msg, timeout=timeout)

    def subscribe(self, topic: str) -> None:
        """Подписать on_message на топик. Вызывается загрузчиком из manifest."""
        self._bus.subscribe(topic, self.on_message)
        self._subscriptions.append(topic)

    def unsubscribe(self, topic: str) -> None:
        """Отписать on_message от топика."""
        self._bus.unsubscribe(topic, self.on_message)
        if topic in self._subscriptions:
            self._subscriptions.remove(topic)

    def __repr__(self) -> str:
        return f"<{type(self).__name__} name={self.name!r}>"
