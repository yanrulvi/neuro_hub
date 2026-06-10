"""
neurohub.core.schema

Схема сообщений шины событий NeuroHub.

Все модули общаются исключительно через BusMessage.
Никакие прямые вызовы между модулями не допускаются.

Жизненный цикл сообщения:
    1. Модуль-источник создает BusMessage и вызывает bus.publish(msg)
    2. MessageBus доставляет сообщение всем подписчикам на данный тип/target
    3. Получатель при необходимости создает новое сообщение с reply_to=msg.id
    4. Исходное сообщение никогда не изменяется (frozen=True)
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from types import MappingProxyType

from pydantic import BaseModel, Field, field_validator


class MessageType(StrEnum):
    """
    Перечень типов сообщений, которые курсируют по шине.

    Тип определяет семантику сообщения - кто его ожидает и что с ним делать.
    Модули подписываются на один или несколько типов через manifest.json

    Используется StrEnum, поэтому работают оба варианта сравнения:
        msg.type == MessageType.COMMAND    # через enum
        msg.type == "COMMAND"              # через строку

    Типы:
        INPUT_TEXT:
            Сырой текст от любого источника ввода.
            Публикуется источниками ввода, потребляется классификаторами намерений.
            Payload: {
                "text": str,            # распознанный текст
                "confidence": float,    # уверенность (0.0-1.0), опционально
                "lang": str,            # язык ("ru", "en"), опционально
                "audio_path": str,      # путь к исходному аудио, опционально
            }

        COMMAND:
            Готовая к исполнению команда конкретному исполнителю.
            Всегда адресована - поле target должно быть заполнено.
            Публикуется классификатором намерений и модулями-оркестраторами.
            Payload: {
                "command": str,         # идентификатор команды, напр. "set_volume"
                "args": dict,           # аргументы команды, опционально
            }

        QUERY:
            Запрос данных из хранилища или другого модуля.
            Отличие от COMMAND: не производит действий, read-only.
            Всегда ожидает ответного RESPONSE с reply_to.
            Payload: {
                "collection": str,      # целевая коллекция, напр. "notes"
                "mode": str,            # "exact" | "semantic" | "filter"
                "q": str,               # поисковой запрос, опционально
                "filters": dict,        # фильтры по полям, опционально
                "limit": int,           # макс. кол-во результатов, опционально
            }

        SYSTEM_EVENT:
            Внутренние события ядра: запуск, остановка, триггеры, ошибки.
            Публикуется ядром системы, не модулями.
            Payload: {
                "event": str,           # идентификатор события
                "data": dict,           # данные события, опционально
            }

        INTENT:
            Распознанное намерение с извлеченными сущностями.
            Промежуточный тип между сырым вводом и командой исполнителю.
            Классификатор намерений читает INPUT_TEXT и публикует INTENT.
            Payload: {
                "intent": str,          # напр. "create_reminde"
                "confidence": float,    # уверенность классифиактора (0.0-1.0)
                "entities: dict,        # извлеченные сущности (дата, имя и т.д.)
                "raw_text": str,        # исходный текст
                "llm_level": int,       # уровень каскада: 1=BERT, 2=local, 3=cloud
            }

        RESPONSE:
            Ответ на COMMAND или QUERY. Всегда содержит reply_to.
            Инициатор запроса ожидает RESPONSE через bus.request().
            Payload: {
                "ok": bool,             # успех или ошибка
                "data": dict,           # результат, если ok=True
                "error": str,           # описание ошибки, если ok=False
            }
    """

    INPUT_TEXT = "INPUT_TEXT"
    COMMAND = "COMMAND"
    QUERY = "QUERY"
    SYSTEM_EVENT = "SYSTEM_EVENT"
    INTENT = "INTENT"
    RESPONSE = "RESPONSE"


class BusMessage(BaseModel):
    """
    Единтсвенный формат сообщений в шине событий NeuroHub.

    Все коммуникации между модулями проходят через этот класс.
    Прямые импорты между модулями запрещены - только BusMessage через шину.

    Иммутабельность (frozen=True):
        После создания сообщение нельзя изменить.
        Сообзение - это факт о том, что что-то произошло в конкретный момент времени.
        Если нужно ответить - создается новое сообщение с reply_to=<id>.

    Версионирование (schema_version):
        При изменении структуры BusMessage в будущем модули
        смогут проверить версию и применить миграцию payload при необходимости.

    Пример - инициирующее сообщение:
        >>> msg = BusMessage(
        ...     source="transcriber",
        ...     type=MessageType.INPUT_TEXT,
        ...     payload={"text": "открой VS Code", "confidence": 0.97},
        ... )

    Пример - ответное сообщение:
        >>> response = BusMessage(
        ...     source="system_executor",
        ...     target=msg.source,
        ...     type=MessageType.RESPONSE,
        ...     payload={"ok": True, "data": {"pid": 12345}},
        ...     reply_to=msg.id,
        ...     session_id=msg.session_id,
        ... )
    """

    id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="UUID4, генерируется автоматически при создании.",
    )
    schema_version: str = Field(
        default="1.0",
        description="Версия схемы BusMessage. Не менять вручную.",
    )
    ts: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="Временная метка создания в формате ISO 8601 с UTC-таймзоной. Хранится как str, а не datetime, чтобы сериализация с JSON была тривиальной и не зависела от настроек сериализатора.",
    )
    source: str = Field(
        description="Имя модуля отправителя. Должно совпадать с полем name в manifest.json модуля. Используется для отладки и трассировки."
    )
    target: str | None = Field(
        default=None,
        description="Имя целевого модуля. None - broadcast: сообщение доставляется всем подписчикам на данный тип. Строка - direct: доставляется только указанному модулю.",
    )
    type: MessageType = Field(
        description="Тип сообщения. Определяет семантику и маршрутизацию в шине."
    )
    payload: dict[str, Any] = Field(
        description="Тело сообщения. Намеренно dict[str, Any] - жесткая схема payload живет внутри каждого модуля, а не в ядре. Это позволяет модулям эволюционировать независимо. Модуль-получатель валидирует payload через свои Pydantic-модели при получении."
    )

    reply_to: str | None = Field(
        default=None,
        description="ID сообщения, на которое это является ответом. Заполняется только в сообщениях типа RESPONSE. Используется bus.request() для сопоставления запроса и ответа.",
    )
    session_id: str | None = Field(
        default=None,
        description="ID сессии для группировки связанных сообщений. Позволяет реконструировать контекст: все что произошло за один диалог или рабочий день объединяется в один session_id.",
    )

    @field_validator("payload", mode="after")
    @classmethod
    def freeze_payload(cls, v: dict[str, Any]) -> MappingProxyType:
        return MappingProxyType(v)

    model_config = {"frozen": True}  # сообщения иммутабельны
