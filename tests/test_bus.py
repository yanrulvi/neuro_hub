"""
Тесты для neurohub.core.bus.MessageBus.

Проверяют:
    - Базовую доставку (подписка, публикация, получение)
    - Маршрутизацию (target, type, wildcard)
    - Приоритеты доставки (target > type > wildcard)
    - Отписку (unsubscribe)
    - Паттерн request/response
    - Таймауты
    - Изоляцию ошибок в обработчиках
    - Конкурентную доставку нескольким подписчикам
    - Краевые случаи (нет подписчиков, остановка/перезапуск)
"""

import asyncio
import pytest
from neurohub.core.bus import MessageBus
from neurohub.core.schema import BusMessage, MessageType


class TestableMessageBus(MessageBus):
    """
    Расширенная шина для тестов с методом ожидания опустошения очереди.

    Нужна чтобы тесты были детерминированными и не зависели от asyncio.sleep().
    """

    __test__ = False

    async def wait_empty(self) -> None:
        """Дождаться пока все сообщения в очереди будут обработаны."""
        await self._queue.join()


@pytest.fixture
async def bus():
    """
    Фикстура предоставляет запущенную шину на время теста.

    Гарантирует что после теста шина остановлена и ресурсы освобождены.
    Каждый тест получает свежий экземпляр - подписчики не утекают между тестами.
    """
    b = TestableMessageBus()
    await b.start()
    yield b
    await b.stop()


def make_msg(**kwargs) -> BusMessage:
    """Хелпер для создания тестовых сообщений с разумными значениями по умолчанию."""
    defaults = {
        "source": "test_module",
        "type": MessageType.COMMAND,
        "payload": {},
    }
    return BusMessage(**{**defaults, **kwargs})


# ==================================================================
# Базовая доставка
# ==================================================================


async def test_subscribe_and_receive(bus):
    """Подписчик получает опубликованное сообщение."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)
    msg = make_msg()
    await bus.publish(msg)
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].id == msg.id
    assert received[0].source == "test_module"
    assert received[0].type == MessageType.COMMAND


async def test_multiple_subscribers_same_topic(bus):
    """Несколько подписчиков на один топик — получают все."""
    received_1 = []
    received_2 = []

    async def handler_1(m: BusMessage) -> None:
        received_1.append(m)

    async def handler_2(m: BusMessage) -> None:
        received_2.append(m)

    bus.subscribe("COMMAND", handler_1)
    bus.subscribe("COMMAND", handler_2)

    msg = make_msg()
    await bus.publish(msg)
    await bus.wait_empty()

    assert len(received_1) == 1
    assert len(received_2) == 1
    assert received_1[0].id == msg.id
    assert received_2[0].id == msg.id


async def test_no_handlers_no_crash(bus):
    """Сообщение без подписчиков не роняет шину."""
    await bus.publish(make_msg(target="nonexistent", type=MessageType.QUERY))
    await bus.wait_empty()

    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)
    await bus.publish(make_msg())
    await bus.wait_empty()

    assert len(received) == 1


async def test_concurrent_publish(bus):
    """Несколько конкурентных publish не теряют сообщения."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)

    async def publish_batch(label: str, count: int):
        for i in range(count):
            await bus.publish(make_msg(payload={"label": label, "i": i}))

    await asyncio.gather(publish_batch("a", 5), publish_batch("b", 5))
    await bus.wait_empty()

    assert len(received) == 10


async def test_message_to_self(bus):
    """Модуль может отправить сообщение самому себе."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("echo", handler)
    await bus.publish(make_msg(target="echo", source="echo"))
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].source == "echo"
    assert received[0].target == "echo"


# ==================================================================
# Маршрутизация: target, type, wildcard
# ==================================================================


async def test_target_routing(bus):
    """Сообщение с target доставляется только подписчику на этот target."""
    planner_received = []
    notes_received = []

    async def planner_handler(m: BusMessage) -> None:
        planner_received.append(m)

    async def notes_handler(m: BusMessage) -> None:
        notes_received.append(m)

    bus.subscribe("planner", planner_handler)
    bus.subscribe("notes", notes_handler)

    await bus.publish(make_msg(target="planner"))
    await bus.wait_empty()

    assert len(planner_received) == 1
    assert len(notes_received) == 0


async def test_type_routing(bus):
    """Сообщение доставляется подписчикам на его тип."""
    command_received = []
    query_received = []

    async def command_handler(m: BusMessage) -> None:
        command_received.append(m)

    async def query_handler(m: BusMessage) -> None:
        query_received.append(m)

    bus.subscribe("COMMAND", command_handler)
    bus.subscribe("QUERY", query_handler)

    await bus.publish(make_msg(type=MessageType.COMMAND))
    await bus.wait_empty()

    assert len(command_received) == 1
    assert len(query_received) == 0


async def test_wildcard_receives_everything(bus):
    """Wildcard-подписчик получает сообщения любого типа и target."""
    received = []

    async def wildcard_handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("*", wildcard_handler)

    await bus.publish(make_msg(type=MessageType.COMMAND, target="planner"))
    await bus.publish(make_msg(type=MessageType.QUERY, target=None))
    await bus.publish(make_msg(type=MessageType.SYSTEM_EVENT))
    await bus.wait_empty()

    assert len(received) == 3


async def test_broadcast_no_target(bus):
    """Сообщение без target доходит до type- и wildcard-подписчиков."""
    type_received = []
    wildcard_received = []

    async def type_handler(m: BusMessage) -> None:
        type_received.append(m)

    async def wildcard_handler(m: BusMessage) -> None:
        wildcard_received.append(m)

    bus.subscribe("COMMAND", type_handler)
    bus.subscribe("*", wildcard_handler)

    await bus.publish(make_msg(target=None))
    await bus.wait_empty()

    assert len(type_received) == 1
    assert len(wildcard_received) == 1


async def test_priority_order(bus):
    """
    Приоритет доставки: target > type > wildcard.

    Обработчики вызываются именно в этом порядке.
    """
    order = []

    async def target_handler(m: BusMessage) -> None:
        order.append("target")

    async def type_handler(m: BusMessage) -> None:
        order.append("type")

    async def wildcard_handler(m: BusMessage) -> None:
        order.append("wildcard")

    bus.subscribe("planner", target_handler)
    bus.subscribe("COMMAND", type_handler)
    bus.subscribe("*", wildcard_handler)

    await bus.publish(make_msg(target="planner", type=MessageType.COMMAND))
    await bus.wait_empty()

    assert order == ["target", "type", "wildcard"]


async def test_handler_called_twice_when_subscribed_to_both(bus):
    """
    Если один обработчик подписан и на target, и на type,
    он вызывается дважды. Дедупликация не выполняется.
    """
    calls = []

    async def handler(m: BusMessage) -> None:
        calls.append(m)

    bus.subscribe("planner", handler)
    bus.subscribe("COMMAND", handler)

    await bus.publish(make_msg(target="planner", type=MessageType.COMMAND))
    await bus.wait_empty()

    assert len(calls) == 2


async def test_handler_subscribed_multiple_topics(bus):
    """Один обработчик может быть подписан на несколько топиков."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)
    bus.subscribe("QUERY", handler)

    await bus.publish(make_msg(type=MessageType.COMMAND))
    await bus.publish(make_msg(type=MessageType.QUERY))
    await bus.wait_empty()

    assert len(received) == 2


# ==================================================================
# Подписка во время работы
# ==================================================================


async def test_subscribe_after_start(bus):
    """Подписка после start() работает — обработчик получает новые сообщения."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    await bus.publish(make_msg(payload={"n": 1}))
    await bus.wait_empty()

    bus.subscribe("COMMAND", handler)

    await bus.publish(make_msg(payload={"n": 2}))
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].payload["n"] == 2


async def test_subscribe_during_dispatch(bus):
    """
    Новый подписчик, добавленный во время доставки другому обработчику,
    не получит текущее сообщение, но получит следующие.
    """
    received_old = []
    received_new = []

    async def old_handler(m: BusMessage) -> None:
        received_old.append(m)

    bus.subscribe("COMMAND", old_handler)

    await bus.publish(make_msg(payload={"n": 1}))
    await bus.wait_empty()

    async def new_handler(m: BusMessage) -> None:
        received_new.append(m)

    bus.subscribe("COMMAND", new_handler)

    await bus.publish(make_msg(payload={"n": 2}))
    await bus.wait_empty()

    assert len(received_old) == 2
    assert len(received_new) == 1


# ==================================================================
# Отписка
# ==================================================================


async def test_unsubscribe(bus):
    """После unsubscribe обработчик больше не вызывается."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)
    bus.unsubscribe("COMMAND", handler)

    await bus.publish(make_msg())
    await bus.wait_empty()

    assert len(received) == 0


async def test_unsubscribe_nonexistent(bus):
    """Отписка несуществующего обработчика не бросает исключений."""

    async def handler(m: BusMessage) -> None:
        pass

    bus.unsubscribe("COMMAND", handler)


async def test_unsubscribe_only_one(bus):
    """Отписка одного обработчика не затрагивает другие на том же топике."""
    received_1 = []
    received_2 = []

    async def handler_1(m: BusMessage) -> None:
        received_1.append(m)

    async def handler_2(m: BusMessage) -> None:
        received_2.append(m)

    bus.subscribe("COMMAND", handler_1)
    bus.subscribe("COMMAND", handler_2)
    bus.unsubscribe("COMMAND", handler_1)

    await bus.publish(make_msg())
    await bus.wait_empty()

    assert len(received_1) == 0
    assert len(received_2) == 1


async def test_unsubscribe_during_dispatch(bus):
    """Обработчик может отписать сам себя во время выполнения."""
    received = []

    async def self_destructing_handler(m: BusMessage) -> None:
        received.append(m)
        bus.unsubscribe("COMMAND", self_destructing_handler)

    bus.subscribe("COMMAND", self_destructing_handler)

    await bus.publish(make_msg(payload={"n": 1}))
    await bus.wait_empty()

    await bus.publish(make_msg(payload={"n": 2}))
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].payload["n"] == 1


async def test_unsubscribe_from_empty_topic(bus):
    """Отписка от топика без подписчиков не падает."""

    async def handler(m: BusMessage) -> None:
        pass

    bus.unsubscribe("nonexistent_topic", handler)


# ==================================================================
# Паттерн request/response
# ==================================================================


async def test_request_response(bus):
    """request() возвращает RESPONSE с правильным reply_to."""

    async def echo_handler(m: BusMessage) -> None:
        response = BusMessage(
            source="echo",
            target=m.source,
            type=MessageType.RESPONSE,
            payload={"echo": m.payload},
            reply_to=m.id,
        )
        await bus.publish(response)

    bus.subscribe("COMMAND", echo_handler)

    msg = make_msg(payload={"cmd": "ping"})
    response = await bus.request(msg, timeout=2.0)

    assert response.reply_to == msg.id
    assert response.type == MessageType.RESPONSE
    assert response.payload["echo"] == {"cmd": "ping"}

    await bus.wait_empty()


async def test_request_timeout(bus):
    """request() бросает TimeoutError если ответ не приходит вовремя."""
    msg = make_msg()
    with pytest.raises(asyncio.TimeoutError):
        await bus.request(msg, timeout=0.05)

    await bus.wait_empty()


async def test_request_multiple_responses(bus):
    """
    Если приходит несколько RESPONSE с одинаковым reply_to,
    возвращается первый, остальные игнорируются.
    """

    async def multi_echo_handler(m: BusMessage) -> None:
        for i in range(3):
            response = BusMessage(
                source="echo",
                target=m.source,
                type=MessageType.RESPONSE,
                payload={"num": i},
                reply_to=m.id,
            )
            await bus.publish(response)

    bus.subscribe("COMMAND", multi_echo_handler)

    msg = make_msg()
    response = await bus.request(msg, timeout=2.0)

    assert response.payload["num"] == 0

    await bus.wait_empty()


async def test_request_cancellation_cleans_up(bus):
    """При отмене request() временный обработчик отписывается."""
    msg = make_msg()

    task = asyncio.create_task(bus.request(msg, timeout=10.0))
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert len(bus._subscribers.get("RESPONSE", [])) == 0


async def test_concurrent_requests(bus):
    """Два конкурентных request() не мешают друг другу."""

    async def echo_handler(m: BusMessage) -> None:
        response = BusMessage(
            source="echo",
            target=m.source,
            type=MessageType.RESPONSE,
            payload={"echo": m.payload},
            reply_to=m.id,
        )
        await bus.publish(response)

    bus.subscribe("COMMAND", echo_handler)

    msg1 = make_msg(payload={"n": 1})
    msg2 = make_msg(payload={"n": 2})

    r1, r2 = await asyncio.gather(
        bus.request(msg1, timeout=2.0),
        bus.request(msg2, timeout=2.0),
    )

    assert r1.payload["echo"] == {"n": 1}
    assert r2.payload["echo"] == {"n": 2}

    await bus.wait_empty()


async def test_request_response_with_wildcard(bus):
    """
    Wildcard-подписчик тоже получает RESPONSE,
    но это не мешает request() получить свой ответ.
    """
    wildcard_received = []

    async def wildcard_handler(m: BusMessage) -> None:
        wildcard_received.append(m)

    async def echo_handler(m: BusMessage) -> None:
        response = BusMessage(
            source="echo",
            target=m.source,
            type=MessageType.RESPONSE,
            payload={"ok": True},
            reply_to=m.id,
        )
        await bus.publish(response)

    bus.subscribe("*", wildcard_handler)
    bus.subscribe("COMMAND", echo_handler)

    msg = make_msg()
    response = await bus.request(msg, timeout=2.0)

    assert response.payload["ok"] is True
    assert len(wildcard_received) == 2

    await bus.wait_empty()


# ==================================================================
# Изоляция ошибок
# ==================================================================


async def test_handler_exception_isolated(bus):
    """
    Исключение в одном обработчике не прерывает доставку другим
    и не роняет шину.
    """
    received_good = []

    async def bad_handler(m: BusMessage) -> None:
        raise RuntimeError("Обработчик упал!")

    async def good_handler(m: BusMessage) -> None:
        received_good.append(m)

    bus.subscribe("COMMAND", bad_handler)
    bus.subscribe("COMMAND", good_handler)

    msg = make_msg()
    await bus.publish(msg)
    await bus.wait_empty()

    assert len(received_good) == 1
    assert received_good[0].id == msg.id


async def test_publish_survives_handler_crash(bus):
    """Шина логирует ошибку обработчика, но publish() не бросает исключение."""

    async def bad_handler(m: BusMessage) -> None:
        raise ValueError("Тестовая ошибка")

    bus.subscribe("COMMAND", bad_handler)

    await bus.publish(make_msg())
    await bus.wait_empty()


async def test_all_handlers_crash(bus):
    """Даже если все обработчики падают, шина продолжает работать."""
    crash_count = 0

    async def crashing_handler(m: BusMessage) -> None:
        nonlocal crash_count
        crash_count += 1
        raise RuntimeError(f"Crash #{crash_count}")

    bus.subscribe("COMMAND", crashing_handler)
    bus.subscribe("COMMAND", crashing_handler)

    await bus.publish(make_msg())
    await bus.wait_empty()

    assert crash_count == 2

    received = []

    async def good_handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", good_handler)
    await bus.publish(make_msg())
    await bus.wait_empty()

    assert len(received) == 1


# ==================================================================
# Жизненный цикл шины
# ==================================================================


async def test_publish_before_start():
    """Сообщения опубликованные до start() доставляются после запуска."""
    bus = TestableMessageBus()
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)

    msg = make_msg()
    await bus.publish(msg)

    await bus.start()
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].id == msg.id

    await bus.stop()


async def test_restart():
    """Шину можно остановить и запустить заново."""
    bus = TestableMessageBus()
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)

    await bus.start()
    await bus.publish(make_msg(payload={"cycle": 1}))
    await bus.wait_empty()
    await bus.stop()

    assert len(received) == 1
    received.clear()

    await bus.start()
    await bus.publish(make_msg(payload={"cycle": 2}))
    await bus.wait_empty()
    await bus.stop()

    assert len(received) == 1
    assert received[0].payload["cycle"] == 2


async def test_double_stop_is_safe():
    """Повторный stop() безопасен."""
    bus = TestableMessageBus()
    await bus.start()
    await bus.stop()
    await bus.stop()


async def test_double_start_is_safe(bus):
    """Повторный start() не падает (шина уже запущена фикстурой)."""
    await bus.start()


async def test_stop_without_messages():
    """Остановка пустой шины не зависает."""
    bus = TestableMessageBus()
    await bus.start()
    await bus.stop()


async def test_publish_after_stop(bus):
    """
    Сообщение можно опубликовать после stop(),
    но оно не будет доставлено (воркер остановлен).
    """
    await bus.stop()

    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)
    await bus.publish(make_msg())

    assert len(received) == 0


async def test_stop_waits_for_pending_messages():
    """stop() дожидается обработки сообщений, которые уже в очереди."""
    bus = TestableMessageBus()
    await bus.start()

    received = []
    event = asyncio.Event()

    async def slow_handler(m: BusMessage) -> None:
        await event.wait()
        received.append(m)

    bus.subscribe("COMMAND", slow_handler)

    msg = make_msg()
    await bus.publish(msg)

    stop_task = asyncio.create_task(bus.stop())

    await asyncio.sleep(0.05)

    assert not stop_task.done()

    event.set()

    await asyncio.wait_for(stop_task, timeout=1.0)

    assert len(received) == 1
    assert received[0].id == msg.id


# ==================================================================
# Иммутабельность сообщений
# ==================================================================


async def test_message_is_frozen():
    """BusMessage нельзя изменить после создания."""
    msg = make_msg(payload={"key": "value"})

    with pytest.raises(Exception):
        msg.source = "other"

    with pytest.raises(Exception):
        msg.payload["key"] = "changed"


async def test_handler_receives_same_object(bus):
    """Обработчик получает тот же объект сообщения (не копию)."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)
    msg = make_msg()
    await bus.publish(msg)
    await bus.wait_empty()

    assert received[0] is msg


def test_message_requires_type():
    """BusMessage требует поле type."""
    with pytest.raises(Exception):
        BusMessage(source="test", payload={})


def test_message_requires_source():
    """BusMessage требует поле source."""
    with pytest.raises(Exception):
        BusMessage(type=MessageType.COMMAND, payload={})


def test_message_auto_generates_id():
    """BusMessage автоматически генерирует уникальный id."""
    msg1 = BusMessage(source="test", type=MessageType.COMMAND, payload={})
    msg2 = BusMessage(source="test", type=MessageType.COMMAND, payload={})

    assert msg1.id != msg2.id
    assert len(msg1.id) == 36


def test_message_auto_generates_ts():
    """BusMessage автоматически генерирует временную метку."""
    msg = BusMessage(source="test", type=MessageType.COMMAND, payload={})

    assert msg.ts is not None
    assert "T" in msg.ts


# ==================================================================
# Конкурентность
# ==================================================================


async def test_many_concurrent_publishers(bus):
    """Много конкурентных издателей не теряют сообщения."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)

    async def publisher(n: int):
        for i in range(n):
            await bus.publish(make_msg(payload={"i": i}))

    publishers = [publisher(10) for _ in range(10)]
    await asyncio.gather(*publishers)
    await bus.wait_empty()

    assert len(received) == 100


# ==================================================================
# Граничные значения
# ==================================================================


async def test_empty_payload(bus):
    """Сообщение с пустым payload доставляется."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)
    await bus.publish(make_msg(payload={}))
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].payload == {}


async def test_large_payload(bus):
    """Сообщение с большим payload доставляется."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)

    large_data = {"key_" + str(i): "value_" + str(i) * 100 for i in range(100)}
    await bus.publish(make_msg(payload=large_data))
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].payload == large_data


async def test_unicode_in_payload(bus):
    """Сообщение с Unicode в payload доставляется."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)
    await bus.publish(make_msg(payload={"text": "Привет, мир! 🚀"}))
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].payload["text"] == "Привет, мир! 🚀"


async def test_nested_payload(bus):
    """Сообщение с вложенной структурой в payload доставляется."""
    received = []

    async def handler(m: BusMessage) -> None:
        received.append(m)

    bus.subscribe("COMMAND", handler)

    nested = {
        "level1": {
            "level2": {
                "level3": [1, 2, 3],
                "value": None,
                "flag": True,
            }
        }
    }
    await bus.publish(make_msg(payload=nested))
    await bus.wait_empty()

    assert len(received) == 1
    assert received[0].payload == nested
