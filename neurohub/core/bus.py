"""
neurohub.core.bus

Асихронная шина событий NeuroHub - центральная точка коммуникации между модулями.

Архитектура:
    Шина реализует паттерн pub/sub поверх asyncio.Queue.
    Все модули общаются исключительно через шину - никаких прямых импортов.

    Module Source -> (publish()) -> MessageBus asyncio.Queue -> (dispatch()) -> Module Subscriber

Топики и маршрутизация:
    Шина поддерживает три типа подписки, которые применяются в следующем порядке:

    1. По target (прямая адресация):
            bus.subscribe("planner", handler)
       Срабатывает если msg.target == "planner".
       Используется когда отправитель точно знает получателя.

    2. По типу сообщения:
            bus.subscribe("COMMAND", handler)
       Срабатывает для всех сообщений с msg.type == "COMMAND",
       независимо от target. Используется модулями-обработчиками.

    3. Wildcard:
            bus.subscribe("*", handler)
       Срабатывает для абсолютно всех сообщений.
       Используется для логирования, мониторинга, отладки.

       Все три типа не исключают друг друга - одно сообщение может
       доставляться несколькими подписчиками разных типов одновременно.

Паттерн request/response:
    Для синхронного взаимодействия (запрос -> ожидание ответа):

        response = await bus.request(query_msg, timeout=5.0)

    Под капотом: publish + временная подписка на RESPONSE с фильтром по reply_to.
    Если ответа нет в течение timeout - бросает asyncio.TimeoutError.

Изоляция ошибок:
    Исключение в одном обработчике не прерывает доставку другим.
    asyncio.gather(..., return_exceptions=True) гарантирует, что упавший модуль не роняет всю шину.

Жизненный цикл:
    bus = MessageNus()
    await bus.start()    # запускает внутренний воркер
    ...
    await bus.stop()    # дожидается обработки очереди, останавливает воркер
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Callable, Awaitable

import structlog

from neurohub.core.schema import BusMessage

log = structlog.get_logger(__name__)

# Тип обработчика — async функция принимающая BusMessage и ничего не возвращающая.
# Все обработчики обязаны быть корутинами: это позволяет шине запускать
# их конкурентно через asyncio.gather без блокировки event loop.
Handler = Callable[[BusMessage], Awaitable[None]]


class MessageBus:
    """
    Асинхронная pub/sub шина событий на базе asyncio.

    Является единственным каналом коммуникации между модулями NeuroHub.
    Ядро не знает о внутреннем устройстве модулей - только о том, на какие топики они подписаны.

    Потокобезопасность:
        Шина рассчитана на работу в одном asyncio event loop.
        Не является потокобезопасной - не использовать из разных потоков.
        Для CPU-bound модулей использовать ProcessPoolExecutor,
        результат передавай обратно в event loop через loop.call_soon_threadsafe().

    Топики:
      - "*"            — получает ВСЕ сообщения (wildcard)
      - "COMMAND"      — по типу сообщения
      - "planner"      — по target (имени модуля)

    Приоритет доставки: сначала точный target, потом тип, потом wildcard.

    Пример базового использования:
        >>> bus = MessageBus()
        >>> await bus.start()
        >>>
        >>> async def my_handler(msg: BusMessage) -> None:
        ...     print(f"Получено: {msg.type} от {msg.source}")
        >>>
        >>> bus.subscribe("COMMAND", my_handler)
        >>>
        >>> await bus.publish(BusMessage(
        ...     source="cli",
        ...     type=MessageType.COMMAND,
        ...     payload={"command": "set_volume", "args": {"level": 40}},
        ... ))
        >>>
        >>> await bus.stop()

    Пример request/response:
        >>> response = await bus.request(
        ...     BusMessage(
        ...         source="planner",
        ...         target="memory",
        ...         type=MessageType.QUERY,
        ...         payload={"collection": "tasks", "mode": "filter"},
        ...     ),
        ...     timeout=3.0,
        ... )
        >>> print(response.payload["data"])
    """

    def __init__(self) -> None:
        # Реестр подписчиков: топик -> список async-обработчиков
        self._subscribers: dict[str, list[Handler]] = defaultdict(list)
        # Внутренняя очередь сообщений.
        self._queue: asyncio.Queue[BusMessage] = asyncio.Queue()
        self._running = False
        # Ссылка на фоновую задачу воркера. Нужна для корректной отмены в stop().
        self._worker_task: asyncio.Task | None = None

    async def start(self) -> None:
        """
        Запустить шину: создать фоновый воркер обработки очереди.

        Должен быть вызыван один раз перед первым publish().
        повторный вызов на уже запущенной шине не безопасен.

        Воркер работает как фоновая asyncio.Task - не блокирует event loop.
        """
        if self._running:
            log.warning("bus.already_started")
            return

        self._running = True
        self._worker_task = asyncio.create_task(
            self._worker(), name="bus-worker"
        )
        log.info("bus.started")

    async def stop(self) -> None:
        """
        Остановить шину корректно.

        После stop() вызов publish() технически работает (кладёт в очередь),
        но воркер уже не запущен - сообщения не будут доставлены.
        Повторный вызов stop() - безопасен.
        """
        if not self._running:
            return

        self._running = False

        # Дожидаемся обработки всех сообщений, которые уже в очереди
        # queue.join() блокируется, пока task_done() не вызван для каждого item.
        await self._queue.join()

        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass  # ожидаемое поведение при отмене задачи

        log.info("bus.stopped")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def subscribe(self, topic: str, handler: Handler) -> None:
        """
        Подписать async-обработчик на топик.

        Метод синхронный - подписку можно оформить до start() и после,
        изменения вступают в силу немедленно.

        Args:
            topic:
                Тописк для подписки. Три варианта:

                "*" - wildcard, все сообщения без исключения.
                    Используется для логгирования, мониторинга, отладки.
                    Пример:
                        bus.subscribe("*", audit_logger)

                "<MessageType>" - по типу сообщения из MessageType, например "COMMAND".
                    Обработчик получит все сообщения данного типа.
                    Пример:
                        bus.subscribe("COMMAND", command_dispatcher)

                "<module_name>" - по имени целевого модуля, например "planner".
                    Обработчик получит только сообщения с target="planner"
                    Пример:
                        bus.subscribe("planner", planner.on_message)

            handler:
                Async-функция сигнатуры (BusMessage) -> Awaitable[None].
                Синхронные функции не поддерживаются - они заблокируют event loop.
                Обработчик не должен бросать исключения наружу: шина их поймает
                и залогирует, но лучше обрабатывать внутри самого обработчика.

        Note:
            Один и тот же handler можно подписать на несколько топиков.
            Повторная подписка одного handler на один топик приводит
            к дублированию вызовов - шина не дедуплицирует.
        """
        self._subscribers[topic].append(handler)
        log.debug("bus.subscribed", topic=topic, handler=handler.__qualname__)

    def unsubscribe(self, topic: str, handler: Handler) -> None:
        """
        Отписать обработчик от топика.

        Если handler не был подписан на этот топик, то не бросается исключение.
        Удаляет только первое вхождение, если handler подписан дважды.

        Args:
            topic: Топик, от которого отписываем (см. subscribe()).
            handler: Сслыка на ту же функцию, что передавалась в subscribe().

        Note:
            Используется внутри request() для уборки временных обработчиков.
            В обычных модулях unsubscribe нужен редко - в большинстве случаев модуль живет столько же сколько и шина (по задумке).
        """
        handlers = self._subscribers.get(topic, [])
        if handler in handlers:
            handlers.remove(handler)
            log.debug(
                "bus.unsubscribed", topic=topic, handler=handler.__qualname__
            )

    async def publish(self, msg: BusMessage) -> None:
        """
        Поставить сообщение в очередь для доставки подписчикам.

        Метод неблокирующий: просто кладет сообщение в asyncio.Queue и возвращает управление.
        Фактическая доставка происходит асинхронно в фоновом воркере.

        Порядок доставки:
            Сообщения доставляются в порядке поступления FIFO.
            Все подписчики одного сообщения вызывается конкурентно (gather),
            но следующее сообщение берется из очереди только после завершения доставки текущего.

        Args:
            msg: Иммутабельное сообщение BusMessage для отправки.

        Note:
            publish() работает даже если шина не запущена.
            Сообщение встает в очередь и будет доставлено после start().
            Это упрощает инициализацию: модули могут публиковать при старте.
        """
        await self._queue.put(msg)
        log.debug(
            "bus.published",
            msg_id=msg.id,
            type=msg.type,
            source=msg.source,
            target=msg.target,
        )

    async def request(
        self,
        msg: BusMessage,
        timeout: float = 5.0,
    ) -> BusMessage:
        """
        Отправить сообщение и дождаться ответного RESPONSE.

        Реализует синхронный паттерн запрос/ответ поверх async шины.

        Пример сценария: модуль хочет получить данные из NeuroMemory и ждет результата перед продолжением работы.

        Args:
            msg: Сообщение запрос. Обычно типа QUERY или COMMAND.
                Должно иметь заполенный target, чтобы нужный модуль знал, что ответить именно ему.
            timeout: Максимальное время ожидания ответа в секундах.
                По умолчанию 5.0 - достаточно для локальных операций с БД.
                Для тяжелых операций типа LLM использовать 30+.

        Returns:
            BusMessage с type=RESPONSE и reply_to=msg.id.
            Payload ответа описан в neurohub.core.schema MessageType.

        Raises:
            asyncio.Timeouterror:
                Если за timeout секунд не получен ответ.
                Означает, что целевой модуль не запущен,
                завис или просто не успел отправить ответ на данный тип запроса.
        """
        # asyncio.Future - одноразовый контейнер для результата.
        # Когда нужный RESPONSE придет, future.set_result() разблокирует await ниже.
        future: asyncio.Future[BusMessage] = (
            asyncio.get_event_loop().create_future()
        )

        async def _wait_response(response: BusMessage) -> None:
            # Фильтруем только ответ на наш конкретный запрос.
            if response.reply_to == msg.id and not future.done():
                future.set_result(response)

        self.subscribe("RESPONSE", _wait_response)
        try:
            await self.publish(msg)
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            # Убираем временный обработчик в любом случае
            self.unsubscribe("RESPONSE", _wait_response)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _worker(self) -> None:
        """
        Фоновая задача - основной цикл обработки очереди сообщений.

        Запускается как asyncio.Task в методе start().
        Работает, пока self._running == True.
        """
        while self._running:
            try:
                # Без таймаута воркер зависнет и никогда не проверит self._running.
                # timeout=0.1 - ручное число, попытка баланса между отзывчивостью остановки и нагрузкой на CPU.
                msg = await asyncio.wait_for(self._queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                continue  # нет сообщений — ждём дальше

            try:
                await self._dispatch(msg)
            except Exception:
                log.exception("bus.dispatch_error", msg_id=msg.id)
            finally:
                # Обязательно сигнализируем очереди, что сообщение обработано
                # Иначе queue.join() в stop() будет ждать вечно.
                self._queue.task_done()

    async def _dispatch(self, msg: BusMessage) -> None:
        """
        Доставить сообщение всем релевантным подписчикам.

        Собирает список обработчиков в порядке приоритета:
            1. Прямая адресация: подписчики на msg.target
            2. Тип сообщения: подписчики на msg.type
            3. Wildcard: подписчики на "*".

        Args:
            msg: Сообщение для доставки.

        Note: дедупликации нет намеренно (может будет испрвленно в будущем):
            Если модуль подписался и на таргет, и на тип, то он получит сообщение дважды. Это его ответственность.

        """
        handlers: list[Handler] = []

        # 1. Точный target (например "planner")
        if msg.target:
            handlers += self._subscribers.get(msg.target, [])

        # 2. По типу сообщения (например "COMMAND")
        handlers += self._subscribers.get(msg.type, [])

        # 3. Wildcard - получают всё
        handlers += self._subscribers.get("*", [])

        if not handlers:
            log.warning(
                "bus.no_handlers",
                msg_id=msg.id,
                type=msg.type,
                target=msg.target,
            )
            return

        log.debug(
            "bus.dispatching",
            msg_id=msg.id,
            type=msg.type,
            handlers_count=len(handlers),
        )

        # Запускаем всех обработчиков конкурентно
        results = await asyncio.gather(
            *[h(msg) for h in handlers], return_exceptions=True
        )

        # Логируем падения обработчиков
        for handler, result in zip(handlers, results):
            if isinstance(result, Exception):
                log.error(
                    "bus.handler_error",
                    handler=handler.__qualname__,
                    msg_id=msg.id,
                    error=str(result),
                    error_type=type(result).__name__,
                )
