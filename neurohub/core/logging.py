import logging
import structlog


def setup_logging(level: str = "INFO", *, dev: bool = True) -> None:
    """
    Настроить structlog. Вызывать один раз при старте.

    Args:
        level: Имя уровня логгирования ("DEBUG", "INFO", "WARNING", "ERROR").
        dev:   True  → человеко-читабельны консольный вывод ConsoleRenderer (разработка).
               False → JSON вывод (production / log aggregation).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    renderer = (
        structlog.dev.ConsoleRenderer()
        if dev
        else structlog.processors.JSONRenderer()
    )

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
    )
