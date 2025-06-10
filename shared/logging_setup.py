import logging


def setup_logging(level=logging.INFO):
    """
    Налаштовує базове логування: вивід у консоль та файл.
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(),  # Вивід у консоль
        ],
    )
    print("Базове логування налаштовано. Логи пишуться у консоль.")
