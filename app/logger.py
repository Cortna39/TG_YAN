import os, logging, sys
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger

LOG_DIR = os.getenv("LOG_DIR", "/var/log/metrika-bx")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_JSON = (os.getenv("LOG_JSON", "true").lower() == "true")

os.makedirs(LOG_DIR, exist_ok=True)

def _make_formatter():
    if LOG_JSON:
        return jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s %(filename)s %(lineno)d",
            timestamp=True
        )
    return logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s (%(filename)s:%(lineno)d)",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def _make_handlers(filename: str):
    formatter = _make_formatter()

    file_handler = RotatingFileHandler(
        os.path.join(LOG_DIR, filename),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(LOG_LEVEL)

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    console.setLevel(LOG_LEVEL)

    return [file_handler, console]

def configure_root(app_logfile: str = "app.log"):
    logging.captureWarnings(True)
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(LOG_LEVEL)
    for h in _make_handlers(app_logfile):
        root.addHandler(h)

def get_logger(name: str):
    return logging.getLogger(name)
