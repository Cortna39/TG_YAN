import logging


class ConsoleFormatter(logging.Formatter):
    default_format = "{asctime} {levelname} {message}"
    warning_format = "{asctime} {levelname} {message} {pathname}"
    error_format = "{asctime} {levelname} {message} {pathname}\n{exc_text}"

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        record.asctime = self.formatTime(record, self.datefmt)

        if record.levelno >= logging.ERROR:
            exc_text = ""
            if record.exc_info:
                exc_text = self.formatException(record.exc_info)
            record.exc_text = exc_text
            fmt = self.error_format
        elif record.levelno >= logging.WARNING:
            record.exc_text = ""
            fmt = self.warning_format
        else:
            record.exc_text = ""
            fmt = self.default_format

        return fmt.format(**record.__dict__)
