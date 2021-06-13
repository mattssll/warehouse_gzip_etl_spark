import logging
from logging import RootLogger
# DEBUG, INFO, WARNING, CRITICAL, ERROR - common log types
# Create and Configure Logger
def configure_logger() -> RootLogger: #-> baseLogger:
    LOG_FORMAT = "MyApp: %(levelname)s - %(asctime)s - %(module)s - %(message)s"
    logging.basicConfig(filename="logs/logs.log", level = logging.DEBUG, format = LOG_FORMAT)
    logger = logging.getLogger("py4j")
    #logger.info("Application started.")
    return logger

configure_logger()
