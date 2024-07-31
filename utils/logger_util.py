import logging

"""
Creates and returns a simple logger with the specified name and level

Parameters:
- name: Name of the logger, typically __name__
- level: Logging level, e.g., logging.DEBUG, logging.INFO, etc

Returns:
- A configured logger instance
"""


# TODO: Expand to Data dog if this ever hits production
class LoggerUtil:
    @staticmethod
    def get_logger(name, level=logging.DEBUG):
        logger = logging.getLogger(name)
        logger.setLevel(level)

        logging.basicConfig(
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        return logger