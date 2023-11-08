"""Provides loggers for use across the application.
"""

import logging


class LoggerFactory:
    """
    A simple factory for configuring standard loggers.
    """

    @staticmethod
    def get(name: str, level: int=logging.INFO) -> logging.Logger:
        """
        Creates a new logger with the given name and
        level and then attaches a stream handler.

        Parameters:
            name (`str`): The logger name.

            level (int): The initial level. Defaults
                to 20 ("INFO").

        Returns:
            (`logging.Logger`): The logger.
        """
        # Create logger and set level
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Create console handler and set level
        ch = logging.StreamHandler()
        ch.setLevel(level)

        # Create formatter and add to handler
        format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(format)
        ch.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(ch)

        return logger

