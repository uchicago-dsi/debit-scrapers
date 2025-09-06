"""Provides loggers for use across the application."""

# Standard library imports
import logging


class LoggerFactory:
    """A simple factory for configuring standard loggers."""

    @staticmethod
    def get(name: str, level: int = logging.CRITICAL) -> logging.Logger:
        """Creates a logger with the given name and level.

        Attaches a stream handler that prints logs in a
        standard format to the console.

        Args:
            name: The logger name.

            level: The initial level. Defaults to 50 ("CRITICAL").

        Returns:
            The logger.
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
