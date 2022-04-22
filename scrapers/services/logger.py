'''
logger.py
'''

import logging


class DebitLogger():
    '''
    A wrapper for Python's default Logger class. Configures a
    logger with a given name and attaches a console handler.
    '''

    def __init__(self, name: str) -> None:
        '''
        The public constructor.

        Parameters:
            name (str): The name of the logger (included in
                output).

        Returns:
            None
        '''
        # Create logging console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)

        # Create logger and attach handler
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(ch)

    
    def info(self, message:str) -> None:
        '''
        Logs an info message to the console.

        Parameters:
            message (str): The message.

        Returns:
            None
        '''
        self.logger.info(message)


    def debug(self, message:str) -> None:
        '''
        Logs a debugging message to the console.

        Parameters:
            message (str): The message.

        Returns:
            None
        '''
        self.logger.debug(message)


    def error(self, message:str) -> None:
        '''
        Logs an error message to the console.

        Parameters:
            message (str): The message.

        Returns:
            None
        '''
        self.logger.error(message)

