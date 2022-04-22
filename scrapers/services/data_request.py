'''
data_request.py
'''

import requests
import random
import time
from typing import Dict, List


class DataRequestClient:
    '''
    A wrapper for the `requests` class to rotate HTTP headers
    and add random delays to avoid throttling.
    '''

    def __init__(self, user_agent_headers: List[str]) -> None:
        '''
        The public constructor.

        Parameters:
            user_agent_headers (list of str): The user agent
                headers in HTTP requests.
        '''
        self._user_agent_headers = user_agent_headers

    
    def get(
        self,
        url: str,
        use_random_user_agent:bool=False,
        use_random_delay:bool=False,
        min_random_delay: int=1,
        max_random_delay:int=3,
        timeout_in_seconds:int=60,
        custom_headers:Dict=None):
        '''
        Makes an HTTP GET request against the given URL.

        Parameters:
            url (str): The resource identifier.

            use_random_user_agent (bool): A boolean indicating
                whether one of several user agent HTTP headers
                should be randomly selected and included.
                Defaults to False.

            use_random_delay (bool): A boolean indicating how
                whether a random delay should be added before
                making the requests. Defaults to False.

            min_random_delay (int): The minimum number of seconds
                that should be included in a random delay.
                Defaults to 1.

            max_random_delay (int): The maximum number of seconds
                that should be included in a random delay.
                Defaults to 3.

            timeout_in_seconds (int): The number of seconds the
                request should be awaited before raising a timeout
                error. Defaults to 60. A value of `None` will cause
                the request to wait indefinitely.
        '''
        if max_random_delay < min_random_delay:
            raise ValueError("The minimum delay time must be less than "
                "the maximum time.")

        if use_random_delay:
            delay = random.randint(min_random_delay, max_random_delay)
            time.sleep(delay)

        if use_random_user_agent and not custom_headers:
            agent_idx = random.randint(0, len(self._user_agent_headers) - 1)
            headers = {"User-Agent": self._user_agent_headers[agent_idx]}
        elif custom_headers:
            headers = custom_headers
        else:
            headers = None

        return requests.get(url, timeout=timeout_in_seconds, headers=headers)

