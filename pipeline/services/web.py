"""Clients for interfacing with the web.
"""

import requests
import random
import time
from pipeline.constants import TEMP_DOWNLOAD_DIR_PATH
from selenium import webdriver
from selenium.webdriver.chrome.webdriver import WebDriver
from typing import Dict, List


class DataRequestClient:
    """A wrapper for the `requests` library to rotate HTTP headers
    and add random delays to avoid throttling.
    """

    def __init__(self, user_agent_headers: List[str]) -> None:
        """Initializes a new instance of a `DataRequestClient`.

        Args:
            user_agent_headers (`list` of `str`): The user agent
                headers in HTTP requests.

        Returns:
            `None`
        """
        self._user_agent_headers = user_agent_headers

    def get(
        self,
        url: str,
        use_random_user_agent:bool=False,
        use_random_delay:bool=False,
        min_random_delay: int=1,
        max_random_delay:int=3,
        timeout_in_seconds:int=60,
        custom_headers:Dict=None) -> requests.Response:
        """Makes an HTTP GET request against the given URL.

        Args:
            url (`str`): The resource identifier.

            use_random_user_agent (bool): A boolean indicating
                whether one of several user agent HTTP headers
                should be randomly selected and included.
                Defaults to False.

            use_random_delay (bool): A boolean indicating how
                whether a random delay should be added before
                making the requests. Defaults to False.

            min_random_delay (`int`): The minimum number of seconds
                that should be included in a random delay.
                Defaults to 1.

            max_random_delay (`int`): The maximum number of seconds
                that should be included in a random delay.
                Defaults to 3.

            timeout_in_seconds (`int`): The number of seconds the
                request should be awaited before raising a timeout
                error. Defaults to 60. A value of `None` will cause
                the request to wait indefinitely.

        Returns:
            (`requests.Response`): The response object.
        """
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

        s = requests.Session()
        return s.get(url, timeout=timeout_in_seconds, headers=headers)

class HeadlessBrowser:
    """Provides headless webdrivers for different browsers.
    """

    @staticmethod
    def launch_chrome() -> WebDriver:
        """Initializes a browser controlled by a Chrome WebDriver.

        Args:
           `None`

        Returns:
            (`WebDriver`): The Chrome WebDriver instance.
        """
        # Initialize default options for headless Chrome WebDriver
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--hide-scrollbars")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.experimental_options["prefs"] = {"download.default_directory": TEMP_DOWNLOAD_DIR_PATH}

        # Initialize WebDriver
        browser = webdriver.Chrome(options=chrome_options)
        default_user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        browser.execute_cdp_cmd("Network.setUserAgentOverride", {"userAgent": default_user_agent})
        browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        return browser
