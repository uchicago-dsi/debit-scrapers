"""Classes for browser automation."""

# Standard library imports
import json
import random
from collections.abc import Callable
from tempfile import NamedTemporaryFile
from typing import IO

# Third-party imports
import playwright
import playwright.sync_api
from django.conf import settings
from playwright_stealth import Stealth
from playwright.sync_api import Page, sync_playwright


class HeadlessBrowser:
    """A wrapper for the Playwright browser automation library."""

    def __init__(self) -> None:
        """Initializes a new instance of a `HeadlessBrowser`.

        Args:
            user_agent_headers: The user agent headers in HTTP requests.

        Returns:
            `None`
        """
        with open(settings.USER_AGENT_HEADERS_FPATH) as f:
            self._user_agent_headers = json.load(f)

    def download_file(
        self, url: str, action: Callable[[Page], None], timeout: int = 60_000
    ) -> IO[bytes]:
        """Downloads a file hosted at the given URL.

        Args:
            url: The URL to the file to download.

            action: The action to trigger the download.

            timeout: The number of milliseconds to wait for the page to load.

        Returns:
            The downloaded file, saved to a temporary location.
        """
        # Select random user agent header
        user_agent = random.choice(self._user_agent_headers)

        with sync_playwright() as p:
            try:
                # Launch headless browser
                browser = p.chromium.launch(headless=True)

                # Open new browser tab and navigate to webpage
                first_page = browser.new_page()
                first_page.set_extra_http_headers({"User-Agent": user_agent})
                first_page.goto(url, timeout=timeout)

                # Trigger download event and note download URL
                with first_page.expect_download(timeout=0) as download_event:
                    action(first_page)
                    download_url = download_event.value.url
                    download_name = download_event.value.suggested_filename

                # Open second browser tab
                second_page = browser.new_page()
                second_page.set_extra_http_headers({"User-Agent": user_agent})

                # Start download context and catch aborted event
                with second_page.expect_download(timeout=0) as download_event:
                    try:
                        second_page.goto(download_url, timeout=timeout)
                    except playwright.sync_api.Error:
                        pass

                # Create temporary file
                prefix, suffix = download_name.split(".")
                temp_file = NamedTemporaryFile(
                    delete=False, prefix=prefix, suffix=f".{suffix}"
                )

                # Close file handle to make writable
                temp_file.close()

                # Save download to temporary file
                download_event.value.save_as(temp_file.name)

                # Return temporary file
                return temp_file
            finally:
                browser.close()

    def get_html(self, url: str) -> str:
        """Retrieves the HTML content of a given URL.

        Args:
            url: The resource identifier.

        Returns:
            The HTML content.
        """
        # Select random user agent header
        import os

        try:
            proxy_endpoint = os.environ["PROXY_ENDPOINT"]
            proxy_username = os.environ["PROXY_USERNAME"]
            proxy_password = os.environ["PROXY_PASSWORD"]
        except KeyError as e:
            raise RuntimeError(f"{e} environment variable not set.") from None

        # Use browser to fetch HTML
        with Stealth().use_sync(sync_playwright()) as p:
            try:
                browser = p.chromium.launch(
                    headless=False,
                    proxy={
                        "server": proxy_endpoint,
                        "username": proxy_username,
                        "password": proxy_password,
                    },
                    args=[
                        "--no-sandbox",
                        "--disable-blink-features=AutomationControlled",
                        "--disable-web-security",
                        "--disable-features=VizDisplayCompositor",
                    ],
                )

                context = browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    viewport={"width": 1920, "height": 1080},
                    java_script_enabled=True,
                    extra_http_headers={
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                        "Accept-Language": "en-US,en;q=0.9",
                        "Accept-Encoding": "gzip, deflate, br",
                        "DNT": "1",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                    },
                )

                page = context.new_page()

                # Disable images, CSS, and fonts to speed up page load
                page.route(
                    "**/*.{png,jpg,jpeg,gif,webp,svg,ico}",
                    lambda route: route.abort(),
                )
                page.route(
                    "**/*.{woff,woff2,ttf,eot}", lambda route: route.abort()
                )

                # Navigate to the page
                page.goto(url)

                # Wait for content to load
                page.wait_for_load_state("networkidle", timeout=60_000)

                # Get the page HTML
                return page.content()
            finally:
                browser.close()
