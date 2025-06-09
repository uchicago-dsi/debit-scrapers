"""Clients for fetching and normalizing financial data.
"""

import json
import logging
import math
import os
import pandas as pd
import requests
import sdmxthon
import time
from decimal import Decimal
from pipeline.constants import (
    COUNTRY_CODES_FPATH,
    CURRENCY_CODES_FPATH,
    FRED_API_BASE_URL,
    FRED_API_KEY,
    OPEN_FIGI_API_KEY,
    OPEN_FIGI_API_BASE_URL,
    OPEN_FIGI_MAX_JOBS_PER_REQUEST,
    OPEN_FIGI_MAX_REQUESTS_PER_WINDOW,
    OPEN_FIGI_REQUEST_WINDOW_SECONDS
)
from pipeline.services.logger import logging
from typing import Any, List


class CurrencyClient:
    """An interface for computing foreign exchange rates
    and deflation rates for the purpose of normalizing
    arbitrary monetary amounts to USD.
    """

    def __init__(self) -> None:
        """Initializes a new instance of a `CurrencyClient`.

        Args:
            `None`

        Returns:
            `None`
        """
        # Parse environment variables
        try:
            self._api_key = os.environ[FRED_API_KEY]
            self._api_base_url = os.environ[FRED_API_BASE_URL]
        except KeyError as e:
            raise RuntimeError("Missing environment variable "
                               f"\"{e}\".") from None
        
        # Initialize data stores
        self.exchange_rates = self._get_exchange_rates()
        self.gdp_deflators = self._get_gdp_price_deflators()  

    def _get_exchange_rates(self) -> pd.DataFrame:
        """Fetches all bilateral, nominal, annual USD exchange
        rates available from the Bank for International 
        Settlements (BIS) Data Portal (192 countries total).

        Documentation:
        - [Dataset Description](https://data.bis.org/topics/XRU)
        - [Dataset Documentation](https://www.bis.org/statistics/xrusd/xrusd_doc.pdf)

        Args:
            `None`

        Returns:
            (`pd.DataFrame`): The rates as a DataFrame.
        """
        # Load exchange rates using third-party SDMX library
        try:
            url = "https://stats.bis.org/api/v1/data/BIS,WS_XRU,1.0/all"
            message_data = sdmxthon.read_sdmx(url)
            df = message_data.content["BIS:WS_XRU(1.0)"].data.copy()
        except Exception as e:
            raise RuntimeError("Failed to fetch foreign exchange "
                               f"rates. \"{e}\"") from None
                
        # Reshape rates DataFrame
        col_map = {
            "TIME_PERIOD": "year",
            "REF_AREA": "country_code",
            "CURRENCY": "currency_code",
            "COLLECTION": "data_type",
            "OBS_VALUE": "exchange_rate"
        }
        df = (df
            .query("(FREQ == 'A') & (COLLECTION == 'E')")
            .loc[:, list(col_map.keys())]
            .rename(columns=col_map)
            .sort_values(by=["country_code", "year"]))
        
        # Transform columns
        df["year"] = df["year"].astype(int)

        # Load and format country codes
        iso_codes = pd.read_csv(COUNTRY_CODES_FPATH)
        iso_codes = iso_codes[["alpha2", "en"]]
        iso_codes["alpha2"] = iso_codes["alpha2"].str.upper()
        iso_codes = iso_codes.rename(columns={"en": "country_name"})

        # Merge rates with country codes
        df = df.merge(
            right=iso_codes,
            left_on="country_code",
            right_on="alpha2")
        
        # Load and format currency codes
        currency_codes = pd.read_csv(CURRENCY_CODES_FPATH)
        currency_codes = currency_codes.rename(columns={
            "unit_text": "currency_name"
        })
        
        # Merge rates with currency codes
        df = df.merge(
            right=currency_codes[["currency", "currency_name"]],
            left_on="currency_code",
            right_on="currency")
        
        # Subset and return final DataFrame
        return df[[
            "year",
            "country_code",
            "country_name",
            "currency_code",
            "currency_name",
            "exchange_rate"
        ]]

    def _get_gdp_price_deflators(self) -> pd.DataFrame:
        """Fetches implicit price deflators for the U.S. gross
        domestic product from 1921 through the present from
        the Federal Reserve Economic Data (F.R.E.D.), an online
        database hosted by the Federal Reserve Bank of St. Louis.
        Observations are aggregated annually and not seasonally
        adjusted. The original data source is the U.S. Bureau of
        Ecomonic Analysis (BEA).

        Documentation:
        - [Dataset Description](https://fred.stlouisfed.org/series/A191RD3A086NBEA#0)
        - [API Documentation](https://fred.stlouisfed.org/docs/api/fred/series_observations.html)
        
        Args:
            `None`

        Returns:
            (`pd.DataFrame`): The observations as a DataFrame
                with date and value columns.
        """
        # Request price deflator series
        url = (f"{self._api_base_url}/series/observations?series_id="
               f"A191RD3A086NBEA&api_key={self._api_key}&file_type=json")
        r = requests.get(url)
        if not r.ok:
            raise RuntimeError(f"The API returned a \"{r.status_code}-"
                               f"{r.reason}\" status code with the "
                               f"message \"{r.text}\".")
        
        # Parse observations from response body
        try:
            observations = r.json()["observations"]
            if not observations:
                raise AssertionError("An unexpected error occurred. "
                                     "No observations found.")
        except (json.JSONDecodeError, AssertionError) as e:
            raise RuntimeError(f"Failed to parse observations "
                               f"from payload. \"{e}\".") from None
        except KeyError:
            raise RuntimeError("Failed to parse observations from "
                               "payload. Missing expected key "
                               "\"observations\".") from None
        
        # Read observations into DataFrame
        try:
            df = pd.DataFrame(observations)[["date", "value"]]
            df["year"] = pd.to_datetime(df["date"]).dt.year
            return df[["year", "value"]]
        except KeyError as e:
            raise RuntimeError(f"An unexpected error occurred. "
                               f"The data schema changed. \"{e}\".") from None
        except Exception as e:
            raise RuntimeError("An unexpected error occurred. "
                               "Unable to parse U.S. GDP deflator "
                               f"date column.") from e

    def normalize(
        self,
        year: int,
        country_code: str,
        currency_code: str, 
        amount: Decimal) -> Decimal:
        """Normalizes a monetary amount expressed in a currency,
        year, and country by converting the amount to 2017 U.S. 
        dollars (i.e., the latest index offered by the U.S. 
        Bureau of Economic Analysis at the time of writing).
        
        NOTE: Here, currencies' country of origin must be 
        considered even when using nominal exchange rates due
        to slight variations in data reporting, retroactive exchange
        rate calculations (e.g., estimating Euro values before
        the currency's creation), and unofficial currency 
        eadoptions (e.g., Montenegro with the Euro in 2002).

        Args:
            year (`int`): The year of the currency.

            country_code (`str`): The two-digit country code,
                as defined by ISO 3166.

            currency_code (`str`): The three-digit currency code,
                as defined by ISO 4217.

            amount (`decimal.Decimal`): The amount.

        Raises:
            - `KeyError` if the currency code or year 
            has not been registered.

        Returns:
            (`Decimal`): The exchange rate. 
        """
        # Look up exchange rate for given currency and year
        rates = self.exchange_rates.query(
                        "(currency_code == @currency_code) & "
                        "(year == @year) & "
                        "(country_code == @country_code)")
        if rates.empty:
            raise KeyError("No exchange rate exists for currency "
                        f"\"{currency_code}\" in country "
                        f"\"{country_code}\" and year {year}.")
        
        # Look up USD deflator for given year
        year_deflators = self.gdp_deflators.query("year == @year")
        if year_deflators.empty:
            raise KeyError(f"No USD deflation rate exists for the year {year}.")

        # Normalize amount
        exchange_rate = 1 / Decimal(rates.iloc[0]["exchange_rate"])
        deflation_rate = 100 / Decimal(year_deflators.iloc[0]["value"])
        normalized = amount * exchange_rate * deflation_rate

        return round(normalized, ndigits=2)

class StocksClient:
    """An interface for fetching stock values and metadata.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """Initializes a new instance of a `StocksClient`.

        Args:
            logger (`logging.Logger`): An instance of
                a standard logger.

        Raises:
            (`RuntimeError`) if any of the following environment variables
                are not present, or if their value is of an unexpected type: 

            - `OPEN_FIGI_API_KEY`
            - `OPEN_FIGI_API_BASE_URL`
            - `OPEN_FIGI_MAX_JOBS_PER_REQUEST`
            - `OPEN_FIGI_MAX_REQUESTS_PER_WINDOW`
            - `OPEN_FIGI_REQUEST_WINDOW_SECONDS`
                
        Returns:
            `None`
        """
        # Set logger
        self._logger = logger

        # Parse environment variables
        try:
            self._api_key = os.environ[OPEN_FIGI_API_KEY]
            self._api_base_url = os.environ[OPEN_FIGI_API_BASE_URL]
            self._max_jobs_per_request = int(
                os.environ[OPEN_FIGI_MAX_JOBS_PER_REQUEST]
            )
            self._max_requests_per_window = int(
                os.environ[OPEN_FIGI_MAX_REQUESTS_PER_WINDOW]
            )
            self._request_window_seconds = int(
                os.environ[OPEN_FIGI_REQUEST_WINDOW_SECONDS]
            )
        except KeyError as e:
            raise RuntimeError("Missing environment variable "
                               f"\"{e}\".") from None
        except ValueError as e:
            raise RuntimeError("Failed to cast environment "
                            "variable to expected data type. "
                            f"\"{e}\".") from None

    def fetch_stock_metadata(self, cusips: List[str]) -> pd.DataFrame:
        """Fetches metadata (e.g., market sector, ticker symbol)
        for one or more stocks given their CUSIP numbers--unique
        nine-digit identification numbers assigned to stocks and
        registered bonds in the U.S. and Canada. The dataset is
        fetched from the Open Figi API run by the Object Management
        Group, an international, open membership, nonprofit
        technology standards consortium. NOTE: Requests are throttled,
        with rates based on whether an API key is used.

        Documentation:
        - [Rate Limit](https://www.openfigi.com/api#rate-limit)
        - [POST /v3/mapping](https://www.openfigi.com/api#post-v3-mapping)

        Args:
            cusips (`list` of `str`): The CUSIP numbers.

        Returns:
            (`pd.DataFrame`): A DataFrame in which each row holds
                a CUSIP number and its stock metadata. 
        """
        # Initialize variables
        num_requests = 0
        stock_metadata = []
        mapping_url = f"{self._api_base_url}/v3/mapping"
        batch_size = self._max_jobs_per_request
        headers = {
            "Content-Type": "application/json",
            "X-OPENFIGI-APIKEY": self._api_key
        }

        # Define local function to batch cusips
        def yield_batch(
            elements: List[Any], 
            batch_size: int) -> List[Any]:
            """Batches a list of elements into smaller lists of the
            given size or smaller and yields one batch at a time.

            Args:
                elements (`list` of `any`): The elements to batch.

                batch_size (`int`): The maximum size of the batch. For
                    example, if there are 11 elements and a batch size
                    of 2 is given, the first five batches will be
                    of size two and the last batch will be of size one
                    to handle the remainder.

            Yields:
                (`list` of `any`): A batch.
            """
            num_batches = math.ceil(len(elements) / batch_size)
            last_index = len(elements) - 1
            for i in range(num_batches):
                is_last_batch = i == num_batches - 1
                start_idx = i * batch_size
                end_idx = last_index if is_last_batch else (i + 1) * batch_size
                yield elements[start_idx:end_idx]
        
        # Process each cusip
        for batch in yield_batch(cusips, batch_size):

            # Map batch of cusips to expected request body
            lookups = [{"idType": "ID_CUSIP", "idValue": c} for c in batch]

            # Make request to map third party identifiers to FIGIs
            self._logger.info("Requesting stock metadata for CUSIPs: "
                              ", ".join(f"\"{c}\"" for c in cusips))
            response = requests.post(
                url=mapping_url,
                headers=headers,
                json=lookups
            )
            num_requests += 1

            # Sleep and reattempt call if throttled
            if response.status_code == 429:
                self._logger.warn("Attempted too many calls. Sleeping "
                                  f"for {self._request_window_seconds} "
                                  "seconds.")
                time.sleep(self._request_window_seconds)
                response = requests.post(
                    url=mapping_url,
                    headers=headers,
                    json=lookups
                )
                num_requests += 1

            # Raise an exception if server returns an error
            if not response.ok:
                raise RuntimeError("Open FIGI server returned "
                                    "an error with the message: "
                                    f"\"{response.json()}\".")
            
            # Sleep if max requests has been reached
            if num_requests % self._max_requests_per_window == 0:
                time.sleep(self._request_window_seconds)

            # Collect stock metadata from response JSON
            for result in response.json():
                if "data" in result:
                    metadata = result["data"][0]
                    metadata["exchCode"] = ", ".join(
                        r["exchCode"] 
                        for r in result["data"] 
                        if r["exchCode"]
                    )
                    stock_metadata.append(metadata)
                else:
                    stock_metadata.append({
                        "figi": "",
                        "name": "",
                        "ticker": "",
                        "exchCode": "",
                        "compositeFIGI": "",
                        "securityType": "",
                        "marketSector": "",
                        "shareClassFIGI": "",
                        "securityType2": "",
                        "securityDescription": ""
                    })
                
        # Read stock metadata into DataFrame
        stock_metadata_df = pd.DataFrame(stock_metadata)
        stock_metadata_df["cusip"] = list(cusips)

        return stock_metadata_df
