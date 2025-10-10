"""Generic module for converting nominal currency amounts."""

# Standard library imports
import json
from typing import Literal

# Third-party imports
import numpy as np
import pandas as pd
from django.conf import settings

# Application imports
from common.http import DataRequestClient


class CurrencyConverter:
    """Performs nominal currency conversions for a given year."""

    def __init__(self) -> None:
        """Initializes a new instance of a `CurrencyConverter`.

        Args:
            `None`

        Returns:
            `None`
        """
        # Initialize data request client
        self._data_request_client = DataRequestClient()

        # Load crosswalk from currency code to representative country code
        self._currency_crosswalk = self._load_currency_crosswalk()

        # Build lookup for annual rates
        annual_rates = self._load_exchange_rates(frequency="A")
        self._annual_lookup = self._build_annual_rate_lookup(annual_rates)

        # Build lookup for monthly rates
        monthly_rates = self._load_exchange_rates(frequency="M")
        self._monthly_lookup = self._build_monthly_rate_lookup(monthly_rates)

    @property
    def european_currency_unit_start_year(self) -> int:
        """The year in which the European currency unit (ECU) was adopted."""
        return 1979

    @property
    def european_currency_unit_end_year(self) -> int:
        """The year in which use of the European currency unit (ECU) ended."""
        return 1999

    @property
    def euro_start_year(self) -> int:
        """The year in which the Euro was adopted."""
        return 1999

    @property
    def imf_euro_area_code(self) -> str:
        """The International Monetary Fund's code for the Euro area."""
        return "G163"

    def _build_annual_rate_lookup(self, annual_rates: pd.DataFrame) -> dict:
        """Builds a lookup table for annual exchange rates.

        Args:
            annual_rates: A DataFrame of annual exchange rates.

        Returns:
            A dictionary mapping a country code and year to the
                exchange rate for that year.
        """
        # Group annual exchange rates by country and convert to dict
        country_rates = annual_rates.set_index("COUNTRY").to_dict(
            orient="index"
        )

        # Create lookup for annual exchange rates
        lookup = {}
        for country_code, rates in country_rates.items():

            # Parse rates into DataFrame, with each row representing a year
            country_df = pd.DataFrame([rates]).T.reset_index()

            # Rename columns
            country_df.columns = ["YEAR", "VALUE"]

            # Transform values from strings to floats
            country_df["VALUE"] = country_df["VALUE"].astype(float)

            # Replace missing values
            country_df = country_df.replace({np.nan: None})

            # Create lookup key
            country_df["KEY"] = country_code + "-" + country_df["YEAR"]

            # Create country lookup
            country_lookup = country_df.set_index("KEY")["VALUE"].to_dict()

            # Update master lookup
            lookup.update(country_lookup)

        # Add fixed rate from Euros to U.S. dollars for years in
        # which the European currency unit (ECU) was in effect
        # NOTE: The ECU was replaced by the Euro at a ratio of 1:1.
        fixed_rate = self._get_fixed_euro_rate(annual_rates)
        for year in range(
            self.european_currency_unit_start_year,
            self.european_currency_unit_end_year,
        ):
            lookup[f"{self.imf_euro_area_code}-{year}"] = fixed_rate

        return lookup

    def _build_monthly_rate_lookup(self, monthly_rates: pd.DataFrame) -> dict:
        """Builds a lookup table for monthly exchange rates.

        Args:
            monthly_rates: A DataFrame of monthly exchange rates.

        Returns:
            A dictionary mapping a country code and year to the
                average exchange rate for the months in that year.
        """
        # Group monthly exchange rates by country and convert to dict
        country_rates = monthly_rates.set_index("COUNTRY").to_dict(
            orient="index"
        )

        # Create lookup for averaged monthly exchange rates
        lookup = {}
        for country_code, rates in country_rates.items():

            # Parse rates into DataFrame, with each row representing a reporting period
            country_df = pd.DataFrame([rates]).T.reset_index()

            # Rename columns
            country_df.columns = ["PERIOD", "VALUE"]

            # Extract year from repoting period
            country_df["YEAR"] = country_df["PERIOD"].str[:4]

            # Transform values from strings to floats
            country_df["VALUE"] = country_df["VALUE"].astype(float)

            # Aggregate rates by year and calculate mean
            country_df = (
                country_df[["YEAR", "VALUE"]]
                .groupby("YEAR")
                .apply(lambda s: s.mean())
                .reset_index()
            )

            # Replace missing values
            country_df = country_df.replace({np.nan: None})

            # Create lookup key
            country_df["KEY"] = country_code + "-" + country_df["YEAR"]

            # Create country lookup
            country_lookup = country_df.set_index("KEY")["VALUE"].to_dict()

            # Update master lookup
            lookup.update(country_lookup)

        return lookup

    def _get_fixed_euro_rate(self, annual_rates_df: pd.DataFrame) -> float:
        """Fetches the Euro to U.S. dollar exchange rate for 1999.

        Args:
            annual_rates_df: A DataFrame containing annual exchange rates.

        Returns:
            The exchange rate.
        """
        is_euro_area = annual_rates_df["COUNTRY"] == self.imf_euro_area_code
        euro_rates_df = annual_rates_df[is_euro_area]
        euro_adopt_rate = euro_rates_df[str(self.euro_start_year)].iloc[0]
        return float(euro_adopt_rate)

    def _load_currency_crosswalk(self) -> dict:
        """Loads a currency crosswalk from a JSON file.

        Args:
            `None`

        Returns:
            A dictionary mapping currency codes to their names.
        """
        with open(settings.CURRENCY_CROSSWALK_FPATH, encoding="utf-8") as f:
            return json.load(f)

    def _load_exchange_rates(
        self, frequency: Literal["A", "M" "Q"]
    ) -> pd.DataFrame:
        """Loads exchange rates from the International Monetary Fund (IMF).

        NOTE: Here, rates represent conversions from domestic/local currency
        amounts to U.S. dollars for a given time period (i.e., a year, month,
        or quarter). The data spans 1940 through the present and includes all
        countries available through the IMF API.

        References:
        - https://portal.api.imf.org/api-details#api=idata-sdmx-api-3-0&operation=get-data-context-agencyid-resourceid-version-key

        Args:
            frequency: The desired publishing frequency of the exchange rates.
                Either "A" for annual, "M" for monthly, or "Q" for quarterly.

        Returns:
            The exchange rates, reshaped as a DataFrame.
        """
        # Fetch data from the public API
        url = f"https://api.imf.org/external/sdmx/3.0/data/dataflow/IMF.STA/ER/4.0.1/*.USD_XDC.PA_RT.{frequency}"
        r = self._data_request_client.get(url, timeout=60)
        if not r.ok:
            raise RuntimeError(
                "Error fetching IMF exchange rate data. "
                f"The request failed with a "
                f'"{r.status_code} - {r.reason}" status '
                f'code and the message "{r.text}".'
            )

        # Parse response
        payload = r.json()

        # Read data into Pandas DataFrame
        series = payload["data"]["dataSets"][0]["series"]
        df = [pd.DataFrame(series[i]["observations"]).loc[0] for i in series]
        df = pd.DataFrame(df)

        # Finalize columns
        dimensions = payload["data"]["structures"][0]["dimensions"]
        dates = [d["value"] for d in dimensions["observation"][0]["values"]]
        countries = [c["id"] for c in dimensions["series"][0]["values"]]
        df.columns = dates
        df["COUNTRY"] = countries
        df = df[["COUNTRY"] + sorted(dates)]

        return df

    def get_usd_exchange_rate(self, currency: str, year: str) -> float | None:
        """Fetches an exchange rate to USD for the given currency and year.

        Args:
            currency: The currency to convert to USD.

            year: The year to fetch the exchange rate for.

        Returns:
            The exchange rate to USD, or `None`
                if the rate cannot be determined.
        """
        # Terminate if currency or year is missing
        if not currency or not year:
            return None

        # Return 1 if currency is USD
        if currency == "USD":
            return 1.0

        # Terminate if currency is not registered in crosswalk
        country_code = self._currency_crosswalk.get(currency)
        if not country_code:
            return None

        # Otherwise, build lookup key
        lookup_key = f"{country_code}-{year}"

        # Attempt to fetch rate based on annual averages
        rate = self._annual_lookup.get(lookup_key)

        # If not available, attempt to fetch rate based on monthly averages
        if not rate:
            rate = self._monthly_lookup.get(lookup_key)

        return rate
