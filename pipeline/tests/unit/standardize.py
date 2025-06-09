"""Unit tests for value standardization functions.
"""

import pandas as pd
import pytest
from decimal import Decimal
from pipeline.transform.finance import CurrencyClient
from pipeline.transform.names import NameStandardizer


@pytest.fixture()
def currency_client() -> CurrencyClient:
    """An instance of a client used to normalize currencies.
    """
    return CurrencyClient()

@pytest.fixture()
def name_standardizer() -> NameStandardizer:
    """An instance of a client used to standardize names.
    """
    return NameStandardizer()

@pytest.mark.parametrize(
    "input,output", [("kosovo*,indien", "India, Kosovo")]
)
def test_map_country_names(
    input: str,
    output: str,
    name_standardizer: NameStandardizer) -> None:
    """Tests that the standardizer correctly maps country
    names to their standard representation in the data store,
    if a standard exists, and returns the names in ascending
    alphabetical order.

    Args:
        input (`str`): The raw input list of countries to test
            (e.g., "kosovo*,indien").
        
        output (`str`): The expected standardized country names
            (e.g., "India, Kosovo").

        name_standardizer (`NameStandardizer`): An instance of a 
            client used to standardize names.

    Returns:
        `None`
    """
    # Prepare data
    df = pd.DataFrame([{"id": 1, "countries": input}])

    # Map countries to new values
    output_df1, output_df2 = name_standardizer.map_country_names(
        raw_countries_df=df,
        id_field='id',
        country_field='countries'
    )

    # Assert that each output DataFrame has expected values, in sorted order
    assert output_df1.countries.tolist() == output.split(", ")
    assert output_df2.countries.tolist()[0] == output

@pytest.mark.parametrize(
    "input,output", [
        (
            "ac - mini-mills,f-ab - sugar and confectionery", 
            "Agribusiness, Manufacturing"
        )
    ]
)
def test_map_project_sector(
    input: str,
    output: str,
    name_standardizer: NameStandardizer) -> None:
    """Tests that the standardizer correctly maps project
    sector names to their standard representation in the data
    store, if a standard exists, and returns the names in 
    ascending alphabetical order.

    Args:
        input (`str`): The raw input list of sectors to test
            (e.g., "ac - mini-mills,f-ab - sugar and confectionery").
        
        output (`str`): The expected standardized sector
            names (e.g., "Agribusiness, Manufacturing").

        name_standardizer (`NameStandardizer`): An instance of a 
            client used to standardize names.

    Returns:
        `None`
    """
    # Prepare data
    df = pd.DataFrame([{"id": 1, "sectors": input}])

    # Map sectors to new values
    output_df1, output_df2 = name_standardizer.map_project_sectors(
        raw_sectors_df=df,
        id_field='id',
        sector_field='sectors'
    )

    # Assert that each output DataFrame has expected values, in sorted order
    assert output_df1.sectors.tolist() == output.split(", ")
    assert output_df2.sectors.tolist()[0] == output

@pytest.mark.parametrize(
    "input,output", [
        (
            ["board approved, pending signing", "dropped"],
            ["Pending", "Cancelled"]
        )
    ]
)
def test_map_project_statuses(
    input: str,
    output: str,
    name_standardizer: NameStandardizer) -> None:
    """Tests that the standardizer correctly maps project
    statuses to their standard representation in the data
    store, if a standard exists.

    Args:
        input (`list` of `str`): The raw input list of statuses to test
            (e.g., `["board approved, pending signing", "dropped"]`).
        
        output (`str`): The expected standardized status
            names (e.g., `["Cancelled", "Pending"]`).

        name_standardizer (`NameStandardizer`): An instance of a 
            client used to standardize names.

    Returns:
        `None`
    """
    # Prepare data
    ids = list(range(1, len(input) + 1))
    df = pd.DataFrame(list(zip(ids, input)), columns=["id", "status"])

    # Map statuses to new values
    output_series = name_standardizer.map_project_statuses(df.status)

    # Assert that the output series has expected values
    assert output_series.tolist() == output

@pytest.mark.parametrize(
    "year,country_code,currency_code,amount,expected", [
        ( 1994, "US", "USD", Decimal("50"), Decimal("76.26")),
        ( 2017, "US", "USD", Decimal("100"), Decimal("100")),
        ( 2022, "US", "USD", Decimal("100"), Decimal("84.77")),
        ( 1980, "FR", "EUR", Decimal("100"), Decimal("367.39"))
    ]
)
def test_normalize_currency(
    year: int,
    country_code: str,
    currency_code: str,
    amount: Decimal,
    expected: Decimal,
    currency_client: CurrencyClient) -> None:
    """Tests that the currency client correctly normalizes
    monetary amounts across time (i.e., years) and space
    (i.e., currency types for countries) to equal 2017
    U.S. dollars.
    
    NOTE: As a sanity check, one can convert
    between USD amounts for different years using an online
    calculator that utilizes the Bureau of Economic Analysis'
    implicit price deflators for GDP (index=2017):
    https://stats.areppim.com/calc/calc_usdlrxdeflator.php

    Args:
        year (`int`): The year of the currency.

        country_code (`str`): The two-digit code of the country
            where the currency was used, as defined by ISO 3166.

        currency_code (`str`): The three-digit currency code,
            as defined by ISO 4217.

        amount (`decimal.Decimal`): The amount of the currency.

        expected (`decimal.Decimal`): The expected normalized 
            currency amount.

        currency_client (`CurrencyClient`): An instance of a 
            client used to normalize currencies.

    Returns:
        `None`
    """
    amount = currency_client.normalize(
        year,
        country_code,
        currency_code,
        amount
    )
    assert amount == expected
