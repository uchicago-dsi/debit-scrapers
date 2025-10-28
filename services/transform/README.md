# transform

A service for cleaning scraped development bank project records.

Consists of a single package, `clean_raw`, that fetches a compressed CSV file of records from a local or remote data store and then transforms the data by applying standard casing and formatting; standardizing categorical values for countries, sectors, and project statuses across banks; transforming delimited columns to lists; and performing nominal conversions from local currencies to U.S. dollars for the project financing year.

## Setup

After installing the required project dependencies described in the repository's main `README.md`, create an `.env.dev` file with the following contents and save it under the current transform directory.

```
# Environment
ENV='dev'
```

### Entrypoints

The service's Makefile provides simple entrypoints for running the application locally as a Docker container. A few pointers:

- All of the commands listed below must be run directly under the current directory.

- Services can be shut down at any point by entering `CTRL-C` or, for services executing in the background, `CTRL-D`. This automatically shuts, stops, and destroys the active Docker containers.

#### Build Image

Builds a Docker image of the data cleaning pipeline.

```bash
make build-clean
```

#### Run Pipeline

Builds and runs the pipeline script as a Docker container while mounting the directories `./services/transform/input` and `./services/transform/output` as volumes.

Download the scraped project file generated from the previous data pipeline step (i.e., data extraction) from Google Cloud Storage and save it under the input directory. After the script runs, a cleaned Parquet file is generated and saved under the output directory.

```bash
make run-clean
```

## Implementation

### Data Standardization

The scraped data sources use different sets of names for countries, economic sectors, project statuses, and project financing types. To standardize the values, light entity resolution was performed manually and with the help of LLMs to generate a set of mapping files:

**country_map.json**: Maps standard country names to lists of lowercase, scraped country names.

**currency_map.json**: Maps **[ISO 4217](https://www.iso.org/iso-4217-currency-codes.html)** three-letter currency codes to lists of lowercase, scraped currency codes.

**finance_type_map.json**: Maps standard finance types to lists of lowercase, scraped finance types.

**sector_map.json**: Maps lowercase, scraped sector names to industries pulled from the **[Global Industry Classification Standard (GICS)](https://www.spglobal.com/spdji/en/landing/topic/gics/)**.

**status_map.json**: Maps standard project statuses to lists of lowercase, scraped project statuses.

This strategy is not sustainable given that categorical values can shift at any time, which would require the affected mapping to be updated. As LLMs improve, they should be able to produce mapping on the fly, without human oversight. This could be introduced as a new feature in the near future.

### Currency Conversion

To perform nominal currency conversions—i.e., direct conversions between currency units without consideration of countries' purchasing power—we first identify the year each project's financing took place. Unfortunately, this process is relatively subjective. The bank's financial commitment is _typically_ reported relative to when it was first signed or approved, but only a few of the banks explicitly confirm this fact on their websites and many only report other dates within the lifecycle of their loans and investments, such as the disclosure, appaisal, or effective date. To handle this variability, the package searches for the following date fields in order and selects the first available:

- `date_signed`
- `date_approved`
- `date_diclosed`
- `date_under_appraisal`
- `date_effective`
- `fiscal_year_effective`
- `date_planned_effective`

Next, the package fetches the conversion rate from the project's local currency (e.g., EUR, INR) to U.S. Dollars (e.g., "USD") for that financing year. The package currently uses annual and quartlery **[historical exchange rates](https://data.imf.org/en/datasets/IMF.STA:ER)** from the International Monetary Fund (IMF) for this purpose.

Once the rates are in hand, the package performs the conversion by simply multipying each project's financial commitment with the conversion rate. Once the calculation is complete, the following fields are added to the dataset:

- `conversion_year`
- `conversion_rate`
- `converted_amount_usd`

NOTE: If the conversion fails for whatever reason (e.g., none of the date fields used to determine the transaction year are populated or a conversion rate cannot be found), `conversion_year` will be an empty string and `conversion_rate` and `converted_amount_usd` will each be `None`.
