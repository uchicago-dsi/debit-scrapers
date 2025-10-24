# debit-scrapers

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

This monorepo houses open source data scrapers for **[DeBIT](https://debit.datascience.uchicago.edu)** (**De**velopment **B**ank **I**nvestment **T**racker), an online research tool developed by **[Inclusive Development International](https://www.inclusivedevelopment.net/)** and the **[University of Chicago Data Science Institute (DSI)](https://datascience.uchicago.edu/)**. DeBIT empowers community advocates to track investments made by development finance institutions and other entities that have independent accountability mechanisms (IAMs). Every two weeks, its scrapers extract data from all investment projects publicly disclosed by the following 17 institutions:

- African Development Bank (AfDB)
- Asian Development Bank (ADB)
- Asian Infrastructure Investment Bank (AIIB)
- Belgian Investment Company for Developing Countries (BIO)
- Deutsche Investitions - und Entwicklungsgesellschaft (DEG)
- U.S. International Development Finance Corporation (DFC)
- European Bank for Reconstruction and Development (EBRD)
- European Investment Bank (EIB)
- Inter-American Development Bank (IDB)
- International Finance Corporation (IFC)
- Kreditanstalt für Wiederaufbau (KfW)
- Multilateral Investment Guarantee Agency (MIGA)
- Nederlandse Financierings-Maatschappij voor Ontwikkelingslanden (FMO)
- Norges Bank Investment Management (NBIM)
- Proparco
- United Nations Development Programme (UNDP)
- World Bank (WB)

These scraped projects are then aggregated, cleaned, and standardized to provide a searchable **["database"](https://debit.datascience.uchicago.edu/database)** of projects by date, country, bank, completion status, and original loan amount, among other fields.

## Structure

### docs

[In Progress] The documentation website. Scaffolded with Astro Starlight. Describes the architectural patterns, infrastructure, and data schemas used for the backend.

### infra

Bash and Python scripts used to deploy the backend infrastructure to the cloud. Currently utilizes Pulumi to deploy to Google Cloud Platform.

### services

Independently deployable stages of the DeBIT backend data processing pipeline.

- **extract**: A Django project for extracting development project data. Contains the business logic of the scrapers, as well as management commands and API endpoints for triggering and monitoring scraper workflows.

- **transform**: Packages for transforming the scraped data into cleaned output. Currently standarizes the data columns and performs nominal currency conversions of the project financing amounts.

- **map**: Packages for mapping the cleaned data to the schema expected by the DeBIT website.

## Getting Started

To run the application locally for development and testing, follow the setup instructions below.

### Dependencies

- Make
- Docker Desktop

### Setup

This project requires use of a bash shell and the ability to run Docker. If you are just getting started, you can implement this suggested setup:

(1) Install **[Windows Subsystem for Linux (WSL2)](https://docs.microsoft.com/en-us/windows/wsl/install)** if you are using a Windows PC.

(2) Install the **[Docker Desktop](https://docs.docker.com/desktop/)** version corresponding to your operating system.

(3) Clone the repository to your local machine.

SSH: `git@github.com:uchicago-dsi/debit-scrapers.git`

HTTP: `https://github.com/uchicago-dsi/debit-scrapers.git`

Windows users should clone the repo in their WSL file system for the **[fastest performance](https://docs.microsoft.com/en-us/windows/wsl/filesystems#file-storage-and-performance-across-file-systems)**.

(4) Install **[make](https://sites.ualberta.ca/dept/chemeng/AIX-43/share/man/info/C/a_doc_lib/aixprggd/genprogc/make.htm)** for your operating system. On macOS and Windows Subsystem for Linux, which runs on Ubuntu, make should be installed by default, which you can verify with `make --version`. If the package is not found, install build-essential (e.g., `sudo apt-get install build-essential`) and then reattempt to verify. If you are working on a Windows PC outside of WSL, follow the instructions **[here](https://gist.github.com/evanwill/0207876c3243bbb6863e65ec5dc3f058)**.

(5) Navigate to the `services` directory and follow the instructions in the `extract`, `transform`, or `map` subfolder to scrape, clean, or map data, respectively.
s

## Contributions

DeBIT is part of a larger open source initiative at the DSI and will continue to be maintained by its staff and contributors from around the world.

### Suggested Feature Requests

The GitHub project board will hold a list of suggested issues that the DeBIT team has identified as high priority. The workflow for contributions in this case is as follows:

- Assign yourself to the issue.
- Fork the repository and clone it locally. Add the original upstream repository as a remote origin and pull in new changes frequently.
- Create a branch for your edits; the name should contain the issue number and a one-to-five word description.
- Make commits of logical units while ensuring that commit messages are in the [proper format](https://cbea.ms/git-commit/).
- Add unit tests for your feature within the `./src/pipeline/extract/tests` directory.
- Submit a pull request to `dev` and reference any supporting issues or documentation.
- Wait for your PR to be reviewed by at least one maintainer found in the `CODEOWNERS` file. If further changes are requested, you will be notified in a tagged comment. If your PR is approved, the maintainers will squash all commits into one, and you will be notified through an email and tagged comment that your work has been successfully merged.

### New Feature Requests

Contributors are encouraged to consider new features that would increase the number of projects available to community advocates and researchers; improve the accuracy and consistency of scraped project fields; and lead to greater efficiency and etiquette in web scraping. When proposing a new issue, tag the maintainers listed in the `CODEOWNERS` file. Following the submission, one of the code maintainers will contact you to approve, modify, or table the suggested enhancement to another time based on other open issues. Once the issue is approved, you can follow the same workflow to commit and submit your changes.
