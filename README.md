# debit-scrapers

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

This repository houses open source web scrapers for [DeBIT](https://debit.datascience.uchicago.edu) (**De**velopment **B**ank **I**nvestment **T**racker), an online research tool developed by [Inclusive Development International](https://www.inclusivedevelopment.net/) and the [University of Chicago Data Science Institute](https://datascience.uchicago.edu/). DeBIT empowers community advocates to track investments made by development finance institutions and other entities that have independent accountability mechanisms (IAMs). Every two weeks, its scrapers extract data from all investment projects publicly disclosed by the following 17 institutions:

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

These scraped projects are then aggregated, cleaned, and standardized to provide a searchable ["database"](https://debit.datascience.uchicago.edu/database) of projects by date, country, bank, completion status, and original loan amount, among other fields. The projects are also merged with available IAM complaint data from [Accountability Counsel](https://www.accountabilitycounsel.org/) to generate an analysis of complaints by bank, sector, country, and complaint issue type.

DeBIT is part of a larger open source initiative at the Data Science Institute (DSI) and will continue to be maintained by the DSI and contributors from around the world. Please see below for instructions on how to set up the repo, run the scrapers, and contribute scrapers for new institutions.

## Installation

This project requires use of a bash shell and the ability to run Docker. If you are just getting started, you can implement this suggested setup:

(1) Install [Windows Subsystem for Linux (WSL2)](https://docs.microsoft.com/en-us/windows/wsl/install) if you are using a Windows PC.

(2) Install the [Docker Desktop](https://docs.docker.com/desktop/) version corresponding to your operating system.

(3) Clone the repository to your local machine.

SSH: `git@github.com:chicago-cdac/debit-scrapers.git`

HTTP: `https://github.com/chicago-cdac/debit-scrapers.git`

Windows users should clone the repo in their WSL file system for the [fastest performance](https://docs.microsoft.com/en-us/windows/wsl/filesystems#file-storage-and-performance-across-file-systems).


## Usage

### Running Scrapers as Standalone Scripts

Scrapers can be executed as standalone Python scripts for local testing. To accomplish this, first create and activate a virtual environment and then install local and PyPI-hosted packages through pip.

Linux Example:
```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
pip install --e .
python3 scrapers/banks/adb.py
```

### Running Scrapers within a Distributed Process

After testing scraping functions in isolation, you may want to crawl an entire bank's website to fetch all project records. Doing so will help you better identify edge cases for HTML parsing, given that webpages published in different years often vary in format.

The following commands will spin up a new multi-container Docker application:

```
make build
make run
```

Services within the application include: 

- `debit-scrapers-database`: PostgreSQL instance. Persists records of queued web scraping jobs and tasks as well as scraped project data.

- `debit-scrapers-api`: Django API instance. Sits in front of the database and provides endpoints for inserting and retrieving jobs, tasks, and projects.

- `debit-scrapers-pubsub`: Google Pub/Sub emulator. Serves as a messaging queue for web scraping tasks.

- `debit-scrapers-pgadmin`: PgAdmin instance. An optional graphical user interface for the database. Refer to the `docker-compose.yaml` file for the username and password required for login.

- `debit-scrapers-queue-func`: Flask instance. A web server containing endpoints for queueing the initial set of scraping tasks and monitoring ongoing scraping jobs.

- `debit-scrapers-run-func`: Python script that pulls a batch of web scraping tasks from `debit-scrapers-pubsub` continuously on an interval and then processes those tasks by performing the web scraping.

It takes roughly 30-45 seconds for all services to begin running successfully. Each service will restart automatically when it encouters an error, until the application is manually shut down.

To queue a new web scraping job, make an HTTP POST request against the `debit-scrapers-queue-func` service at `https://localhost:5000/` or `https://0.0.0.0:5000/`. The JSON request body should have the following schema, with one or more "data sources" to be scraped specified:
```
{
  "sources": [
    "adb",
    "kfw"
  ]
}
```

The current list of sources available to be scraped includes:
```
adb
afdb
aiib
bio
deg
ebrd
eib
fmo
idb
ifc
kfw
miga
nbim
opic
pro
undp
wb
```

The HTTP request should also have two headers, `X-CloudScheduler-JobName` and `X-Cloud-Trace-Context`, the combination of which is a unique id for the scraping job. As a `curl` command, this would look like:

```
curl -d '{"sources":["adb"]}' \
-H 'Content-Type: application/json' \
-H 'X-CloudScheduler-JobName: job1' \
-H 'X-Cloud-Trace-Context: ac78c01b-6b21-4552-abbd-08078a687ff3' \
-X POST http://localhost:5000/
```

The Docker application can be shut down by entering `CTRL+C`.

## Reporting Bugs

If you find a bug in one of the scrapers or supporting packages, please double check that it has not already been reported by another user. If that is not the case, you can alert the maintainers by opening a new issue with a `bug` label. The ticket should provide as much detail as possible—specifying your OS version, environment setup, and configuration and describing the steps necessary to reproduce the error. Logs and stack traces will also be helpful in troubleshooting. Please try to isolate the problem by reproducing the bug using the lowest number of dependencies possible.

## Contributions

### Suggested Feature Requests

The GitHub project board will hold a list of suggested issues that the DeBIT team has identified as high priority. The workflow for contributions in this case is as follows:

- Assign yourself to the issue.
- Fork the repository and clone it locally. Add the original upstream repository as a remote origin and pull in new changes frequently.
- Create a branch for your edits; the name should contain the issue number and a one-to-five word description.
- Make commits of logical units while ensuring that commit messages are in the [proper format](https://cbea.ms/git-commit/).
- Add unit tests for your feature within the `tests` directory.
- Submit a pull request to `dev` and reference any supporting issues or documentation.
- Wait for your PR to be reviewed by at least one maintainer found in the `CODEOWNERS` file. If further changes are requested, you will be notified in a tagged comment. If your PR is approved, the maintainers will squash all commits into one, and you will be notified through an email and tagged comment that your work has been successfully merged.

### New Feature Requests
Contributors are encouraged to consider new features that would increase the number of projects available to community advocates and researchers; improve the accuracy and consistency of scraped project fields; and lead to greater efficiency and etiquette in web scraping. When proposing a new issue, tag the maintainers listed in the `CODEOWNERS` file. Following the submission, one of the code maintainers will contact you to approve, modify, or table the suggested enhancement to another time based on other open issues. Once the issue is approved, you can follow the same workflow to commit and submit your changes.
