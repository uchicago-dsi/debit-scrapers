# debit-scrapers

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL_v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)

This repository houses open source data scrapers for **[DeBIT](https://debit.datascience.uchicago.edu)** (**De**velopment **B**ank **I**nvestment **T**racker), an online research tool developed by **[Inclusive Development International](https://www.inclusivedevelopment.net/)** and the **[University of Chicago Data Science Institute (DSI)](https://datascience.uchicago.edu/)**. DeBIT empowers community advocates to track investments made by development finance institutions and other entities that have independent accountability mechanisms (IAMs). Every two weeks, its scrapers extract data from all investment projects publicly disclosed by the following 17 institutions:

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

DeBIT is part of a larger open source initiative at the DSI and will continue to be maintained by its staff and contributors from around the world. Please see below for instructions on how to set up the repo, run the scrapers, and contribute scrapers for new financial institutions.

## Local Development

To run the application locally for development and testing, follow the setup instructions and then execute one of several entrypoint commands described below.

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

(5) Create an `.env.dev` file with the following contents and save it under the `./src` directory. NOTE: If you do not provide a Gemini API key, requests made to the API will fail and the program logic will gracefully fall back to traditional webscraping without bubbling up the error.

```
# Django
DJANGO_ALLOWED_HOST=*
DJANGO_PORT=8080
DJANGO_SECRET_KEY=
DJANGO_SETTINGS_MODULE=config.settings

# Environment
ENV=DEV

# Google Cloud Platform
GEMINI_API_KEY=
GOOGLE_CLOUD_PROJECT_ID=
GOOGLE_CLOUD_PROJECT_REGION=

# Orchestration
MAX_TASK_RETRIES=2
MAX_WAIT_IN_MINUTES=1440
POLLING_INTERVAL_IN_MINUTES=2

# pgAdmin
COMPOSE_HTTP_TIMEOUT="300"
PGADMIN_DEFAULT_EMAIL="admin@pgadmin.com"
PGADMIN_DEFAULT_PASSWORD="p@sssw0rd!123"
PGADMIN_LISTEN_PORT="443"
PGADMIN_CONFIG_SERVER_MODE="False"
PGADMIN_CONFIG_MASTER_PASSWORD_REQUIRED="False"

# PostgreSQL
POSTGRES_DB=postgres
POSTGRES_HOST=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_USER=postgres
```

### Entrypoints

The project's Makefile provides simple entrypoints for running the application locally as a Docker Compose application. A few pointers:

- All of the commands listed below must be run under the root of the project.

- Services can be shut down at any point by entering CTRL-C or, for services executing in the background, CTRL-D. This automatically shuts stops and destroys the active Docker containers.

- Data from the PostgreSQL database is persisted in the Docker volume `pgdata`, which is saved under the project root and ignored by Git. Because the project's Docker containers are run as the root user, you will need to assign yourself ownership of the directory if you'd like to delete or modify it (e.g., `sudo chown -R <username> pgdata`).

- For all commands, pgAdmin is provided as a GUI for the PostgreSQL databases. To use pgAdmin, navigate to localhost:443 in a web browser, select servers in the dropdown in the lefthand sidebar, click on the database you would like to inspect, and then log in with the password `postgres` when prompted. Browse tables and query the loaded data using raw SQL statements.

#### Build Scrapers

Builds a Docker image of the scrapers, which was architected as a Django project utilizing Django REST Framework.

WARNING: Playwright and its dependencies are downloaded and installed, so the image is large (around 1GB).

```bash
make build-scrapers
```

#### Develop Scrapers

Runs a container with an interactive bash terminal from the built Docker image while persisting the entire `src` directory as a volume. This allows you to make changes to scripts and immediately test the results.

```bash
make run-scrapers-bash
```

#### Test Scrapers

_Scraping Logic Only_

Set up an interactive terminal to the scraper service:

```bash
make build-scrapers
make run-scrapers-bash
```

Once the terminal is open, run a bash script to execute the suite of integration tests for the data scraping logic:

```bash
bash run_tests.sh
```

Tests for scrapers that involve downloading data or seeding URLs are generated automatically, whereas those related to extracting data from search result or individual project webpages or API payloads must be configured. To configure additional tests, add URLs to the appropriate YAML file under `./src/pipeline/extract/tests/params/`.

_Scraper Service API Endpoint_

Simulate the cloud-based production environment by spinning up a database, orchestrator service, and scraper service as a network of Docker containers. (NOTE: The cloud task queues are mocked, and tasks are simply printed to the console when enqueued.)

```bash
make build-scrapers
make run-scraper-network
```

When the orchestrator container starts, it will create a new job for the current date in the database, write the first set of scraping tasks to the database, and then queue those tasks to be processed. Afterwards, it will poll the database periodically until the timeout is reached or all of the tasks corresponding to the job have reached a terminal state (i.e., "Completed" or "Error" with no more retries).

Once the orchestrator begins polling, log into pgAdmin as described above and view the rows of the `extraction_task` database table. Use the data there to compose HTTP POST requests for the scraper service API endpoint. For example, the following request would conduct a partial page scrape of the IDB webpage at https://www.iadb.org/en/project/BO0060 for task 22860, a child of job 1:

```
ENDPOINT:
http://0.0.0.0:8000/api/v1/gcp/tasks

EXAMPLE REQUEST BODY:
{
    "id": 22860,
    "job_id": 1,
    "source": "idb",
    "workflow_type": "project-partial-page-scrape",
    "url": "https://www.iadb.org/en/project/BO0060"
}
```

Here, `id` refers to the id of the task in the database, `job_id` the id of the task's parent job, `workflow_type` the type of workflow to execute, and `url` the location of the data source to scrape.

You can make requests one at a time to confirm that the scraper service endpoint works as expected and that there are no errors persisting additional tasks or scraped project records to the database tables. The service will automatically reload whenever it is saved to facilitate development.

#### Tear Down Scrapers

Removes the Docker compose network and the database volume `pgdata`. It is advised to run `docker system prune` separately as well.

```bash
make tear-down-scraper-network
```

## Contributions

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
