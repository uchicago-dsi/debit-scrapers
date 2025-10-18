# extract

A service for extracting development bank project finance details from heterogeneous data sources such as APIs, data files, and webpages.

Consists of a Django project, `pipeline`, with "scrapers" for each data source. An orchestrator client queues the first data extraction task for each source. As tasks are completed, subsequent tasks are queued when applicable to continue "crawling" the source (e.g., a search results webpage would generate links to project projects) while full or partial project records are written to a database as the data is encountered.

## Setup

After installing the required project dependencies described in the repository's main `README.md`, create an `.env.dev` file with the following contents and save it under the current extract directory. NOTE: If you do not provide a Gemini API key, requests made to the API will fail and the program logic will gracefully fall back to traditional webscraping without bubbling up the error.

```
# Django
DJANGO_ALLOWED_HOST='*'
DJANGO_PORT='8080'
DJANGO_SECRET_KEY=''
DJANGO_SETTINGS_MODULE='config.settings'

# Environment
ENV='dev'

# Google Cloud Platform
GEMINI_API_KEY=''
GOOGLE_CLOUD_PROJECT_ID=''
GOOGLE_CLOUD_PROJECT_REGION=''

# Orchestration
MAX_TASK_RETRIES='2'
MAX_WAIT_IN_MINUTES='1440'
POLLING_INTERVAL_IN_MINUTES='2'

# pgAdmin
COMPOSE_HTTP_TIMEOUT='300'
PGADMIN_DEFAULT_EMAIL='admin@pgadmin.com'
PGADMIN_DEFAULT_PASSWORD='p@sssw0rd!123'
PGADMIN_LISTEN_PORT='443'
PGADMIN_CONFIG_SERVER_MODE='False'
PGADMIN_CONFIG_MASTER_PASSWORD_REQUIRED='False'

# PostgreSQL
POSTGRES_DB='postgres'
POSTGRES_HOST='postgres'
POSTGRES_PASSWORD='postgres'
POSTGRES_USER='postgres'
```

### Entrypoints

The service's Makefile provides simple entrypoints for running the application locally as a Docker Compose application. A few pointers:

- All of the commands listed below must be run directly under the current directory.

- Services can be shut down at any point by entering `CTRL-C` or, for services executing in the background, `CTRL-D`. This automatically shuts, stops, and destroys the active Docker containers.

- Data from the PostgreSQL database is persisted in the Docker volume `pgdata`, which is saved under `extract` and ignored by Git. NOTE: Because the service's Docker containers run as the root user, you will need to assign yourself ownership of the directory if you'd like to manually delete or modify it (e.g., `sudo chown -R $(id -un):$(id -gn) .`, assuming `extract` is your current working directory).

- For all commands, pgAdmin is provided as a GUI for the PostgreSQL databases. To use pgAdmin, navigate to localhost:443 in a web browser, select "Servers" from the dropdown in the lefthand sidebar, click on the database you would like to inspect, and then log in with the password `postgres` when prompted. Browse tables through the interface and query data using raw SQL statements.

#### Build Image

Builds a Docker image of the scrapers, which was architected as a Django project utilizing Django REST Framework.

WARNING: Playwright and its dependencies are downloaded and installed, so the image is large (around 1GB).

```bash
make build-scrapers
```

#### Run Interactive Terminal

Runs a container with an interactive bash terminal from the built Docker image while persisting the entire `./services/extract/src` directory as a volume. This allows you to make changes to scripts and immediately test the results.

```bash
make run-scrapers-bash
```

#### Run Tests

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
make run-scrapers
```

When the orchestrator container starts, it will create a new job for the current date in the database, write the first set of scraping tasks to the database, and then queue those tasks to be processed. Afterwards, it will poll the database periodically until the timeout is reached or all of the tasks corresponding to the job have reached a terminal state (i.e., "Completed" or "Error" with no more retries).

Once the orchestrator begins polling, log into pgAdmin as described above and view the rows of the `extraction_task` database table. Use the data there to compose HTTP POST requests for the scraper service API endpoint. For example, the following request would conduct a partial page scrape of the IDB webpage at https://www.iadb.org/en/project/BO0060 for task 22860, a child of job 1:

```
ENDPOINT:
http://0.0.0.0:8000/api/v1/gcp/tasks

EXAMPLE REQUEST BODY:
{
    "id": 22860,
    "job": 1,
    "source": "idb",
    "workflow_type": "project-partial-page-scrape",
    "url": "https://www.iadb.org/en/project/BO0060"
}
```

Here, `id` refers to the id of the task in the database, `job_id` the id of the task's parent job, `workflow_type` the type of workflow to execute, and `url` the location of the data source to scrape.

You can make requests one at a time to confirm that the scraper service endpoint works as expected and that there are no errors persisting additional tasks or scraped project records to the database tables. The service will automatically reload whenever it is saved to facilitate development.

#### Tear Docker Resources

Removes the Docker compose network and the database volume `pgdata`. It is advised to run `docker system prune` separately as well.

```bash
make tear-down-scrapers
```
