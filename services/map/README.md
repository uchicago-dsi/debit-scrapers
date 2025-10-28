# map

A service for mapping clean development bank project records to the format required by the DeBIT website.

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

Builds a Docker image of the data mapping pipeline.

```bash
make build-mapper
```

#### Run Pipeline

Builds and runs the pipeline script as a Docker container while mounting the directories `./services/map/input` and `./services/map/output` as volumes.

Download the clean project file generated from the previous data pipeline step and save it under the input directory. After the script runs, a mapped Parquet file is generated and saved under the output directory.

```bash
make run-mapper
```
