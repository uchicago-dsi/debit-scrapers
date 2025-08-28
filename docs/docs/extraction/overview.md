---
sidebar_position: 1
---

# Overview

The DeBIT data extraction pipeline retrieves project data by crawling and scraping webpages, programmatically downloading data files, querying APIs, or some combination of these methods. Because this process largely consists of awaiting network requests, it is I/O bound and requests can be parallelized for greater efficiency.

A single unit of work is therefore defined as an **_extraction task_** in which a URL is requested; the response is processed; any extracted project data is upserted into the database; and any follow-up tasks are queued.

To scale the application and allow multiple server nodes—each with their own, ephemeral IP address—to divide and conquer the work using a **_distributed architecture_**, enqueuing a task involves persisting the task to the database and then publishing a message to a Google Cloud Pub/Sub topic that can be retrieved by subscribers later. Each server node pulls a batch of the messages via its subscriber, processes the tasks represented by the messages in parallel using multithreading, and then publishes new messages/tasks for any work that remains to be done. This allows the larger **_extraction job_** to be completed.

To implement this architecture, the pipeline makes heavy use of microservices—

:::tip[Important Note]

Each microservice is designed to be idempotent, so the entire Cloud Workflow can be retried in case of system failure.

:::

1. A **[Google Cloud Scheduler](https://cloud.google.com/scheduler/docs/overview)** instance triggers an invocation of **[Google Cloud Workflows](https://cloud.google.com/workflows/docs/overview)**, a managed orchestration platform, once every two weeks.

2. The Workflow spins up temporary infrastructure, including a Postgres database server on **[Google Cloud SQL](https://cloud.google.com/sql/docs/introduction)** and **[Google Cloud Task](https://cloud.google.com/tasks/docs/dual-overview)** queues, if they don't already exist by calling a **[Google Cloud Run Job](https://cloud.google.com/run/docs/overview/what-is-cloud-run)** (**"Deploy Extraction Infrastructure"**).

3. The Workflow initializes the database by calling a Cloud Run Job (**"Initialize Database"**) to apply database migrations and install fixtures via Django management commands.

4. The Workflow calls another Cloud Run Job (**"Trigger Extraction Job"**) while passing its invocation id and the list of banks to process as arguments. If the id does not correspond to a data extraction job in the database, a new job is created; otherwise, the existing job is received. The Cloud Run Job then queues the first set of tasks for those banks in the database if they haven't been queued already.

5. The Workflow coordinates multiple instances of a Cloud Run Job running in parallel (**"Extract Data"**) to extract project data and write it to the database. Each instance pulls a batch of messages from a Pub/Sub subscription (Note: Batch size is determined by configuration), and each message corresponds to a task/single URL. The instance then uses multithreading to process those tasks, acknowledging the messages as they are completed.

6. Once the data extraction job has completed, the Workflow exports the extraction job metadata and project records to a **[Google Cloud Storage](https://cloud.google.com/storage/docs/introduction)** bucket as TSV files by calling the Cloud Run Job **"Export Extracted Data"**.

7. Finally, the Workflow destroys the database and Pub/Sub infrastructure and marks the extraction job as complete in the database by calling the Cloud Run Job **"Destroy Extraction Infrastructure"**.
