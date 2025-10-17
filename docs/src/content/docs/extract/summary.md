---
title: Data Extraction Pipeline
description: A guide in my new Starlight docs site.
---

The DeBIT data extraction pipeline retrieves project data by crawling and scraping webpages, programmatically downloading data files, querying APIs, or some combination of these methods. Because this process largely consists of awaiting network requests, it is I/O bound and requests can be parallelized for greater efficiency.

A single unit of work is therefore defined as an **_extraction task_** in which a URL is requested, the response is processed, any extracted project data is upserted into the database, and any follow-up tasks are queued.

To scale the application and allow multiple server nodes—each with their own, ephemeral IP address—to divide and conquer the work using a **_distributed architecture_**, enqueuing a task involves persisting the task to the database and then publishing a message to the appropriate queue, to be delivered to subscribers later. Once the subscriber receives the tasks represented by the messages in parallel using multithreading, and then publishes new messages/tasks for any work that remains to be done. This allows the larger **_extraction job_** to be completed.

To implement this architecture, the pipeline makes heavy use of microservices—

:::tip[Important Note]

Each microservice is designed to be idempotent, so the entire workflow can be retried in case of system failure.

:::

TODO
