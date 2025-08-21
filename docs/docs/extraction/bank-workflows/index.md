---
sidebar_position: 1
---

import DocCardList from '@theme/DocCardList';

# Bank Workflows

Custom workflows have been written for each financial institution by overriding the **[common/template workflows](/extraction/common-workflows)**. To extract data for a given bank, one or more of these workflows are executed in sequence.

To give a hypothetical example, first a "Seed URLs" workflow could be called to generate a list of all search result pages on a website and then queue tasks to crawl those URLs. For each task, the "Crawl Search Results" workflow could be called to extract links to individual project webpages and queue new tasks to scrape those links. Finally, for each project scraping task, "Insert Project Details" could be called to extract the project data and save it to the database. This **_fan out_** pattern is suitable for the application's **[distributed architecture](/extraction/overview)**.

Within the codebase, a registry class maps each data source to its _starter workflow_, which kicks off its data extraction process. The pipeline then queues the first data extraction tasks for all configured financial institutions.

<DocCardList />
