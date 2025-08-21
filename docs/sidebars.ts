import type { SidebarsConfig } from "@docusaurus/plugin-content-docs";

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/**
 * Creating a sidebar enables you to:
 - create an ordered group of docs
 - render a sidebar for each doc of that group
 - provide next/previous navigation

 The sidebars can be generated from the filesystem, or explicitly defined here.

 Create as many sidebars as you want.
 */
const sidebars: SidebarsConfig = {
  tutorialSidebar: [
    "intro",
    {
      type: "category",
      label: "Data Extraction",
      link: {
        type: "generated-index",
        title: "Data Extraction",
        description:
          "Learn how data is collected for DeBIT using a distributed architecture.",
        slug: "/extraction",
      },
      items: [
        "extraction/overview",
        "extraction/data-schema",
        {
          type: "category",
          label: "Common Workflows",
          link: {
            id: "extraction/common-workflows/index",
            type: "doc",
          },
          items: [
            "extraction/common-workflows/seed-urls",
            "extraction/common-workflows/crawl-search-results",
            "extraction/common-workflows/crawl-scrape-search-results",
            "extraction/common-workflows/insert-project-details",
            "extraction/common-workflows/update-project-details",
            "extraction/common-workflows/download-complete-projects",
            "extraction/common-workflows/download-partial-projects",
          ],
        },
        {
          type: "category",
          label: "Bank Workflows",
          link: {
            id: "extraction/bank-workflows/index",
            type: "doc",
          },
          items: [
            "extraction/bank-workflows/adb",
            "extraction/bank-workflows/afdb",
            "extraction/bank-workflows/aiib",
            "extraction/bank-workflows/bio",
            "extraction/bank-workflows/deg",
            "extraction/bank-workflows/dfc",
            "extraction/bank-workflows/ebrd",
            "extraction/bank-workflows/eib",
            "extraction/bank-workflows/fmo",
            "extraction/bank-workflows/idb",
            "extraction/bank-workflows/ifc",
            "extraction/bank-workflows/kfw",
            "extraction/bank-workflows/miga",
            "extraction/bank-workflows/nbim",
            "extraction/bank-workflows/pro",
            "extraction/bank-workflows/undp",
            "extraction/bank-workflows/wb",
          ],
        },
      ],
    },
  ],
};

export default sidebars;
