// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

import tailwindcss from "@tailwindcss/vite";

import react from "@astrojs/react";

// https://astro.build/config
export default defineConfig({
  integrations: [
    starlight({
      title: "DeBIT",
      customCss: ["./src/styles/global.css"],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/withastro/starlight",
        },
      ],
      sidebar: [
        { label: "Introduction", slug: "intro" },
        {
          label: "Data Extraction Pipeline",
          items: [
            {
              label: "Overview",
              slug: "extract/summary",
            },
            {
              label: "Data Schema",
              slug: "extract/data-schema",
            },
            {
              label: "Template Workflows",
              items: [
                { label: "Overview", slug: "extract/common-workflows" },
                {
                  label: "Seed URLs",
                  slug: "extract/common-workflows/seed-urls",
                },
                {
                  label: "Crawl Search Results",
                  slug: "extract/common-workflows/crawl-search-results",
                },
                {
                  label: "Crawl & Scrape Search Results",
                  slug: "extract/common-workflows/crawl-scrape-search-results",
                },
                {
                  label: "Insert Project Details",
                  slug: "extract/common-workflows/insert-project-details",
                },
                {
                  label: "Update Project Details",
                  slug: "extract/common-workflows/update-project-details",
                },
                {
                  label: "Download Complete Projects",
                  slug: "extract/common-workflows/download-complete-projects",
                },
                {
                  label: "Download Partial Projects",
                  slug: "extract/common-workflows/download-partial-projects",
                },
              ],
            },
            {
              label: "Bank Workflows",
              items: [
                { label: "Overview", slug: "extract/bank-workflows" },
                {
                  label: "Asian Development Bank (ADB)",
                  slug: "extract/bank-workflows/adb",
                },
                {
                  label: "African Development Bank (AFDB)",
                  slug: "extract/bank-workflows/afdb",
                },
                {
                  label: "Asian Infrastructure Investment Bank (AIIB)",
                  slug: "extract/bank-workflows/aiib",
                },
                {
                  label:
                    "Belgian Investment Company for Developing Countries (BIO)",
                  slug: "extract/bank-workflows/bio",
                },
                {
                  label:
                    "Deutsche Investitions- und Entwicklungsgesellschaft (DEG)",
                  slug: "extract/bank-workflows/deg",
                },
                {
                  label:
                    "U.S. International Development Finance Corporation (DFC)",
                  slug: "extract/bank-workflows/dfc",
                },
                {
                  label:
                    "European Bank for Reconstruction and Development (EBRD)",
                  slug: "extract/bank-workflows/ebrd",
                },
                {
                  label: "European Investment Bank (EIB)",
                  slug: "extract/bank-workflows/eib",
                },
                {
                  label: "Dutch Entrepreneurial Development Bank (FMO)",
                  slug: "extract/bank-workflows/fmo",
                },
                {
                  label: "Inter-American Development Bank (IDB)",
                  slug: "extract/bank-workflows/idb",
                },
                {
                  label: "International Finance Corporation (IFC)",
                  slug: "extract/bank-workflows/ifc",
                },
                {
                  label: "Kreditanstalt für Wiederaufbau (KFW)",
                  slug: "extract/bank-workflows/kfw",
                },
                {
                  label: "Multilateral Investment Guarantee Agency (MIGA)",
                  slug: "extract/bank-workflows/miga",
                },
                { label: "Proparco (PRO)", slug: "extract/bank-workflows/pro" },
                {
                  label: "United Nations Development Programme (UNDP)",
                  slug: "extract/bank-workflows/undp",
                },
                { label: "World Bank (WB)", slug: "extract/bank-workflows/wb" },
              ],
            },
          ],
        },
        {
          label: "Data Transformation Pipeline",
          items: [
            {
              label: "Summary",
              slug: "transform/summary",
            },
          ],
        },
      ],
    }),
    react(),
  ],

  vite: {
    plugins: [tailwindcss()],
  },
});
