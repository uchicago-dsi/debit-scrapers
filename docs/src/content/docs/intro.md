---
title: Introduction
description: A guide in my new Starlight docs site.
---

To shed light on the opaque world of international development finance, the **Development Bank Investment Tracker (DeBIT)** collects, standardizes, augments, and aggregates data on projects funded by high-profile financial institutions with independent acountability mechanisms (IAMs). Currently, there are 16 such institutions:

- African Development Bank (AfDB)
- Asian Development Bank (ADB)
- Asian Infrastructure Investment Bank (AIIB)
- Belgian Investment Company for Developing Countries (BIO)
- Deutsche Investitions - und Entwicklungsgesellschaft (DEG)
- European Bank for Reconstruction and Development (EBRD)
- European Investment Bank (EIB)
- Inter-American Development Bank (IDB)
- International Finance Corporation (IFC)
- Kreditanstalt für Wiederaufbau (KfW)
- Multilateral Investment Guarantee Agency (MIGA)
- Nederlandse Financierings-Maatschappij voor Ontwikkelingslanden (FMO)
- U.S. International Development Finance Corporation (DFC)
- Proparco
- United Nations Development Programme (UNDP)
- World Bank (WB)

The backend data pipeline—the subject of this documentation—is written in Python and consists of three stages executed in sequence: data extraction, data cleaning, and data mapping to destination services. Read on for further details of each.
