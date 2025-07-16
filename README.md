# Bakehouse: Data Pipeline for Cookies Dataset (DAIS 2024)

This repository contains the source code and transformation logic for the Bakehouse data pipeline, built on Databricks using Delta Live Tables (DLT). It processes and analyzes the [Cookies Dataset from DAIS 2024](https://marketplace.databricks.com/details/f8498740-31ea-49f8-9206-1bbf533f3993/Databricks_Cookies-Dataset-DAIS-2024-) to generate production-ready analytics tables and KPIs.

## Repository Structure
* `explorations/`
Ad-hoc notebooks for data profiling, schema validation, and metric exploration.

* `transformations/`
Core transformation logic structured by layer: Bronze (raw ingestion), Silver (cleaned and enriched), and Gold (aggregate ready outputs).

* `utilities/`
Reusable Python modules and helper functions for transformation logic and pipeline consistency.


## Getting Started
Begin with the transformations folder, which contains the primary DLT pipeline logic:

* `bronze_ingestion.py`:
Defines raw ingestion logic for source tables from the Cookies Dataset. These are set up as streaming or batch DLT tables.

* `silver_transformation_layer.py`:
Implements all intermediate transformations including data cleaning, normalization, enrichment, and deduplication.

* `gold_tables.py`:
Defines the final fact_transactions table within the bakehouse schema, joining transactional, customer, and franchise data for downstream analytics.
