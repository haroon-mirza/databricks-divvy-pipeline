# Databricks DLT & dbt Hybrid Data Pipeline

This project delivers a robust, end-to-end data engineering solution for ingesting and transforming complex bike-share data. It leverages a modern, hybrid approach on Databricks, handling inconsistent raw data, enforcing data quality, and structuring it for analytics.

<img width="1146" height="660" alt="data architecture" src="https://github.com/user-attachments/assets/4fe7f493-31db-42a9-9931-c0bdb9373514" />

*Architecture: Raw Files -> DLT & PySpark (Bronze) -> dbt & SQL (Silver, Gold) -> Power BI*

---

## Tech Stack

* **Cloud Platform:** Databricks
* **Data Governance:** Unity Catalog (Catalogs, Schemas, Volumes)
* **Ingestion:** Delta Live Tables (DLT) & Python/PySpark
* **Transformation:** dbt (data build tool) & SQL
* **Data Architecture:** Medallion Architecture (Bronze, Silver, Gold)
* **Data Quality:** DLT Expectations & dbt Tests
* **Code Quality:** Pytest (for Python transformation logic)
* **CI/CD:** GitHub Actions (configured, awaiting full platform support)
* **Analytics:** Power BI
* **Version Control:** Git & GitHub 

---

## Project Highlights & Concepts Demonstrated

This project showcases a range of critical data engineering skills:

* **Hybrid Pipeline Architecture:** Demonstrates a modern, best-of-both-worlds approach, using the programmatic power of **DLT and PySpark** for complex ingestion (especially schema evolution) and the declarative, SQL-based power of **dbt** for sophisticated transformations.
* **Robust Ingestion & Schema Evolution:** The pipeline is built to handle messy, real-world data. It successfully processes source files with wildly inconsistent schemas and column names, conforming them to a single, reliable standard in the Bronze layer.
* **Declarative & Orchestrated Data Flows:** Leverages Delta Live Tables to manage the initial data ingestion, showcasing a declarative approach where DLT handles dependencies, orchestration, and infrastructure.
* **SQL-Driven Transformations:** Utilizes dbt for building clean, maintainable, and testable Silver (cleaned and refined) and Gold (aggregated and business-ready) data models entirely in SQL.
* **Comprehensive Data Quality:** Implements data quality validation using **DLT Expectations** at the ingestion layer and robust **dbt Tests** on the final models. Unit tests further ensure the reliability of Python transformation logic.
* **Modern Data Governance:** All data assets, from the raw file landing zone (Unity Catalog Volumes) to the final analytical tables, are securely managed and governed through Databricks Unity Catalog.
* **CI/CD Pipeline Setup:** A GitHub Actions workflow is configured to automate testing and deployment of the dbt models on every code push. (Note: Full execution of the CI/CD pipeline is currently limited by Databricks Free Tier security restrictions.)
