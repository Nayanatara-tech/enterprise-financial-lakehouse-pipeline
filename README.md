# enterprise-financial-lakehouse-pipeline

# Enterprise Financial Lakehouse Pipeline on Databricks

End-to-end Databricks Medallion architecture project implementing incremental ingestion, data quality validation, SCD Type 2 historical tracking, Gold analytics, anomaly detection, AI-generated executive insights, Delta Lake features, and workflow orchestration.

## Tech Stack

- Databricks Community Edition
- PySpark
- Spark SQL
- Delta Lake
- Databricks Workflows
- Git & GitHub

## Key Features

- Incremental Bronze ingestion with processed file tracking
- Partitioned Delta tables for transaction data
- Validation and quarantine framework
- SCD Type 2 customer dimension using MERGE
- Conformed Silver dimensions
- Enriched fact table with currency conversion
- Gold business analytics
- Z-score anomaly detection
- AI executive summaries using `ai_query()`
- Delta History and Time Travel
- Workflow orchestration with email notifications

## Architecture

<img width="795" height="926" alt="image" src="https://github.com/user-attachments/assets/526073d8-34b6-4fe3-bc8b-33f158351a5c" />


## Workflow

<img width="940" height="446" alt="image" src="https://github.com/user-attachments/assets/ee4c2a8f-c28b-45a8-abec-e1434a62007a" />


## SCD2 Example

<img width="940" height="403" alt="image" src="https://github.com/user-attachments/assets/20d5f629-86ee-48a1-9353-7a123d22af4d" />


## Partition Pruning

<img width="940" height="378" alt="image" src="https://github.com/user-attachments/assets/ec9de1ae-1445-4fcf-acf1-ac5f6614ebe0" />


## Gold Analytics

<img width="940" height="473" alt="image" src="https://github.com/user-attachments/assets/c5005ede-e511-4f58-ab4c-8a6d450ffe21" />


## DESCRIBE HISTORY / MERGE history

<img width="940" height="422" alt="image" src="https://github.com/user-attachments/assets/4bc9b76b-043c-4f26-919c-04d04c2085c1" />


## AI Executive Summary

<img width="940" height="410" alt="image" src="https://github.com/user-attachments/assets/a51c7631-0d15-4275-8526-e46ec4b29efa" />

<img width="940" height="418" alt="image" src="https://github.com/user-attachments/assets/4361748f-513a-477d-9359-21038ab2002a" />


## Engineering Practices

### Git Workflow

This project was developed using a feature-branch workflow:

```text
feature/* → develop → main
```

- Feature branches were used for isolated development
- Changes were validated in `develop`
- Final production-ready code was merged into `main`

a collaborative enterprise development process using pull requests and controlled merges.

### Databricks Workflow Orchestration

- Implemented as a Databricks Workflow (Job)
- Task dependencies enforce Medallion execution order
- Email notifications enabled for workflow monitoring

### Delta Lake Performance Concepts

The project includes hands-on exploration of:

- **Partitioning** (`TransactionDate`)
- **Partition pruning** verified through `EXPLAIN`
- **Delta History**
- **Time Travel**
- **OPTIMIZE**
- **ZORDER**

For this demo dataset, `OPTIMIZE` did not rewrite files because the table was already compact, which is the expected Delta Lake behavior for very small tables.

## Repository Structure

```text
notebooks/
screenshots/
README.md
```

## Future Improvements

- Parameterized environments (dev/test/prod)
- CI/CD with Databricks Asset Bundles
- Automated schema evolution
- Data quality monitoring dashboard
