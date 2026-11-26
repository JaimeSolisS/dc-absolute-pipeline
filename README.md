# Absolute Pipeline

An end-to-end data engineering project focused on collecting, processing, and serving comic book metadata from DC Comics' Absolute Universe using the Comic Vine API.

## Overview

Absolute Pipeline aims to build a centralized and automated data platform for exploring Absolute Universe comic book series, individual issues, and variant covers.

The project follows a medallion architecture and prioritizes incremental ingestion, efficient API consumption, reproducibility, and data quality.

## Objectives

- Collect comic book metadata from the Comic Vine API.
- Preserve original source data for reproducibility.
- Minimize unnecessary API requests through incremental ingestion.
- Build a structured analytical data model.
- Track pipeline executions and synchronization state.
- Provide a data foundation for a future web-based comic book catalog.

## Architecture

### Current Implementation

```text
Comic Vine API
      |
      v
Python Extraction
      |
      v
BigQuery - Bronze
```

### Target Architecture

```text
Comic Vine API
      |
      v
Python Ingestion <---- Synchronization State
      |
      v
BigQuery - Bronze
      |
      v
dbt Transformations
      |
      v
BigQuery - Silver
      |
      v
BigQuery - Gold
      |
      v
Serving Layer
      |
      v
Web Application
```

## Technology Stack

| Technology      | Purpose                          | Status      |
| --------------- | -------------------------------- | ----------- |
| Python          | Data extraction and ingestion    | Implemented |
| uv              | Dependency management            | Implemented |
| Jupyter         | Data exploration and prototyping | Implemented |
| Pandas          | Data exploration                 | Implemented |
| Google BigQuery | Data warehouse                   | Implemented |
| dbt             | Data transformations             | Planned     |
| Terraform       | Infrastructure as Code           | Planned     |
| Airflow         | Pipeline orchestration           | Proposed    |
| GitHub Actions  | CI/CD                            | Planned     |

## Data Source

The project uses the [Comic Vine API](https://comicvine.gamespot.com/api/documentation/) as its primary data source.

Resources:

| Endpoint       | Purpose                      | Status      |
| -------------- | ---------------------------- | ----------- |
| `/volumes/`    | Comic book series metadata   | Implemented |
| `/issues/`     | Individual comic book issues | Planned     |
| `/issue/{id}/` | Detailed issue metadata      | Planned     |

Variant cover extraction is planned and will be evaluated during source exploration.

## Data Architecture

### Bronze Layer

Stores original source records alongside ingestion metadata.

The Bronze layer follows an append-only approach to preserve historical source data.

#### `bronze.volumes`

| Column            | Data Type | Description                   |
| ----------------- | --------- | ----------------------------- |
| volume_id         | INT64     | Comic Vine volume identifier  |
| payload           | JSON      | Original API record           |
| source_updated_at | TIMESTAMP | Source modification timestamp |
| extracted_at      | TIMESTAMP | Extraction timestamp          |
| run_id            | STRING    | Pipeline execution identifier |

Table configuration:

- Daily partitioning by `extracted_at`.
- Clustering by `volume_id`.
- Batch ingestion using BigQuery Load Jobs.
- Append-only write strategy.

### Control Layer

Reserved for pipeline execution metadata, synchronization state, incremental ingestion, and API usage tracking.

### Silver Layer

Planned layer for data normalization, deduplication, validation, and business rules.

### Gold Layer

Planned layer for consumption-ready datasets and the future comic book catalog.

## Getting Started

### Prerequisites

- Python
- uv
- Google Cloud CLI
- Google Cloud project with BigQuery enabled
- Comic Vine API key

### Install Dependencies

```bash
uv sync
```

### Configure Environment Variables

```bash
cp .env.example .env
```

Set your Comic Vine API key in the `.env` file.

```dotenv
API_KEY=your_api_key
```

### Configure Google Cloud Authentication

```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### Data Exploration

Open the exploration notebook using Visual Studio Code or a compatible Jupyter environment.

The notebook currently covers:

- Comic Vine API requests.
- Pagination.
- Volume metadata exploration.
- Preliminary business filters.
- BigQuery dataset and table creation.
- Record preparation.
- Batch ingestion into BigQuery.

## Roadmap

### Phase 1 — Initial Ingestion

- [x] Initialize Python project.
- [x] Configure dependency management with uv.
- [x] Explore Comic Vine API.
- [x] Implement volume pagination.
- [x] Explore and filter volume metadata.
- [x] Configure BigQuery authentication.
- [x] Create Bronze and Control datasets.
- [x] Create the Bronze volumes table.
- [x] Implement initial batch ingestion.

### Phase 2 — Incremental Ingestion

- [ ] Implement synchronization control tables.
- [ ] Detect new and modified volumes.
- [ ] Implement idempotent ingestion.
- [ ] Implement API rate limiting and retries.
- [ ] Track pipeline executions.

### Phase 3 — Issues and Variant Covers

- [ ] Explore issue endpoints.
- [ ] Implement issue ingestion.
- [ ] Investigate variant cover availability.
- [ ] Implement incremental issue synchronization.

### Phase 4 — Data Transformation

- [ ] Initialize dbt.
- [ ] Implement Silver models.
- [ ] Implement Gold models.
- [ ] Implement data quality tests.

### Phase 5 — Production Deployment

- [ ] Refactor notebooks into reusable Python modules.
- [ ] Implement automated testing.
- [ ] Define infrastructure using Terraform.
- [ ] Implement pipeline orchestration.
- [ ] Configure CI/CD.

### Phase 6 — Data Serving

- [ ] Design the serving data model.
- [ ] Implement image storage.
- [ ] Implement the serving layer.
- [ ] Build the web application.

## Project Status

Under active development. The initial volume extraction and BigQuery ingestion have been successfully implemented.
