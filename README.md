# Absolute Pipeline

## Overview

Absolute Pipeline is an end-to-end data engineering project designed to collect, process, and serve information about DC Comics' Absolute Universe using the Comic Vine API.

The project aims to build a scalable, incremental, and cost-efficient data pipeline that supports a future web application for exploring comic book series, issues, and variant covers.

## Objectives

- Collect comic book metadata from the Comic Vine API.
- Minimize API consumption through incremental extraction strategies.
- Preserve historical source data in a Bronze layer.
- Maintain synchronization state to detect new and modified records.
- Transform and model data using dbt and BigQuery.
- Automate pipeline execution and monitoring.
- Provide structured data for a future comic book catalog application.
- Follow software and data engineering best practices.

## Architecture

### Current Architecture

```text
Comic Vine API
      |
      v
Python Extraction
      |
      v
Volume Change Detection <----- BigQuery Control
      |
      v
BigQuery Bronze
```

Initial ingestion, synchronization state initialization, and change detection have been implemented. The integration of these components into a complete incremental ingestion workflow is currently under development.

### Target Architecture

```text
Comic Vine API
      |
      v
Python Incremental Extraction
      |
      +------> Control Layer
      |
      v
BigQuery Bronze
      |
      v
dbt Transformations
      |
      v
BigQuery Silver
      |
      v
BigQuery Gold
      |
      v
Serving Layer
      |
      v
Web Application
```

Pipeline orchestration, cloud execution, and infrastructure provisioning will be introduced in later phases.

## Technology Stack

| Technology        | Purpose                                  | Status           |
| ----------------- | ---------------------------------------- | ---------------- |
| Python 3.13       | Extraction and processing                | Implemented      |
| uv                | Dependency and environment management    | Implemented      |
| Jupyter Notebooks | Exploration and prototyping              | Implemented      |
| Comic Vine API    | Data source                              | Implemented      |
| Google BigQuery   | Data warehouse and synchronization state | Implemented      |
| hashlib           | Record change detection                  | Implemented      |
| Hatchling         | Python package build backend             | Planned          |
| pytest            | Automated testing                        | Planned          |
| dbt               | Data transformation and modeling         | Planned          |
| Terraform         | Infrastructure as code                   | Planned          |
| Apache Airflow    | Proposed pipeline orchestration          | Under evaluation |
| GitHub Actions    | CI/CD                                    | Planned          |

## Data Source

The pipeline uses the Comic Vine API to retrieve comic book metadata.

Base URL:

```text
https://comicvine.gamespot.com/api/
```

### API Resources

| Resource       | Purpose                               | Status              |
| -------------- | ------------------------------------- | ------------------- |
| Volumes        | Discover comic book series            | Implemented         |
| Issues         | Retrieve comic book issues            | Planned             |
| Issue details  | Retrieve detailed issue metadata      | Planned             |
| Variant covers | Collect alternative cover information | Under investigation |

The initial discovery process searches for volumes containing `Absolute` in their names.

Additional filtering is applied to identify relevant DC Comics volumes published from 2024 onward. These filtering rules are preliminary and will be refined as the catalog evolves.

## Data Architecture

The data warehouse follows a Medallion Architecture with an additional Control layer.

### Bronze Layer

The Bronze layer stores raw source records and preserves historical versions of ingested data.

Dataset: `bronze`

#### bronze.volumes

| Column            | Type      | Description                   |
| ----------------- | --------- | ----------------------------- |
| volume_id         | INT64     | Comic Vine volume identifier  |
| payload           | JSON      | Original API response         |
| source_updated_at | TIMESTAMP | Source modification timestamp |
| extracted_at      | TIMESTAMP | Extraction timestamp          |
| run_id            | STRING    | Pipeline execution identifier |

Table configuration:

- Daily partitioning by `extracted_at`.
- Clustering by `volume_id`.
- Append-only ingestion.
- Original JSON payload preservation.
- Historical record versioning.

The `source_updated_at` field is currently nullable. Source timestamp handling will be finalized after validating the API timestamp semantics.

### Control Layer

The Control layer maintains synchronization metadata independently from the raw source data.

Dataset: `control`

#### control.volumes

Stores the latest known synchronization state for each discovered volume.

| Column            | Type      | Description                                       |
| ----------------- | --------- | ------------------------------------------------- |
| volume_id         | INT64     | Comic Vine volume identifier                      |
| issue_count       | INT64     | Latest observed issue count                       |
| last_issue_id     | INT64     | Latest observed issue identifier                  |
| source_updated_at | TIMESTAMP | Source modification timestamp                     |
| last_checked_at   | TIMESTAMP | Last successful source observation                |
| last_fetched_at   | TIMESTAMP | Last successful Bronze ingestion                  |
| record_hash       | STRING    | SHA-256 hash of the source payload                |
| is_tracked        | BOOL      | Whether the volume is part of the tracked catalog |
| run_id            | STRING    | Pipeline execution identifier                     |

#### control.volumes_staging

Temporary working table used to prepare synchronization records before merging them into the main Control table.

The staging table uses the same schema as `control.volumes`.

Synchronization state initialization follows these steps:

1. Retrieve the latest version of each volume from Bronze.
2. Identify tracked volumes.
3. Calculate record hashes.
4. Prepare synchronization records.
5. Load records into the staging table using `WRITE_TRUNCATE`.
6. Merge new records into the Control table.

The initialization MERGE uses insert-only semantics to avoid overwriting existing synchronization state.

### Silver Layer

Planned dataset: `silver`

The Silver layer will contain cleaned, normalized, deduplicated, and validated comic book data.

Planned entities:

- Volumes
- Issues
- Publishers
- Characters
- Variant covers

Transformations will be implemented using dbt.

### Gold Layer

Planned dataset: `gold`

The Gold layer will contain business-ready data models optimized for analytics and application consumption.

Potential models include:

- Absolute Universe catalog
- Volume summaries
- Issue release history
- Variant cover collections
- Upcoming releases

## Incremental Ingestion Strategy

The pipeline is designed to avoid unnecessary data ingestion and minimize external API consumption.

### Record Hashing

Each source record is serialized into a deterministic JSON representation and hashed using SHA-256.

```python
serialized = json.dumps(record, sort_keys=True)
record_hash = hashlib.sha256(
    serialized.encode("utf-8")
).hexdigest()
```

Sorting JSON keys ensures that differences in dictionary key ordering do not generate different hashes.

Ingestion metadata is excluded from the hash calculation.

### Change Detection

The current implementation retrieves synchronization state from BigQuery into a Python dictionary indexed by `volume_id`.

Incoming records are classified into three categories:

| Status    | Condition                              | Intended Action                     |
| --------- | -------------------------------------- | ----------------------------------- |
| NEW       | Volume does not exist in Control       | Insert into Bronze and Control      |
| UPDATED   | Incoming hash differs from stored hash | Append to Bronze and update Control |
| UNCHANGED | Incoming hash matches stored hash      | Update observation metadata only    |

Volumes missing from an API response are not automatically considered deleted.

The change detection function is independent of external services, allowing it to be tested without consuming API requests or executing BigQuery operations.

### Synchronization Workflow

The planned incremental workflow is:

```text
Retrieve source records
          |
          v
Retrieve Control state
          |
          v
Detect changes
          |
          +-- NEW --------+
          |               |
          +-- UPDATED ----+--> Append to Bronze
          |                        |
          +-- UNCHANGED -----------+
                                   |
                                   v
                             Update Control
```

Control state must only advance after the corresponding Bronze ingestion has completed successfully.

Unchanged records will not generate additional Bronze versions.

### API Optimization

Record hashing reduces unnecessary BigQuery writes but does not directly reduce API consumption.

Additional API optimization strategies will be implemented separately:

- Paginated bulk extraction.
- Source modification timestamps and watermarks, where supported.
- Selective issue and issue-detail extraction.
- Volume-level issue count comparisons.
- Configurable synchronization intervals.
- API request tracking.
- Rate limit handling.
- Retry and backoff mechanisms.
- Periodic reconciliation.

API capabilities and filtering behavior will be validated before implementing these strategies.

## Project Structure

The project is transitioning from notebook-based exploration to a reusable Python package.

```text
absolute-pipeline/
├── notebooks/
│   ├── 01_comicvine_exploration.ipynb
│   └── 02_incremental_ingestion.ipynb
├── src/
│   └── absolute_pipeline/
│       ├── __init__.py
│       ├── ingestion.py
│       └── utils.py
├── .env.example
├── .gitignore
├── .python-version
├── pyproject.toml
├── uv.lock
└── README.md
```

The `src/` package structure is currently being introduced.

### Notebooks

| Notebook                       | Purpose                                                                                  |
| ------------------------------ | ---------------------------------------------------------------------------------------- |
| 01_comicvine_exploration.ipynb | API exploration, pagination, filtering, and initial Bronze ingestion                     |
| 02_incremental_ingestion.ipynb | Control initialization, synchronization state, hashing, and incremental change detection |

### Reusable Modules

| Module       | Responsibility                              |
| ------------ | ------------------------------------------- |
| utils.py     | Record hashing and shared utility functions |
| ingestion.py | Record preparation and BigQuery ingestion   |

Additional modules will be introduced as the pipeline evolves.

## Getting Started

### Prerequisites

- Python 3.13
- uv
- Google Cloud CLI
- Google Cloud project with BigQuery enabled
- Comic Vine API key
- Git

### Install Dependencies

```bash
uv sync
```

### Configure Environment Variables

Create a local environment file:

```bash
cp .env.example .env
```

Configure the Comic Vine API key:

```env
API_KEY=your_comicvine_api_key
```

Never commit API keys or credentials to the repository.

### Configure Google Cloud Authentication

```bash
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### Run the Notebooks

Open the project in Visual Studio Code and select the project's `.venv` Python environment as the Jupyter kernel.

Execute the notebooks in numerical order when initializing the environment for the first time.

Avoid rerunning initial Bronze ingestion cells unnecessarily, as they append historical records.

## Testing

Change detection is currently validated using simulated source records and synchronization state.

The following scenarios have been successfully tested:

- [x] New volume detection.
- [x] Modified volume detection.
- [x] Unchanged volume detection.
- [x] Empty incoming records.
- [x] Empty synchronization state.
- [x] All incoming records unchanged.

Formal automated tests using pytest will be introduced as reusable functions are extracted from the notebooks.

## Development Roadmap

### Phase 1 — Initial Data Ingestion

**Status: Completed**

- [x] Initialize Python project using uv.
- [x] Configure Comic Vine API authentication.
- [x] Explore the volumes endpoint.
- [x] Implement API pagination.
- [x] Explore and filter volume metadata.
- [x] Configure BigQuery authentication.
- [x] Create Bronze and Control datasets.
- [x] Create the Bronze volumes table.
- [x] Implement raw record preparation.
- [x] Implement initial Bronze ingestion.
- [x] Validate initial ingestion.

### Phase 2 — Incremental Volume Ingestion

**Status: In Progress**

- [x] Design and create the volume Control table.
- [x] Implement deterministic record hashing.
- [x] Retrieve the latest volume versions from Bronze.
- [x] Identify tracked volumes.
- [x] Prepare synchronization records.
- [x] Create the Control staging table.
- [x] Implement staging ingestion with explicit schema.
- [x] Implement Control initialization MERGE.
- [x] Implement synchronization state retrieval.
- [x] Implement volume change detection.
- [x] Validate change detection using simulated data.
- [ ] Complete reusable Python package configuration.
- [ ] Extract reusable functions from notebooks.
- [ ] Implement incremental Bronze ingestion.
- [ ] Implement incremental Control updates.
- [ ] Implement failure recovery and idempotency safeguards.
- [ ] Optimize Comic Vine API consumption.
- [ ] Implement automated tests.
- [ ] Validate the complete incremental workflow.

### Phase 3 — Issues and Variant Covers

**Status: Planned**

- [ ] Explore the Comic Vine issues endpoint.
- [ ] Design issue-level synchronization state.
- [ ] Implement incremental issue extraction.
- [ ] Investigate variant cover availability.
- [ ] Implement selective issue-detail extraction.
- [ ] Implement cover metadata ingestion.
- [ ] Validate data completeness.

### Phase 4 — Data Transformation

**Status: Planned**

- [ ] Initialize dbt project.
- [ ] Configure BigQuery connection.
- [ ] Implement staging models.
- [ ] Implement Silver models.
- [ ] Implement Gold models.
- [ ] Implement data quality tests.
- [ ] Generate data documentation.

### Phase 5 — Infrastructure and Orchestration

**Status: Planned**

- [ ] Finalize cloud execution architecture.
- [ ] Provision infrastructure using Terraform.
- [ ] Implement pipeline orchestration.
- [ ] Configure scheduled executions.
- [ ] Implement logging and monitoring.
- [ ] Implement API consumption tracking.
- [ ] Configure CI/CD.
- [ ] Implement failure notifications.

### Phase 6 — Serving Layer and Web Application

**Status: Planned**

- [ ] Define application data requirements.
- [ ] Select serving database and object storage.
- [ ] Implement Gold-to-serving synchronization.
- [ ] Implement cover image management.
- [ ] Develop the comic book catalog application.
- [ ] Deploy the application.

## Engineering Principles

- Incremental processing over full reloads.
- API efficiency over unnecessary extraction.
- Raw source data preservation.
- Separation of data and synchronization state.
- Deterministic change detection.
- Idempotent pipeline operations.
- Explicit schema management.
- Modular and reusable Python code.
- Automated testing.
- Infrastructure as code.
- Secure credential management.
- Observable pipeline execution.

## Project Status

The project is currently in Phase 2: Incremental Volume Ingestion.

Initial volume extraction and Bronze ingestion are operational. The synchronization Control table has been initialized, and volume change detection has been implemented and successfully validated using simulated data.

The next milestone is to complete the reusable Python package and integrate change detection with incremental Bronze ingestion and Control state updates.

This project is being developed incrementally as a hands-on data engineering learning project.
