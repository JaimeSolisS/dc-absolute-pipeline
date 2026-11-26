from google.cloud import bigquery
import json
from absolute_pipeline.utils import calculate_record_hash

def get_control_state( client: bigquery.Client, table_id: str) -> dict[int, dict]:

    query = f"""
        SELECT *
        FROM `{table_id}`
    """

    query_job = client.query(query)
    results = query_job.result()

    control_state = { row["volume_id"]: dict(row) for row in results}

    return control_state

def prepare_control_records(volumes: list[dict],tracked_volume_ids: set[int]) -> list[dict]:

    records = []

    for volume in volumes:

        # Prepare payload
        payload = volume["payload"]

        if isinstance(payload, str):
            payload = json.loads(payload)

        # Calculate record hash
        record_hash = calculate_record_hash(payload)

        # Prepare timestamps
        extracted_at = volume["extracted_at"].isoformat()

        source_updated_at = (
            volume["source_updated_at"].isoformat()
            if volume["source_updated_at"] is not None
            else None
        )

        # Build control record
        record = {
            "volume_id": volume["volume_id"],
            "issue_count": volume["issue_count"],
            "last_issue_id": volume["last_issue_id"],
            "source_updated_at": source_updated_at,
            "last_checked_at": extracted_at,
            "last_fetched_at": extracted_at,
            "record_hash": record_hash,
            "is_tracked": volume["volume_id"] in tracked_volume_ids,
            "run_id": volume["run_id"]
        }

        records.append(record)

    return records

def load_staging_records(client: bigquery.Client, records: list[dict], table_id: str):
    # Configure the load job
        job_config = bigquery.LoadJobConfig(
            schema=client.get_table(table_id).schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    
        # Submit the load job
        job = client.load_table_from_json(json_rows=records, destination=table_id, job_config=job_config)
    
        # Wait for completion
        job.result()
    
        # Print the number of loaded rows
        print(job.job_id)
        print(job.state)
        print(job.output_rows)
        print(job.errors)
        return job

def detect_volume_changes(volumes: list[dict], control_state: dict[int, dict]) -> dict[str, list[dict]]:

    changes = {
        "new": [],
        "updated": [],
        "unchanged": []
    }

    for volume in volumes:

        # Get volume ID
        volume_id= volume["id"]

        # Calculate incoming record hash
        volume_hash = calculate_record_hash(volume)

        # Get previous state
        previous_state = control_state.get(volume_id)

        # Classify volume
        if previous_state is None:
            changes["new"].append(volume)

        elif volume_hash != previous_state["record_hash"]:
            changes["updated"].append(volume)

        else:
            changes["unchanged"].append(volume)

    return changes