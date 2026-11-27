from google.cloud import bigquery
import json
from absolute_pipeline.utils import calculate_record_hash
from datetime import datetime as DateTime

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

def prepare_volume_control_updates(changes: dict[str, list[dict]], control_state: dict[int, dict], tracked_volume_ids: set[int], run_id: str, extracted_at: DateTime,
) -> list[dict]:

    records = []
    timestamp = extracted_at.isoformat()

    for volume in changes["unchanged"]:
        volume_id = volume["id"]

        previous_state = control_state[volume_id]

        record = previous_state.copy()

        record["last_checked_at"] = extracted_at.isoformat()
        record["run_id"] = run_id

        for field in ["source_updated_at", "last_fetched_at"]:
            if isinstance(record[field], DateTime):
                record[field] = record[field].isoformat()

        records.append(record)

    changed_volumes = changes["new"] + changes["updated"]

    for volume in changed_volumes:
        volume_id = volume["id"]

        previous_state = control_state.get(volume_id)

        for volume in changed_volumes:
            volume_id = volume["id"]
            previous_state = control_state.get(volume_id)

            last_issue = volume.get("last_issue") or {}

            # Preserve existing tracking decisions
            if previous_state is None:
                is_tracked = volume_id in tracked_volume_ids
            else:
                is_tracked = previous_state["is_tracked"]

            record = {
                "volume_id": volume_id,
                "issue_count": volume.get("count_of_issues"),
                "last_issue_id": last_issue.get("id"),
                "source_updated_at": None,
                "last_checked_at": timestamp,
                "last_fetched_at": timestamp,
                "record_hash": calculate_record_hash(volume),
                "is_tracked": is_tracked,
                "run_id": run_id,
            }

        records.append(record)


    return records

def merge_volume_control_records(client: bigquery.Client, control_table_id: str, staging_table_id: str,) -> int:

    merge_query = f"""
        MERGE `{control_table_id}` AS T
        USING `{staging_table_id}` AS S
        ON T.volume_id = S.volume_id

        WHEN MATCHED THEN
            UPDATE SET
                T.issue_count = S.issue_count,
                T.last_issue_id = S.last_issue_id,
                T.source_updated_at = S.source_updated_at,
                T.last_checked_at = S.last_checked_at,
                T.last_fetched_at = S.last_fetched_at,
                T.record_hash = S.record_hash,
                T.is_tracked = S.is_tracked,
                T.run_id = S.run_id

        WHEN NOT MATCHED THEN
            INSERT (
                volume_id,
                issue_count,
                last_issue_id,
                source_updated_at,
                last_checked_at,
                last_fetched_at,
                record_hash,
                is_tracked,
                run_id
            )
            VALUES (
                S.volume_id,
                S.issue_count,
                S.last_issue_id,
                S.source_updated_at,
                S.last_checked_at,
                S.last_fetched_at,
                S.record_hash,
                S.is_tracked,
                S.run_id
            );
    """

    merge_job = client.query(merge_query)
    merge_job.result()

    affected_rows = merge_job.num_dml_affected_rows

    print(f"Affected rows: {affected_rows}")

    return affected_rows

def update_volume_control_state(client: bigquery.Client, changes: dict[str, list[dict]], control_state: dict[int, dict], tracked_volume_ids: set[int], control_table_id: str, staging_table_id: str, run_id: str, extracted_at: DateTime) -> int:

    control_records = prepare_volume_control_updates(
    changes=changes,
    control_state=control_state,
    tracked_volume_ids=tracked_volume_ids,
    run_id=run_id,
    extracted_at=extracted_at,
    )

    print(f"Prepared control records: {len(control_records)}")

    if not control_records:
        return 0


    staging_job = load_staging_records(
        client=client,
        records=control_records,
        table_id=staging_table_id,
    )

    if staging_job.output_rows != len(control_records):
        raise RuntimeError(
            f"Staging row count mismatch: "
            f"expected {len(control_records)}, "
            f"loaded {staging_job.output_rows}"
        )

    affected_rows = merge_volume_control_records(
        client=client,
        control_table_id=control_table_id,
        staging_table_id=staging_table_id,
    )
    
    if affected_rows != len(control_records):
        raise RuntimeError(
            f"MERGE row count mismatch: "
            f"expected {len(control_records)}, "
            f"affected {affected_rows}"
        )

    return affected_rows
