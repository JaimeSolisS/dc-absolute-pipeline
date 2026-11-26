from google.cloud import bigquery

def prepare_volume_records(volumes, run_id, extracted_at):
    records = []

    for volume in volumes:
        record = {
            "volume_id": volume["id"], 
            "payload": volume, 
            "source_updated_at": None, 
            "extracted_at" : extracted_at.isoformat(), 
            "run_id" : run_id
        }
        records.append(record)

    return records

def load_volume_records(client, records, table_id):
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
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