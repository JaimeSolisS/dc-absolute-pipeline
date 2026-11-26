import hashlib
import json


def calculate_record_hash(record: dict) -> str:
    serialized = json.dumps(record, sort_keys=True)

    encoded = serialized.encode("utf-8")

    record_hash = hashlib.sha256(encoded).hexdigest()

    return record_hash