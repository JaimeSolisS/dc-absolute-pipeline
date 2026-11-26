import requests
import time

def fetch_paginated_results(
    api_key: str,
    base_url: str, 
    endpoint: str,
    filters: str | None = None,
    limit: int = 100,
    request_delay: float = 2.0,
) -> list[dict]:

    if not api_key:
        raise ValueError("API key is required")

    if not 1 <= limit <= 100:
        raise ValueError("Limit must be between 1 and 100")

    if request_delay < 0:
        raise ValueError("Request delay cannot be negative")

    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    headers = {
        "User-Agent": "absolutePipeline"  
    }   

    all_results = []
    offset = 0

    while True:
        params = {
            "api_key": api_key,
            "format": "json", 
            "limit" : limit,
            "offset" : offset, 
            #"field_list": "id,name,count_of_issues,date_last_updated,last_issue,publisher,start_year"
        }

        if filters is not None:
            params["filter"] = filters

        response = requests.get(url=url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data["status_code"] != 1:
            raise RuntimeError(f"Comic Vine API error: {data.get('error')}")

        results = data["results"]
        number_of_total_results = data["number_of_total_results"]
        all_results.extend(results)

        print(f"Fetched {len(results)} records. Accum Records: {len(all_results)}. Number of total results: {number_of_total_results}")

        if len(all_results) >= number_of_total_results:
            break

        if not results:
            raise RuntimeError("Unexpected empty page before pagination completed")

        offset += len(results)
        print("waiting...")
        time.sleep(request_delay)

    print(f"Final count: {len(all_results)}")

    return all_results






