import dlt
import requests

BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.resource(name="nyc_taxi_data", write_disposition="replace")
def nyc_taxi_data():

    page = 1
    total_rows = 0

    while True:
        print(f"Fetching page {page}...")

        response = requests.get(
            BASE_URL,
            params={"page": page},
            timeout=30
        )
        response.raise_for_status()

        data = response.json()

        # stop ketika kosong
        if not data:
            print("No more data. Stopping.")
            break

        print(f"Rows received: {len(data)}")

        total_rows += len(data)

        yield data
        page += 1

    print(f"Total rows loaded: {total_rows}")


if __name__ == "__main__":

    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi",
        refresh="drop_sources",
        progress="log",
    )

    load_info = pipeline.run(nyc_taxi_data())
    print(load_info)
