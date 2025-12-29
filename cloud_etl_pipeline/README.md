This project automatically processes review data: it reads JSON files (for example from Google Maps), flattens the data, and outputs standardized CSV files ready to be loaded into BigQuery or other analytics systems.

Requirements
- Python 3.8+ and pip
- Packages listed in `requirements.txt` (including `functions-framework`, `pandas`, `google-cloud-storage`)

Main files
- `main.py`: Cloud Function handler  the actual entry point is `transform_reviews_gcs`.
- `main_test.py`: local test logic; contains `transform_gcs_file` used by `run.bat`.
- `run.bat`: Windows helper to install dependencies quickly and run the local test (it creates a temporary `run_test.py` that calls `transform_gcs_file`).
- `source_data.json`: sample input data for testing.
- `staging/`: directory where transformed CSV output files are saved.

Run locally
1. Open a terminal at the `cloud_etl_pipeline` folder.
2. Install dependencies:

```
pip install -r requirements.txt
```

3. Run the local test (Windows):

```
.\run.bat
```

Note: `run.bat` creates a temporary `run_test.py` that calls `transform_gcs_file` from `main_test.py` (it does not invoke `main.py`), so the local test uses `transform_gcs_file`.

Deploy to Google Cloud Functions
1. Create a Cloud Storage bucket (e.g. `review-etl-bucket`) and a `raw/` folder to upload JSON files.
2. Create a Cloud Function (Runtime: Python 3.10, 2nd gen):
   - Trigger: Cloud Storage  event `google.cloud.storage.object.v1.finalized`.
   - Entry point: `transform_reviews_gcs` (the function defined in `main.py`).
   - Include `requirements.txt` in the function configuration.
3. Grant the Function's Service Account the `Storage Object Admin` role so it can write to `staging/` and move files to `processed/`.

Load into BigQuery
- After the CSV files appear in the `staging/` folder on the bucket, use `bq load` to import them into a dataset. Example:

```
bq load --source_format=CSV --autodetect --skip_leading_rows=1 \
  reviews_dataset.Staging_Reviews \
  "gs://[YOUR_BUCKET]/staging/transformed_reviews_*.csv"
```

Quick check SQL

```
SELECT author_name, rating
FROM `your-project.reviews_dataset.Staging_Reviews`
LIMIT 10;
```

