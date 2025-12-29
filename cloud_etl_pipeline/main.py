import functions_framework
import pandas as pd
from google.cloud import storage
import json
import io
import os
from datetime import datetime

storage_client = storage.Client()

@functions_framework.cloud_event
def transform_reviews_gcs(cloud_event):
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    
    if not file_name.startswith("raw/"):
        print(f"Ignore file {file_name} Because not in /raw/")
        return

    print(f"Start processing file: {file_name} from bucket: {bucket_name}")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    json_data = json.loads(content)

    # Transform/Flatten
    all_reviews = []
    for place in json_data:
        reviews = place.get('reviews', [])
        if reviews:
            for rev in reviews:
                all_reviews.append({
                    'author_name': rev.get('name'),
                    'rating': rev.get('stars')
                })
    
    if not all_reviews:
        print("No reviews found to process.")
        return

    df = pd.DataFrame(all_reviews)

    # Save /staging/ with new name and timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Get original file name without "raw/" path and ".json" extension
    base_name = os.path.basename(file_name).replace('.json', '')
    new_file_name = f"staging/transformed_{base_name}_{timestamp}.csv"
    
    # Convert DataFrame to CSV string (using io.StringIO to avoid temporary file on disk)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    
    # Upload to GCS
    output_blob = bucket.blob(new_file_name)
    output_blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
    print(f"Saved transformed file at: {new_file_name}")

    # Move original file to /processed/ directory (Instead of deleting to keep a record)
    new_raw_location = file_name.replace("raw/", "processed/")
    bucket.rename_blob(blob, new_raw_location)
    print(f"Moved original file to: {new_raw_location}")