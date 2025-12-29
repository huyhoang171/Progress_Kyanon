import os
import json
import pandas as pd
from datetime import datetime

def transform_logic(json_data):

    all_reviews = []
    
    for place in json_data:
        
        reviews = place.get('reviews', [])
        for rev in reviews:
            all_reviews.append({
                'author_name': rev.get('name'), 
                'rating': rev.get('stars')      
            })
    
    if not all_reviews:
        print("Warning: Don't have any reviews to process.")
        return pd.DataFrame(columns=['author_name', 'rating'])

    df = pd.DataFrame(all_reviews)
    return df

def transform_gcs_file(event, context=None):
    file_name = event.get('name', 'source_data.json')
    
    print(f"--- Processing file: {file_name} ---")

   
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {file_name} not found")
        return

    transformed_df = transform_logic(json_data)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"transformed_reviews_{timestamp}.csv"
    
    if not os.path.exists('staging'):
        os.makedirs('staging')
        
    output_path = os.path.join('staging', output_filename)
    transformed_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    
    print(f"--- Success! Processed {len(transformed_df)} reviews ---")
    print(f"--- File saved at: {output_path} ---")