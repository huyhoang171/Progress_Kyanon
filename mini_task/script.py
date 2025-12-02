import pandas as pd
import json
import sqlite3

# --- CẤU HÌNH TÊN FILE ---
INPUT_JSON_FILE = 'source_data.json'
OUTPUT_CSV_FILE = 'cleaned_locations.csv'
DB_FILE = 'places_data.db'

def process_data():
    print("1. Đang đọc và xử lý dữ liệu JSON từ Apify...")
    
    try:
        # Đọc file JSON
        with open(INPUT_JSON_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Tạo DataFrame
        df = pd.DataFrame(data)

        # --- BƯỚC LÀM SẠCH & CHUẨN HÓA ---

        # 1. Đổi tên các cột
        df = df.rename(columns={
            'placeId': 'place_id',
            'title': 'name',
            'totalScore': 'rating',
            'reviewsCount': 'user_ratings_total',
            'address': 'address'
        })

        # 2. Xử lý dữ liệu thiếu (NaN)
        # Điền 0 cho user_ratings_total nếu bị thiếu
        df['user_ratings_total'] = df['user_ratings_total'].fillna(0).astype(int)

        # 3. Chuẩn hóa Tọa độ (Latitude / Longitude)
        def get_lat(loc):
            return loc.get('lat') if isinstance(loc, dict) else None
        def get_lng(loc):
            return loc.get('lng') if isinstance(loc, dict) else None

        df['latitude'] = df['location'].apply(get_lat)
        df['longitude'] = df['location'].apply(get_lng)

        # 4. Chuẩn hóa Categories (Types)
        df['types'] = df['categories'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else str(x)
        )

        # 5. Chọn các cột cần thiết
        final_columns = [
            'place_id', 'name', 'rating', 'user_ratings_total', 
            'latitude', 'longitude', 'address', 'types'
        ]
        # Chỉ lấy những cột tồn tại
        df_clean = df[[c for c in final_columns if c in df.columns]]

        # --- LƯU RA CSV ---
        df_clean.to_csv(OUTPUT_CSV_FILE, index=False, encoding='utf-8-sig')
        print(f"   -> Đã lưu file CSV sạch: {OUTPUT_CSV_FILE}")

        # --- NHẬP VÀO SQLITE ---
        print("2. Đang nhập vào SQLite...")
        conn = sqlite3.connect(DB_FILE)
        table_name = 'places'
        
        df_clean.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"   -> Đã nhập dữ liệu vào bảng '{table_name}'")

        # --- TRUY VẤN SQL (Ranking) ---
        print("3. Thực hiện truy vấn xếp hạng (Window Function)...")
        
        sql_query = f"""
            SELECT 
                name, 
                rating, 
                user_ratings_total,
                types,
                DENSE_RANK() OVER (
                    PARTITION BY types 
                    ORDER BY rating DESC, user_ratings_total DESC
                ) AS rank_in_type
            FROM 
                {table_name}
            WHERE rating IS NOT NULL
            ORDER BY 
                types, 
                rank_in_type
        """
        
        result_df = pd.read_sql_query(sql_query, conn)
        
        print("\n=== KẾT QUẢ XẾP HẠNG (Top 5 ví dụ) ===")
        print(result_df.head(10).to_string(index=False))
        
        conn.close()

    except Exception as e:
        print(f"Lỗi: {e}")

if __name__ == "__main__":
    process_data()