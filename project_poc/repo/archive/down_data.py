import requests
import pandas as pd
import os

api_url = "https://hdrdata.org/api/CompositeIndices/query-detailed?apikey=HDR-E7QF6KmtnAbP2T3wD2P5MPdo94bAzJtP&countryOrAggregation=VNM,THA,SGP,PHL,MMR,MYS,LAO,IDN,KHM,BRN&indicator=hdi,le,mys" 

# 2. Giả lập trình duyệt (Để server không chặn bot)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json"
}

try:
    print("Dang goi API lay du lieu tu UNDP...")
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        print("Ket noi thanh cong! Dang xu ly du lieu...")
        
        # Lấy dữ liệu JSON trả về
        data_json = response.json()
        
        df = pd.DataFrame(data_json)
        
        print(f"Da lay duoc {len(df)} dong du lieu.")
        print(df.head())
        
        output_path = "/opt/airflow/dags/data/undp_hdi_data_raw.csv"
        
        # Tạo thư mục data nếu chưa có (để tránh lỗi)
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Lưu file vào đường dẫn mới
        df.to_csv(output_path, index=False)
        print(f"Da luu file tai: {output_path}")
        
    else:
        print(f"Loi: Server tra ve ma {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"Loi xay ra: {e}")