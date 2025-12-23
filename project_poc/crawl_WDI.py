import wbgapi as wb
import pandas as pd

# 1. Cấu hình
# 10 quốc gia Đông Nam Á (ASEAN): BRN, KHM, IDN, LAO, MYS, MMR, PHL, SGP, THA, VNM
countries_asean = ['BRN', 'KHM', 'IDN', 'LAO', 'MYS', 'MMR', 'PHL', 'SGP', 'THA', 'VNM']

# Ánh xạ Mã Chỉ số World Bank sang tên cột theo yêu cầu POC
indicators = {
    'NY.GDP.PCAP.CD': 'GDP_Per_Capita',
    'BX.KLT.DINV.CD.WD': 'FDI_Net_Inflows'
}

# Phạm vi năm theo yêu cầu POC
time_range = range(1990, 2024) 

# 2. Lấy dữ liệu từ World Bank API
print("Đang tải dữ liệu WDI cho 10 quốc gia ASEAN (1990-2023) từ World Bank...")
# wb.data.DataFrame tự động lấy dữ liệu ở dạng Wide format
df_wdi_raw = wb.data.DataFrame(
    indicators, 
    countries_asean, 
    time=time_range, 
    labels=True # Trả về tên đầy đủ của quốc gia và chỉ số
)

# 3. Làm sạch và Chuẩn hóa Dữ liệu (Transform)
# Đặt lại index về các cột thông thường
df_wdi_temp = df_wdi_raw.reset_index()

# Chuyển đổi từ wide format sang long format (melt)
# Lấy các cột năm (YR1990, YR1991, ..., YR2023)
year_columns = [col for col in df_wdi_temp.columns if col.startswith('YR')]

# Melt data: chuyển các cột năm thành hàng
df_wdi_final = df_wdi_temp.melt(
    id_vars=['economy', 'series'],
    value_vars=year_columns,
    var_name='Year',
    value_name='Value'
)

# Làm sạch cột Year: loại bỏ tiền tố 'YR' và chuyển sang số nguyên
df_wdi_final['Year'] = df_wdi_final['Year'].str.replace('YR', '').astype(int)

# Đổi tên các cột
df_wdi_final.rename(columns={
    'economy': 'Country_Code',
    'series': 'Indicator_Code'
}, inplace=True)

# Sắp xếp lại thứ tự cột: Country_Code, Year, Indicator_Code, Value
df_wdi_final = df_wdi_final[[
    'Country_Code', 
    'Year', 
    'Indicator_Code', 
    'Value'
]].sort_values(by=['Country_Code', 'Year', 'Indicator_Code']).reset_index(drop=True)

# 4. Kiểm tra kết quả
print("\n--- 5 Hàng Dữ liệu WDI Đầu tiên Đã Được Làm Sạch ---")
print(df_wdi_final.head().to_markdown(index=False, numalign="left", stralign="left"))
print(f"\nTổng số bản ghi đã lấy: {len(df_wdi_final)}")

# 5. Lưu trữ Tệp CSV Đã Làm Sạch (Bước L - Load)
import os

# Tạo thư mục raw_data nếu chưa tồn tại
os.makedirs('raw_data', exist_ok=True)

cleaned_wdi_file = 'raw_data/Source2_WDI_Cleaned_ASEAN.csv'
df_wdi_final.to_csv(cleaned_wdi_file, index=False)
print(f"\nDữ liệu WDI đã làm sạch và được lưu tại {cleaned_wdi_file}")