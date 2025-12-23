import pandas as pd
from sqlalchemy import create_engine

# --- 1. Load Data ---
df_hdi_raw = pd.read_csv('raw_data/HDR25_Composite_indices_complete_time_series.csv', encoding='latin-1')
df_wdi = pd.read_csv('raw_data/Source2_WDI_Cleaned_ASEAN.csv')

# --- 2. Xử lý dữ liệu HDR (Transformation) ---

# 2.1 Filter ASEAN Countries
asean_codes = ['VNM', 'THA', 'IDN', 'MYS', 'PHL', 'SGP', 'MMR', 'KHM', 'LAO', 'BRN']
df_hdi_asean = df_hdi_raw[df_hdi_raw['iso3'].isin(asean_codes)].copy()

# 2.2 Chọn các cột cần thiết (CẬP NHẬT MỚI)
# Chúng ta cần lấy các prefix: hdi (HDI), le (Life Expectancy), eys (Education Years)
target_metrics = ['hdi', 'le', 'eys']

# Tạo danh sách các cột cần giữ lại
# Logic: Giữ iso3, country và bất kỳ cột nào bắt đầu bằng 'hdi_', 'le_', 'eys_' theo sau là số
cols_to_keep = ['iso3', 'country']
for col in df_hdi_asean.columns:
    # Kiểm tra xem cột có thuộc định dạng 'metric_year' không (ví dụ: le_1990)
    if any(col.startswith(m + '_') for m in target_metrics) and col.split('_')[-1].isdigit():
        cols_to_keep.append(col)

df_hdi_asean = df_hdi_asean[cols_to_keep]

# 2.3 Reshape từ Wide sang Long (CẬP NHẬT MỚI: Dùng wide_to_long)
# wide_to_long cực mạnh khi xử lý nhiều time-series cùng lúc (HDI, LE, EYS)
df_hdi_long = pd.wide_to_long(
    df_hdi_asean, 
    stubnames=target_metrics,  # Các tiền tố: hdi, le, eys
    i=['iso3', 'country'],     # Các cột định danh giữ nguyên
    j='Year',                  # Tên cột mới chứa năm
    sep='_',                   # Ký tự ngăn cách giữa metric và năm
    suffix='\d+'               # Định dạng của năm (số)
).reset_index()

# 2.4 Clean & Rename
df_hdi_long.rename(columns={
    'iso3': 'Country_Code', 
    'country': 'Country_Name',
    'hdi': 'HDI_Value',
    'le': 'Life_Expectancy',
    'eys': 'Education_Years'
}, inplace=True)

# Data Typing
cols_numeric = ['HDI_Value', 'Life_Expectancy', 'Education_Years']
for col in cols_numeric:
    df_hdi_long[col] = pd.to_numeric(df_hdi_long[col], errors='coerce')

# --- 3. Xử lý dữ liệu WDI ---
# (Phần này giữ nguyên logic của bạn, chỉ thêm comment cho rõ)
df_wdi['Year'] = df_wdi['Year'].astype(int)

df_wdi_wide = df_wdi.pivot_table(
    index=['Country_Code', 'Year'],
    columns='Indicator_Code',
    values='Value',
    aggfunc='first'
).reset_index()

df_wdi_wide.columns.name = None
df_wdi_wide.rename(columns={
    'BX.KLT.DINV.CD.WD': 'FDI_Net_Inflows',
    'NY.GDP.PCAP.CD': 'GDP_Per_Capita'
}, inplace=True)

# --- 4. Integration (Join) ---
# Merge dữ liệu HDR (đã có đủ 3 chỉ số) với WDI
df_merged = pd.merge(
    df_hdi_long, 
    df_wdi_wide, 
    on=['Country_Code', 'Year'], 
    how='inner'
)

# --- 5. Feature Engineering ---

# 5.1 Calculated Field
# Ví dụ: Tỷ lệ FDI trên Tuổi thọ (Demo tính toán thêm)
if 'Life_Expectancy' in df_merged.columns:
     df_merged['FDI_per_Year_Life'] = df_merged['FDI_Net_Inflows'] / df_merged['Life_Expectancy']

# 5.2 Categorization: Level
def classify_hdi(hdi):
    if pd.isna(hdi): return None # Handle Null
    if hdi < 0.550: return 'Low'
    elif hdi < 0.700: return 'Medium'
    elif hdi < 0.800: return 'High'
    else: return 'Very High'

df_merged['HDI_Level'] = df_merged['HDI_Value'].apply(classify_hdi)

# In kiểm tra
print("Dữ liệu sau khi tích hợp (bao gồm Life Exp & Education):")
print(df_merged[['Country_Code', 'Year', 'HDI_Value', 'Life_Expectancy', 'Education_Years', 'GDP_Per_Capita']].head())
print(df_merged.info())

# --- 6. Data Storage ---
db_connection_str = 'postgresql://postgres:huyhoang113@localhost:5433/undp_worldbank_dw'
db_connection = create_engine(db_connection_str)

try:
    df_merged.to_sql('fact_development_indicators', db_connection, if_exists='replace', index=False)
    print("Đã load dữ liệu vào PostgreSQL thành công!")
except Exception as e:
    print(f"Lỗi kết nối: {e}")

# Xuất CSV check
df_merged.to_csv('integrated_data_final.csv', index=False)