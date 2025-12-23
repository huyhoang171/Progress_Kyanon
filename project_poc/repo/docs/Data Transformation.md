# Data Transformation - Process Overview

## Overview

The Transform stage converts raw (long-format) data into cleaned, standardized, wide-format data that is ready for analysis.

**Source files:**
- `src/transform/hdi_transformer.py`
- `src/transform/wdi_transformer.py`

---

## 1. HDI Data Transformation

### 1.1 Raw data (input)

Input file: `data/raw/hdi_raw.csv`

Columns in raw HDI data:

| Column | Description |
|--------|-------------|
| `countryIsoCode` | ISO3 country code |
| `country` | Country name |
| `year` | Year |
| `indicatorCode` | Indicator code (`hdi`, `le`, `mys`) |
| `value` | Indicator value |

### 1.2 Transform steps (summary)

Raw (long) → Clean (wide):

1. Load: read raw CSV into a pandas DataFrame.
2. Extract & pivot: pivot indicator rows into separate columns (one column per indicator).
3. Convert data types: strings for names/codes, `Int64` for `Year`, `Float64` for numeric indicators.
4. Normalize keys: standardize `Country_Code` (uppercase, trimmed).
5. Handle missing values: default is to remove rows with NaN; optional interpolation by time series.

### 1.3 Cleaned data (output)

Output file: `data/cleaned/hdi_cleaned.csv`

| Column | Type | Description |
|--------|------|-------------|
| `Country_Code` | string | ISO3 country code (e.g. `VNM`) |
| `Country_Name` | string | Country name (e.g. `Viet Nam`) |
| `Year` | Int64 | Year of the observation |
| `HDI_Value` | Float64 | Human Development Index [0-1] |
| `Life_Expectancy` | Float64 | Life expectancy at birth (years) |
| `Education_Years` | Float64 | Mean years of schooling |

---

## 2. WDI Data Transformation

### 2.1 Raw data (input)

Input file: `data/raw/wdi_parsed.csv`

Columns in raw WDI data:

| Column | Description |
|--------|-------------|
| `Country_Code` | ISO3 country code |
| `Country_Name` | Country name |
| `Year` | Year |
| `Indicator_Name` | Indicator name (e.g. `GDP per capita`) |
| `Value` | Indicator value |

### 2.2 Transform steps (summary)

Raw (long) → Clean (wide):

1. Load: read raw CSV into a pandas DataFrame.
2. Extract & pivot: pivot `Indicator_Name` rows into separate columns.
3. Convert data types: strings for names/codes, `Int64` for `Year`, `Float64` for indicator columns.
4. Normalize keys: standardize `Country_Code` (uppercase, trimmed).
5. Handle missing values: default is to remove rows with NaN; optional interpolation by time series.

### 2.3 Cleaned data (output)

Output file: `data/cleaned/wdi_cleaned.csv`

| Column | Type | Description |
|--------|------|-------------|
| `Country_Code` | string | ISO3 country code (e.g. `VNM`) |
| `Country_Name` | string | Country name (e.g. `Viet Nam`) |
| `Year` | Int64 | Year of the observation |
| `GDP per capita (current US$)` | Float64 | GDP per capita |
| `FDI, net inflows (BoP, current US$)` | Float64 | Net FDI inflows |
| *...other indicators...* | Float64 | Other economic indicators |

---

## 3. Run Transform

```bash
# Transform HDI
python -m src.transform.hdi_transformer

# Transform WDI
python -m src.transform.wdi_transformer
```

---

## 4. Missing value strategies

| Strategy | Description |
|----------|-------------|
| `remove` (default) | Drop rows that contain any NaN |
| `interpolate` | Linear interpolation along each country's time series |

### Rationale for choosing `remove` as the default

- Historical coverage differences: older WDI years (e.g. pre-1970) are frequently null while HDI data starts later (e.g. 1990–2023); dropping nulls avoids mixing uneven time coverage.
- Simplicity & safety: dropping incomplete rows avoids introducing potentially misleading imputed values when the time series is too sparse for reliable interpolation.
- Easier to audit and trace: working only with complete records simplifies debugging and validation.
- Suitable for a POC: faster and requires fewer assumptions; switch to interpolation when series density supports it.

To enable interpolation: call `transform(missing_strategy='interpolate')`.

