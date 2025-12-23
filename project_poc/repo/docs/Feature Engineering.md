## Feature Engineering Overview

This document summarizes how the project computes feature-engineering outputs for the merged HDI–WDI dataset. The pipelines rely on configuration in [config/settings.yaml](config/settings.yaml) so behavior can be adjusted without code changes.

### Ratio Calculation
- Purpose: derive additional analytical ratios from the merged HDI–WDI dataset.
- Data source: merged data stored at the curated stage (default `cleaned/hdi_wdi_merged.csv` in GCS/local as defined under `paths`).
- Implementation: `RatioCalculator` in [src/transform/ratio_calculation.py](src/transform/ratio_calculation.py).
- Configuration: each ratio is defined under `feature_engineering` in [config/settings.yaml](config/settings.yaml) using a `target_col` and a Python-evaluable `formula`. Example: `FDI_to_HDI_Ratio = FDI_Net_Inflows / HDI_Value`.
- Processing flow:
	1) Load config to pick up bucket, paths, and formulas.
	2) Read merged dataset from GCS/local as configured.
	3) Evaluate each formula with pandas `eval`, replacing inf/-inf with NaN.
	4) Persist curated output locally and to GCS (path resolved from `paths.curated`/`paths.curated_path`).

### HDI Categorization
- Purpose: assign an HDI level label to each record based on HDI_Value thresholds.
- Data source: curated dataset (default `curated/hdi_wdi_curated.csv`).
- Implementation: `HDICategorizer` in [src/transform/categorization.py](src/transform/categorization.py).
- Configuration: rules live under `hdi_rules` in [config/settings.yaml](config/settings.yaml). Each rule has `label` and `max_value`; rules are sorted by `max_value` and the first threshold that exceeds `HDI_Value` is applied.
- Processing flow:
	1) Load config to obtain bucket, paths, and rule thresholds.
	2) Read curated dataset from GCS/local.
	3) Apply categorization to produce `HDI_Level`.
	4) Persist categorized output locally and to GCS (overwriting the curated file unless a custom path is provided).

### Source for HDI threshold rules
The HDI level thresholds configured in `hdi_rules` are informed by Vietnamese guidance summarized in the article on Thư Viện Pháp Luật: https://thuvienphapluat.vn/lao-dong-tien-luong/hdi-la-gi-hdi-viet-nam-la-bao-nhieu-va-chi-so-nay-co-anh-huong-den-nguoi-lao-dong-viet-nam-hay-khon-10117.html.

### How to adjust behavior
- Add or edit ratio formulas under `feature_engineering` in [config/settings.yaml](config/settings.yaml); ensure referenced columns exist in the merged dataset.
- Update HDI thresholds under `hdi_rules` to reflect new labeling schemes; order is determined by `max_value` ascending.
- Override data locations via `paths.raw/cleaned/curated` or absolute `raw_path/cleaned_path/curated_path`. Set `paths.settings_path` or env var `SETTINGS_PATH` if the config file lives elsewhere.

