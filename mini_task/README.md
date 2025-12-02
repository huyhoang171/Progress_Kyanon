# Apify Data Crawling & Processing Project

A data pipeline project that crawls location data from Google Maps using Apify and processes it into a structured database with ranking capabilities.

## Overview

This project uses Apify to scrape business location data from Google Maps, then processes and cleans the data into both CSV format and SQLite database. It includes a ranking system that ranks locations by category based on ratings and review counts.

## Project Structure

```
.
├── source_data.json          # Raw data from Apify crawl
├── cleaned_locations.csv     # Processed data in CSV format
├── places_data.db           # SQLite database with processed data
├── query.py                 # SQL query script for data ranking
└── script.py                # Main data processing pipeline
```

## Features

- **Data Crawling**: Automated scraping from Google Maps via Apify
- **Data Cleaning**: Standardizes and normalizes raw JSON data
- **Database Storage**: Stores data in SQLite for efficient querying
- **Ranking System**: Ranks locations by category using SQL window functions
- **CSV Export**: Exports cleaned data for easy analysis

## Files Description

### 1. script.py
Main data processing pipeline that handles the entire ETL (Extract, Transform, Load) process.

**Key Functions:**
- Reads raw JSON data from `source_data.json`
- Cleans and normalizes data:
  - Standardizes column names
  - Handles missing values
  - Extracts latitude/longitude from location objects
  - Normalizes categories/types
- Exports to CSV (`cleaned_locations.csv`)
- Loads data into SQLite database (`places_data.db`)
- Executes ranking query using window functions

**Data Processing:**
- Renames columns to standard format (e.g., `placeId` → `place_id`)
- Fills missing `user_ratings_total` with 0
- Extracts coordinates from nested location objects
- Converts category arrays to comma-separated strings

### 2. query.py
SQL query script for ranking analysis.

**Functionality:**
- Connects to SQLite database
- Executes ranking query using `DENSE_RANK()` window function
- Partitions by category type
- Orders by rating (DESC) and review count (DESC)
- Displays top-ranked locations per category

## Requirements

```bash
pip install pandas apify-client
```

**Dependencies:**
- `pandas`: Data manipulation and CSV handling
- `apify-client`: Apify API integration (for crawling)
- `sqlite3`: Database operations (included in Python standard library)

## Usage

### Step 1: Crawl Data (using Apify)

First, you need to crawl data using Apify's Google Maps Scraper. Set up your Apify API token and run the crawler to generate `source_data.json`.

**Note:** You'll need an Apify account and API token.

### Step 2: Process Data

```bash
python script.py
```

This script will:
1. Read and clean data from `source_data.json`
2. Export cleaned data to `cleaned_locations.csv`
3. Import data into SQLite database (`places_data.db`)
4. Run ranking query and display results

### Step 3: Query Rankings

```bash
python query.py
```

View ranked locations by category based on ratings and review counts.

## Data Schema

### Cleaned Data Columns

| Column | Type | Description |
|--------|------|-------------|
| `place_id` | String | Unique Google Places ID |
| `name` | String | Business name |
| `rating` | Float | Average rating (0-5) |
| `user_ratings_total` | Integer | Total number of reviews |
| `latitude` | Float | Latitude coordinate |
| `longitude` | Float | Longitude coordinate |
| `address` | String | Full address |
| `types` | String | Comma-separated categories |

### Database Table: `places`

The SQLite database contains a single table with the same schema as the cleaned data.

## Ranking Logic

The ranking system uses SQL window functions:

```sql
DENSE_RANK() OVER (
    PARTITION BY types 
    ORDER BY rating DESC, user_ratings_total DESC
) AS rank_in_type
```

- **PARTITION BY types**: Groups locations by category
- **ORDER BY**: Ranks by rating first, then review count
- **DENSE_RANK()**: Assigns ranks without gaps for ties

## Customization

### Modify Ranking Criteria

Edit the `ORDER BY` clause in the SQL query in both `script.py` and `query.py`:

```python
ORDER BY rating DESC, user_ratings_total DESC
```

You can change the ranking factors or add additional criteria.

### Filter Specific Categories

Add a `WHERE` clause to filter specific business types:

```python
WHERE types LIKE '%restaurant%' AND rating IS NOT NULL
```

### Adjust Output Columns

Modify the `final_columns` list in `script.py`:

```python
final_columns = [
    'place_id', 'name', 'rating', 'user_ratings_total', 
    'latitude', 'longitude', 'address', 'types'
    # Add more columns as needed
]
```

## Example Output

```
=== RANKING RESULTS (Top 5 Example) ===
name                          rating  user_ratings_total  types              rank_in_type
Coffee Shop A                 4.8     1250               cafe               1
Coffee Shop B                 4.8     980                cafe               2
Restaurant A                  4.9     2100               restaurant         1
Restaurant B                  4.7     1500               restaurant         2
```

## Error Handling

The script includes basic error handling:
- Catches exceptions during data processing
- Displays error messages in Vietnamese/English
- Handles missing data gracefully

## License

Private project

## Notes

- Ensure `source_data.json` exists before running `script.py`
- The database file `places_data.db` will be created automatically
- Table data is replaced on each run (`if_exists='replace'`)
- Requires valid JSON format from Apify output
