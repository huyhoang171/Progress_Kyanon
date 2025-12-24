# Mini Airflow - Place Metric Ingestion Pipeline

A complete Apache Airflow ETL pipeline that extracts place data, calculates average ratings, and loads results into a database.

## 📋 Overview

This project demonstrates an Airflow DAG with 3 sequential tasks:
1. **Extract**: Fetch place data from mock API (JSON file)
2. **Transform**: Calculate overall average rating from places
3. **Load**: Insert/update the metric into a database table

## 🏗️ Architecture

```
place_metric_ingestion DAG
│
├── Task 1: extract_places (PythonOperator)
│   ├── Reads source_data.json
│   ├── Pushes places list to XCom
│   └── Returns summary count
│
├── Task 2: transform_rating (PythonOperator)
│   ├── Pulls places from XCom
│   ├── Calculates average rating
│   ├── Pushes average to XCom
│   └── Returns transformation results
│
└── Task 3: load_to_db (PythonOperator)
    ├── Pulls average rating from XCom
    ├── Creates/updates Daily_Metrics table
    └── Returns load results
```

## 📁 Project Structure

```
mini_airflow/
├── dags/
│   └── place_metric_ingestion.py      # Main DAG definition
├── src/
│   ├── ingestion/
│   │   └── mock_api_extract.py        # Extract task logic
│   ├── transform/
│   │   └── calculate_average_rating.py # Transform task logic
│   └── load/
│       └── db_loader.py                # Load task logic
├── data/
│   └── source/
│       └── source_data.json            # Place data (38K+ records)
├── logs/                               # Airflow logs
├── config/                             # Airflow configuration
├── docker-compose.yaml                 # Docker orchestration
├── Dockerfile                          # Custom Airflow image
├── requirements.txt                    # Python dependencies
└── README.md                           # This file
```

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed
- At least 4GB RAM allocated to Docker

### Step 1: Start Airflow

```powershell
# Navigate to mini_airflow directory
cd d:\kyanon\progress\mini_airflow

# Start all services
docker-compose up -d
```

### Step 2: Access Airflow UI

1. Open browser to: http://localhost:8080
2. Login credentials:
   - Username: `airflow`
   - Password: `airflow`

### Step 3: Run the DAG

1. Find `place_metric_ingestion` in the DAG list
2. Toggle the DAG to "ON" (unpause it)
3. Click the "Play" button to trigger a manual run
4. Watch the tasks execute sequentially

## 📊 Database Schema

The pipeline creates/updates the `Daily_Metrics` table:

```sql
CREATE TABLE Daily_Metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_date DATE NOT NULL UNIQUE,
    average_rating REAL NOT NULL,
    total_places INTEGER,
    places_with_rating INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🔍 Verifying the Pipeline

### Check XCom Values

1. Go to Airflow UI → DAGs → `place_metric_ingestion`
2. Click on a successful run
3. Go to "XCom" tab
4. Verify:
   - `extract_places` pushed `places_data` (list of places)
   - `transform_rating` pushed `average_rating` (float value)

### Check Logs

Click on each task to view logs:

**Extract Task Logs:**
```
Successfully fetched 38993 places from mock API
Extract complete: 38993 total places, XXXXX with ratings
```

**Transform Task Logs:**
```
Calculated average rating: X.XX from XXXXX places
Transform complete: {'average_rating': X.XX, 'total_places': 38993, ...}
```

**Load Task Logs:**
```
Daily_Metrics table created/verified
Successfully inserted/updated metric for 2025-12-24: average_rating=X.XX
```

### Query the Database

Connect to the Airflow worker container to query the database:

```powershell
# Enter the worker container
docker exec -it mini_airflow-airflow-worker-1 bash

# Run SQLite query
sqlite3 /opt/airflow/airflow.db "SELECT * FROM Daily_Metrics ORDER BY metric_date DESC LIMIT 5;"
```

Expected output:
```
id|metric_date|average_rating|total_places|places_with_rating|created_at|updated_at
1|2025-12-24|4.52|38993|38500|2025-12-24 10:30:45|2025-12-24 10:30:45
```

## 🛠️ Troubleshooting

### DAG not appearing in UI
```powershell
# Check DAG for syntax errors
docker exec -it mini_airflow-airflow-worker-1 python /opt/airflow/dags/place_metric_ingestion.py

# View dag processor logs
docker logs mini_airflow-airflow-dag-processor-1
```

### Task failures
1. Click on the failed task in Airflow UI
2. View logs for error details
3. Check XCom values to ensure data is being passed correctly

### Container issues
```powershell
# View all containers
docker ps -a

# Check specific service logs
docker logs mini_airflow-airflow-scheduler-1
docker logs mini_airflow-airflow-worker-1

# Restart services
docker-compose restart
```

## 📈 Monitoring

- **DAG Runs**: Track execution history in Airflow UI
- **Task Duration**: View task performance metrics
- **XCom**: Monitor data flow between tasks
- **Logs**: Access detailed execution logs per task
- **Database**: Query Daily_Metrics table for historical data

## 🔄 Extending the Pipeline

### Add Data Validation
Modify `transform_task` to validate data quality before calculating average.

### Add Email Alerts
Configure SMTP settings in Airflow and add email alerts on failure:
```python
default_args = {
    'email_on_failure': True,
    'email': ['your-email@example.com'],
}
```

### Schedule Changes
Modify the DAG schedule:
```python
schedule_interval='@daily'    # Daily at midnight
schedule_interval='0 6 * * *' # Daily at 6 AM
schedule_interval='@hourly'   # Every hour
```

## 🧹 Cleanup

```powershell
# Stop all services
docker-compose down

# Remove volumes (deletes all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```

## 📚 Key Concepts Demonstrated

✅ **Airflow DAG**: Task orchestration and scheduling
✅ **PythonOperator**: Custom Python task execution  
✅ **XCom**: Inter-task communication and data passing
✅ **Task Dependencies**: Sequential task execution (`>>` operator)
✅ **Context Variables**: Access to task instance and execution context
✅ **Database Operations**: SQLite CRUD operations
✅ **Docker Compose**: Multi-container orchestration
✅ **ETL Pattern**: Extract, Transform, Load pipeline

## 📝 Notes

- The DAG uses Airflow's default SQLite database (`airflow.db`) for storing metrics
- Data source contains 38,993+ place records with ratings
- XCom is used to pass data between tasks (suitable for small-medium datasets)
- For production, consider using an external database (PostgreSQL, MySQL, etc.)
- Tasks are idempotent - re-running updates existing records based on date

## 🎯 Success Criteria

- ✅ DAG appears in Airflow UI and can be toggled on
- ✅ All 3 tasks execute successfully in sequence
- ✅ XCom shows data transfer between tasks
- ✅ Logs confirm data processing at each step
- ✅ Database contains the calculated average rating
- ✅ Daily_Metrics table is created with correct schema
- ✅ Pipeline is idempotent (can be re-run safely)
