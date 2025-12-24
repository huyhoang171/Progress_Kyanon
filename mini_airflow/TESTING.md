# 🧪 Testing Checklist for Place Metric Ingestion DAG

Use this checklist to verify that the Airflow DAG meets all requirements.

## ✅ Pre-deployment Checks

- [x] All Python files have valid syntax (run `check_syntax.py`)
- [x] All modules have `__init__.py` files
- [x] Source data file exists at `data/source/source_data.json`
- [x] Docker volumes configured in `docker-compose.yaml`
- [x] Requirements.txt includes all dependencies

## ✅ DAG Configuration

- [ ] DAG ID is `place_metric_ingestion`
- [ ] DAG appears in Airflow UI DAG list
- [ ] DAG can be toggled ON (unpaused)
- [ ] DAG has 3 tasks: extract_places, transform_rating, load_to_db
- [ ] Tasks are sequential: extract >> transform >> load

## ✅ Task 1: Extract (PythonOperator)

- [ ] Task executes successfully
- [ ] Logs show "Successfully fetched X places from mock API"
- [ ] Logs show "Extract complete: X total places, Y with ratings"
- [ ] XCom key `places_data` is created
- [ ] XCom value contains list of place dictionaries
- [ ] Task returns summary dictionary with counts

### Verification Steps:
```
1. Go to Airflow UI
2. Click on DAG run → extract_places task
3. View Logs tab - should show success messages
4. View XCom tab - should show 'places_data' key
```

## ✅ Task 2: Transform (PythonOperator)

- [ ] Task executes successfully
- [ ] Logs show "Calculated average rating: X.XX from Y places"
- [ ] Logs show "Transform complete: {dictionary with results}"
- [ ] Task pulls `places_data` from XCom
- [ ] XCom key `average_rating` is created
- [ ] XCom value is a float number
- [ ] Task returns transformation results dictionary

### Verification Steps:
```
1. Go to Airflow UI
2. Click on DAG run → transform_rating task
3. View Logs tab - should show calculation messages
4. View XCom tab - should show 'average_rating' key with float value
```

## ✅ Task 3: Load (DB Operator)

- [ ] Task executes successfully
- [ ] Logs show "Daily_Metrics table created/verified"
- [ ] Logs show "Successfully inserted/updated metric for YYYY-MM-DD"
- [ ] Logs show database record details
- [ ] Task pulls `average_rating` from XCom
- [ ] Database table `Daily_Metrics` is created
- [ ] Record is inserted with correct data
- [ ] Task returns success status dictionary

### Verification Steps:
```
1. Go to Airflow UI
2. Click on DAG run → load_to_db task
3. View Logs tab - should show database operations
4. Run query_db.bat to verify database record
```

## ✅ XCom Data Flow

- [ ] extract_places pushes data to XCom
- [ ] transform_rating pulls data from XCom
- [ ] transform_rating pushes result to XCom
- [ ] load_to_db pulls result from XCom

### Verification Steps:
```
1. Go to Airflow UI → DAG run
2. Click "XCom" tab
3. Verify these keys exist:
   - places_data (from extract_places)
   - average_rating (from transform_rating)
```

## ✅ Database Verification

- [ ] Daily_Metrics table exists
- [ ] Table has correct schema (id, metric_date, average_rating, etc.)
- [ ] Record is inserted for the execution date
- [ ] average_rating value matches XCom value
- [ ] total_places and places_with_rating are populated
- [ ] created_at and updated_at timestamps are set
- [ ] Re-running DAG updates existing record (not duplicates)

### Verification Commands:
```powershell
# Enter worker container
docker exec -it mini_airflow-airflow-worker-1 bash

# Check table schema
sqlite3 /opt/airflow/airflow.db ".schema Daily_Metrics"

# Query records
sqlite3 /opt/airflow/airflow.db "SELECT * FROM Daily_Metrics;"

# Count records
sqlite3 /opt/airflow/airflow.db "SELECT COUNT(*) FROM Daily_Metrics;"
```

## ✅ Log Messages

### Extract Task Logs Should Contain:
```
Successfully fetched 50 places from mock API
Extract complete: 50 total places, XX with ratings
```

### Transform Task Logs Should Contain:
```
Calculated average rating: X.XX from XX places
Transform complete: {'average_rating': X.XX, 'total_places': 50, ...}
```

### Load Task Logs Should Contain:
```
Daily_Metrics table created/verified in /opt/airflow/airflow.db
Successfully inserted/updated metric for 2025-12-24: average_rating=X.XX, ...
Database record: (1, '2025-12-24', X.XX, ...)
```

## ✅ Error Handling

- [ ] DAG handles missing data gracefully
- [ ] Tasks log errors clearly
- [ ] Failed tasks can be retried
- [ ] Database operations are idempotent

### Test Scenarios:
```
1. Delete source_data.json - extract should handle gracefully
2. Modify data to have no ratings - transform should handle None
3. Re-run the same DAG - should update, not create duplicates
```

## ✅ Performance & Monitoring

- [ ] DAG completes within reasonable time
- [ ] Task durations are logged
- [ ] Graph View shows task dependencies correctly
- [ ] Gantt Chart shows execution timeline
- [ ] No memory issues or container crashes

## ✅ Documentation

- [ ] README.md explains the pipeline
- [ ] Code has docstrings
- [ ] Comments explain complex logic
- [ ] Testing checklist is complete

## 📊 Expected Results

| Metric | Expected Value |
|--------|---------------|
| Total Places | ~50 (from truncated JSON) |
| Places with Rating | ~45-50 |
| Average Rating | ~4.0-5.0 range |
| Task Count | 3 |
| XCom Keys | 2 (places_data, average_rating) |
| DB Records | 1 per execution date |

## 🎯 Success Criteria (All must pass)

1. ✅ DAG named `place_metric_ingestion` running successfully
2. ✅ Task 1 (Extract) calls mock API and pushes to XCom
3. ✅ Task 2 (Transform) calculates average rating and pushes to XCom
4. ✅ Task 3 (Load) inserts/updates Daily_Metrics table
5. ✅ Logs confirm XCom value transfer
6. ✅ Logs confirm successful database update
7. ✅ Database query shows correct data

## 🐛 Troubleshooting

### DAG not appearing
```powershell
# Check for syntax errors
python check_syntax.py

# Check dag processor logs
docker logs mini_airflow-airflow-dag-processor-1
```

### Task failures
```
1. Click on failed task
2. View logs for error details
3. Check XCom tab to verify data flow
4. Verify file paths and permissions
```

### Database issues
```powershell
# Verify worker is running
docker ps | findstr worker

# Check worker logs
docker logs mini_airflow-airflow-worker-1

# Access database directly
docker exec -it mini_airflow-airflow-worker-1 bash
sqlite3 /opt/airflow/airflow.db
```

## 📝 Testing Notes

Date: _________________
Tester: _______________

Issues Found:
_________________________________
_________________________________
_________________________________

Resolutions:
_________________________________
_________________________________
_________________________________
