# 🚀 Quick Start Guide - Place Metric Ingestion Pipeline

## What You'll Get

✅ Airflow DAG named `place_metric_ingestion`  
✅ 3 sequential tasks (Extract → Transform → Load)  
✅ XCom data passing between tasks  
✅ Daily_Metrics database table with average ratings  
✅ Full logging and monitoring  

---

## 🎯 5-Minute Setup

### 1. Start Airflow
```powershell
cd d:\kyanon\progress\mini_airflow
.\start.bat
```
Wait 2-3 minutes for services to start.

### 2. Access Airflow UI
- Open browser: http://localhost:8080
- Login: `airflow` / `airflow`

### 3. Run the DAG
1. Find `place_metric_ingestion` in the DAG list
2. Click the toggle to **ON** (unpause)
3. Click the **▶ Play** button → "Trigger DAG"
4. Watch the tasks turn green ✅

### 4. Verify Success

**Check XCom values:**
- Click on the DAG run
- Go to "XCom" tab
- Verify `places_data` and `average_rating` exist

**Check Logs:**
- Click each task (extract_places, transform_rating, load_to_db)
- View logs to see data processing

**Check Database:**
```powershell
.\query_db.bat
```

---

## 📊 What Happens in Each Task

### Task 1: `extract_places` (Extract)
```
├─ Reads source_data.json (38,993 place records)
├─ Logs: "Successfully fetched X places from mock API"
└─ Pushes places list to XCom
```

### Task 2: `transform_rating` (Transform)
```
├─ Pulls places from XCom
├─ Filters places with valid ratings
├─ Calculates average rating
├─ Logs: "Calculated average rating: X.XX from Y places"
└─ Pushes average to XCom
```

### Task 3: `load_to_db` (Load)
```
├─ Pulls average rating from XCom
├─ Creates Daily_Metrics table (if not exists)
├─ Inserts/updates record for today's date
├─ Logs: "Successfully inserted/updated metric for YYYY-MM-DD"
└─ Returns success status
```

---

## 🎓 Key Airflow Concepts Demonstrated

| Concept | How It's Used |
|---------|---------------|
| **PythonOperator** | All 3 tasks use custom Python functions |
| **XCom** | Data passed between tasks via push/pull |
| **Task Context** | Access to `ti` (task instance) and `ds` (date) |
| **Task Dependencies** | `extract >> transform >> load` |
| **Idempotency** | Re-running updates existing DB records |
| **Logging** | All tasks log their operations |

---

## 📁 File Structure

```
mini_airflow/
├── dags/
│   └── place_metric_ingestion.py     # ⭐ Main DAG definition
├── src/
│   ├── ingestion/
│   │   └── mock_api_extract.py       # Extract logic
│   ├── transform/
│   │   └── calculate_average_rating.py # Transform logic
│   └── load/
│       └── db_loader.py               # Load logic
├── data/source/
│   └── source_data.json               # 38K+ place records
├── start.bat                          # 🚀 Start Airflow
├── stop.bat                           # 🛑 Stop Airflow
├── query_db.bat                       # 🔍 Query database
└── README.md                          # Full documentation
```

---

## ✅ Verification Checklist

After running the DAG, verify:

- [ ] All 3 tasks show green checkmarks in Airflow UI
- [ ] XCom tab shows `places_data` and `average_rating`
- [ ] Extract logs: "Successfully fetched X places"
- [ ] Transform logs: "Calculated average rating: X.XX"
- [ ] Load logs: "Successfully inserted/updated metric"
- [ ] Database query returns a record with today's date
- [ ] Average rating value is between 1.0 and 5.0

---

## 🛠️ Useful Commands

| Command | Description |
|---------|-------------|
| `.\start.bat` | Start all Airflow services |
| `.\stop.bat` | Stop all services |
| `.\logs.bat` | View live logs |
| `.\query_db.bat` | Query Daily_Metrics table |
| `docker ps` | Check running containers |
| `docker-compose down -v` | Stop and delete all data |

---

## 🎯 Success Indicators

**In Airflow UI:**
```
place_metric_ingestion
  └── DAG Runs: 1 success
      ├── extract_places ✅ (00:00:05)
      ├── transform_rating ✅ (00:00:02)
      └── load_to_db ✅ (00:00:01)
```

**In Database:**
```sql
id | metric_date | average_rating | total_places | places_with_rating
1  | 2025-12-24  | 4.52          | 38993        | 38500
```

**In Logs:**
```
[2025-12-24 10:30:45] Successfully fetched 38993 places
[2025-12-24 10:30:47] Calculated average rating: 4.52 from 38500 places
[2025-12-24 10:30:48] Successfully inserted metric for 2025-12-24
```

---

## 🆘 Troubleshooting

### DAG not appearing?
```powershell
# Check for errors
docker logs mini_airflow-airflow-dag-processor-1
```

### Task failed?
1. Click on the red task box
2. View "Log" tab
3. Check error message
4. Fix the issue and click "Clear" → "Retry"

### Can't query database?
```powershell
# Check worker is running
docker ps | findstr worker

# If not, restart services
.\stop.bat
.\start.bat
```

---

## 📚 Learn More

- Full documentation: [README.md](README.md)
- Testing guide: [TESTING.md](TESTING.md)
- Official Airflow docs: https://airflow.apache.org

---

## 🎉 What's Next?

Try modifying the pipeline:
- Change the schedule: `schedule_interval='@hourly'`
- Add data validation in transform task
- Load data to PostgreSQL instead of SQLite
- Add email notifications on failure
- Create additional metrics (min, max, median ratings)

---

**Ready? Let's go!** 🚀

```powershell
cd d:\kyanon\progress\mini_airflow
.\start.bat
```

Then open http://localhost:8080 and trigger your first DAG run!
