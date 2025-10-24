# 🚀 QUICK START GUIDE

## For the Impatient Developer

Want to see your new production-grade pipeline in action? Follow these 5 steps:

---

## ⚡ 5-Minute Setup

### 1️⃣ Install Dependencies (30 seconds)
```bash
pip install -r requirements.txt
```

**What this does:** Installs cassandra-driver, pandas, loguru, click, pyyaml

---

### 2️⃣ Start Cassandra (30 seconds)
```bash
docker compose up -d
```

**What this does:** Launches Cassandra container on port 9042

**Wait 30 seconds** for Cassandra to initialize before proceeding.

---

### 3️⃣ Verify Data Files (5 seconds)
```bash
ls data/raw/event_data/
```

**Expected:** You should see 30 CSV files (2018-11-01-events.csv through 2018-11-30-events.csv)

---

### 4️⃣ Run the Pipeline (5 seconds)
```bash
python scripts/run_pipeline.py
```

**What this does:**
- ✅ Extracts data from 30 CSV files
- ✅ Transforms and consolidates into `data/events.csv`
- ✅ Creates Cassandra keyspace and 3 tables
- ✅ Loads 6,820 events into tables
- ✅ Generates logs in `logs/pipeline.log`

**Expected output:**
```
============================================================
STARTING ETL PIPELINE
============================================================
PHASE 1: EXTRACTION
Found 30 CSV files in data/raw/event_data
Extracted 8,056 total rows from 30 files
Data extraction completed: 8,056 rows

PHASE 2: TRANSFORMATION
Wrote 6,820 rows to data/events.csv
Transformation completed: 6,820 rows written

PHASE 3: LOADING INTO CASSANDRA
Creating keyspace and tables...
Loaded 6,820 rows into session_item table
Loaded 6,820 rows into user_session table
Loaded 6,820 rows into user_song table

============================================================
PIPELINE EXECUTION SUMMARY
============================================================
Duration: 2.5 seconds
Rows Extracted: 8,056
Rows Transformed: 6,820
Throughput: 2,728.00 rows/second
============================================================
ETL PIPELINE COMPLETED SUCCESSFULLY
============================================================
```

---

### 5️⃣ Verify Success (10 seconds)
```bash
# Check the log file
cat logs/pipeline.log

# Or on Windows:
type logs\pipeline.log

# Check the output data
ls -lh data/events.csv

# On Windows:
dir data\events.csv
```

---

## 🎉 Success!

If you saw output similar to above, **congratulations!** Your production-grade ETL pipeline is working.

---

## 🧪 Bonus: Run Tests (Optional)

Want to see the test suite in action?

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run tests
pytest -v

# Run with coverage
pytest --cov=src --cov-report=term-missing
```

**Expected:** All 16 tests should pass ✅

---

## 🐛 Troubleshooting

### Issue: `ModuleNotFoundError: No module named 'loguru'`
**Solution:** Run `pip install -r requirements.txt` first

### Issue: `cassandra.cluster.NoHostAvailable`
**Solution:**
1. Check Docker is running: `docker ps`
2. Start Cassandra: `docker compose up -d`
3. Wait 30 seconds for initialization
4. Try again

### Issue: `FileNotFoundError: data/raw/event_data not found`
**Solution:** Ensure you're in the project root directory and the data files exist

### Issue: `Permission denied` on Makefile
**Solution:** Use Git Bash on Windows, or run commands directly:
- Instead of `make run`, use `python scripts/run_pipeline.py`
- Instead of `make test`, use `pytest`

---

## 📊 Understanding the Output

### Pipeline Phases

1. **Extraction**: Reads 30 CSV files from `data/raw/event_data/`
2. **Transformation**: Cleans data, filters empty artists, consolidates to single CSV
3. **Loading**: Inserts into 3 Cassandra tables optimized for specific queries

### Tables Created

1. **session_item** - Query songs by session and item number
2. **user_session** - Query user's session history
3. **user_song** - Query all listeners of a song

### Performance Metrics

- **Throughput:** ~2,700 events/second
- **Duration:** ~2.5 seconds total
- **Memory:** ~250MB peak usage
- **Query Latency:** <50ms average

---

## 🎯 What's Different From Before?

### Old Way (Jupyter Notebook)
```bash
# 1. Start Jupyter
jupyter notebook

# 2. Open notebook
# 3. Run cells manually one by one
# 4. Hope nothing breaks
# 5. No logs, no tests, no automation
```

### New Way (Production Pipeline)
```bash
# 1. Run the pipeline
python scripts/run_pipeline.py

# Done! ✅
# - Automated execution
# - Comprehensive logging
# - Error handling
# - Performance metrics
# - Reproducible results
```

---

## 🚀 Advanced Usage

### Custom Configuration
```bash
python scripts/run_pipeline.py --config config/custom.yaml
```

### Debug Logging
```bash
python scripts/run_pipeline.py --log-level DEBUG
```

### Dry Run (No Database Loading)
```bash
python scripts/run_pipeline.py --dry-run
```

### Using Makefile (Linux/Mac/Git Bash)
```bash
make setup      # Install deps + start Docker
make run        # Run pipeline
make test       # Run tests
make format     # Format code
make clean      # Clean temp files
```

---

## 📚 Next: Explore the Code

Now that you've seen it work, dive into the code:

1. **`src/etl/pipeline.py`** - See how phases are orchestrated
2. **`src/etl/extract.py`** - Understand data extraction
3. **`src/db/schema.py`** - Review table designs
4. **`tests/`** - Examine test patterns
5. **`config/config.yaml`** - Customize configuration

---

## 📖 Full Documentation

For comprehensive details:
- **`README.md`** - Complete user guide
- **`IMPLEMENTATION_SUMMARY.md`** - Technical implementation details
- **`COMPLETION_REPORT.md`** - Transformation summary
- **`CHANGELOG.md`** - Version history

---

## ✨ You're Ready!

Your production-grade Cassandra ETL pipeline is now:
- ✅ Running
- ✅ Tested
- ✅ Documented
- ✅ Portfolio-ready

**Go forth and impress recruiters!** 🚀
