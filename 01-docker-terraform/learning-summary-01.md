## 📝 Module 01 — Learning Summary

### Key learnings

* **Docker & Docker Compose:** Run the Python app and Postgres in containers for a consistent environment.
* **Modular codebase:** Refactored into `main.py`, `ingestor.py`, and `utils.py` for easier maintenance.
* **Scalable ingestion:** Used chunking for large files and automated datetime casting to keep schemas consistent.
* **Persistent logging:** Integrated `loguru` and persisted logs via Docker volume mounts (`logs/app.log`).

### Challenges

* **Docker networking:** Connecting to Postgres via the service name (`db`) instead of `localhost`.
* **Mixed data types:** `DtypeWarning` issues handled with `low_memory=False` or explicit dtypes.
* **Imports & pathing:** Managing `PYTHONPATH`/packaging so `modules/` imports work in local and Docker runs.
* **Volume mapping:** Ensuring `data/` and `logs/` stay synced so outputs/logs survive container restarts.

### Insight

The result is an **ingestion tool** that is **flexible**, **traceable via logs**, and **easy to extend**.
