# crm-erp-etl pipeline

---

## 🧩 Data Flow Overview

| Stage      | Description                                                                 |
|------------|-----------------------------------------------------------------------------|
| **Extract**   | Verifies presence of source CSVs, reads them into pandas DataFrames, and writes raw copies to `datasets/raw`. |
| **Transform** | Cleans and standardizes data: type conversions, missing/negative value handling, datetime normalization, mapping categorical values. Saves cleaned outputs to `datasets/clean`. |
| **Load**      | Copies cleaned files from `datasets/clean` to `datasets/final`.            |
| **Orchestration** | A single daily DAG scheduled at midnight, with tasks chained: `task_1` → `extract` → `transform` → `load`. |


- **Clear ETL pipeline** with logical separation: extract, transform, load.
- **Data quality enforcement**:
  - Null filtering and deduplication.
  - Zeroing out negative numbers.
  - Correcting future dates.
  - Normalizing strings and categories.
- **Idempotent pipeline**, ensuring consistent outputs across runs.

---

## 🚀 Next‑Level Enhancements

1. **TaskFlow API** – Switch to `@dag` and `@task` decorators for cleaner syntax and XCom handling.
2. **Smaller transform tasks** – Break `transform()` into separate functions (e.g., `clean_customers`, `clean_products`, `clean_sales`) for better parallelism and maintainability.
3. **Retries and alerts** – Enhance robustness with `retries`, `retry_delay`, and `on_failure_callback` for error notifications.
4. **Unit tests** – Extract transform logic into testable functions and add tests for quality assurance.
5. **Typed parameters** – Use default args and Jinja templates for dates & variables, avoiding Python `now()` at compile time.



