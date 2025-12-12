# RevOps Daily Metrics Pipeline

## Overview

This project is a **portfolio-ready RevOps analytics pipeline** that ingests raw CRM data, models it into analytics-friendly fact tables, and computes **daily revenue operations metrics** automatically.

The system is intentionally designed to mirror **real-world data team practices**:

* Clear separation between *source-of-truth CRM data* and *analytics/metrics*
* SQL-first metric definitions inside PostgreSQL
* Idempotent daily runs with historical backfill support
* Automated execution via GitHub Actions

---

## High-Level Architecture

**Data Flow:**

```
Raw CRM JSON
   ↓
PostgreSQL (revops_training)
  └── crm.*  ← Salesforce-style mirror
   ↓ (postgres_fdw)
PostgreSQL (revops_metrics)
  ├── crm_remote.*   ← read-only foreign tables
  └── metrics.*      ← daily fact tables
   ↓
GitHub Actions (scheduled & manual)
```

**Key principle:** All business logic lives in SQL. Python is used only as a lightweight orchestration layer.

---

## Databases

### 1. `revops_training` (Source)

Purpose: **Raw CRM mirror**

* Populated from Salesforce-style JSON exports
* Tables live in the `crm` schema
* Write access limited to ETL scripts

Tables:

* `crm.leads`
* `crm.opportunities`
* `crm.activities`
* `crm.interactions`
* `crm.reps`
* `crm.accounts`

---

### 2. `revops_metrics` (Analytics)

Purpose: **Analytics & reporting**

Schemas:

* `crm_remote` — foreign tables via `postgres_fdw`
* `metrics` — derived fact tables

This database contains *no raw ingestion logic* and is safe for BI tools and automation.

---

## Metrics Tables

All metrics are computed **per day** and are fully recomputable.

### `metrics.daily_lead_funnel`

Daily funnel health metrics:

* Total leads to date
* New leads created
* SQLs (qualified leads)
* Opportunities created

Null lead sources are normalized to `"Unknown"`.

---

### `metrics.rep_daily_activity`

Sales activity per rep per day:

* Calls
* Emails
* Meetings
* Total activities

Unassigned activities are bucketed under `rep_id = -1` / `rep_name = 'Unassigned'`.

---

### `metrics.rep_daily_pipeline`

Pipeline and revenue performance per rep:

* Opportunities created
* Pipeline amount created
* Closed-won opportunities
* Revenue won

Handles missing ownership, amounts, and close dates defensively.

---

### `metrics.lead_stage_snapshot`

Daily snapshot of lead stages and velocity:

* Leads per status
* Average lead age (days)

Null statuses are normalized to `"Unknown"`. Negative ages are clamped to zero.

---

### `metrics.run_log`

Operational metadata:

* One row per successful daily run
* Used for freshness checks and monitoring

---

## SQL-First Design

All metric logic is implemented as **PL/pgSQL functions**:

* `metrics.refresh_daily_lead_funnel(date)`
* `metrics.refresh_rep_daily_activity(date)`
* `metrics.refresh_rep_daily_pipeline(date)`
* `metrics.refresh_lead_stage_snapshot(date)`

### Orchestration

```sql
SELECT metrics.run_daily_metrics(<date>);
```

This single entry point:

* Deletes and recomputes metrics for the given day
* Is fully idempotent
* Logs successful runs

---

## Backfill Support

Historical recomputation is handled natively in PostgreSQL.

### Backfill last N days

```sql
SELECT metrics.backfill_last_n_days(30);
```

### Backfill a date range

```sql
SELECT metrics.backfill_date_range('2025-10-01', '2025-12-11');
```

No Python loops or data duplication required.

---

## Automation (GitHub Actions)

The pipeline runs automatically using GitHub Actions.

### Scheduled Run

* Runs daily (cron)
* Computes yesterday’s metrics

### Manual Run

* Supports manual triggering
* Optional backfill parameter (`backfill_days`)

Secrets used:

* `PG_HOST`
* `PG_PORT`
* `PG_DATABASE`
* `PG_USER`
* `PG_PASSWORD`

Old Render-based secrets were removed after migrating infrastructure to AWS EC2.

---

## Python Orchestration

`run_daily_metrics.py` is a thin wrapper that:

* Connects to PostgreSQL
* Calls the appropriate SQL function
* Supports both daily runs and backfills

All business logic remains in the database.

---

## Why This Project Matters

This project demonstrates:

* SQL-first analytics engineering
* Realistic RevOps metric modeling
* Defensive handling of incomplete CRM data
* Cross-database querying with PostgreSQL FDW
* Idempotent pipelines with backfill support
* CI/CD-style automation

It intentionally mirrors patterns used by modern data teams.

---

## Future Enhancements

* BI dashboard (Metabase / Superset)
* Data quality checks & alerts
* Metrics freshness monitoring
* Dimension tables for reps & dates
* Incremental optimizations for scale

---

## Resume Summary

> Built a SQL-first RevOps analytics pipeline on PostgreSQL, separating raw CRM ingestion and analytics into distinct databases. Implemented daily funnel, activity, and pipeline metrics using idempotent PL/pgSQL functions, added historical backfill support, and automated execution via GitHub Actions with secure secret management.

---

## Author

Corey Smith
