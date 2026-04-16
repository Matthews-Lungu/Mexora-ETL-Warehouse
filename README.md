# Mexora Analytics — ETL Pipeline & Data Warehouse

> **Mini-Project 1 — Data Engineering**  
> Abdelmalek Essaâdi University — Bachelor's Degree in Data Analytics  
> Company: Mexora (fictional Moroccan e-commerce platform)

---

## Project Overview

This project builds a complete Business Intelligence system for Mexora:
a dimensional data warehouse loaded by a Python ETL pipeline, with a
Power BI dashboard answering 5 key business questions.

**Architecture:**

```
Raw CSV/JSON files
      │
      ▼
[Python ETL Pipeline]  ←  extract / transform / load
      │
      ▼
[PostgreSQL DWH]  ←  star schema (5 dimensions + 1 fact table)
      │
      ▼
[Power BI Dashboard]  ←  5 analytical pages
```

---

## Prerequisites

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11+ | ETL pipeline |
| PostgreSQL | 15+ | Data warehouse |
| DBeaver | any | SQL interface (recommended over pgAdmin) |
| Power BI Desktop | any | Dashboard |
| Git | any | Version control |

---

## Project Structure

```
mexora_etl/
├── config/
│   └── settings.py          # all parameters and DB credentials
├── data/                    # raw source files (generated)
│   ├── orders_mexora.csv
│   ├── clients_mexora.csv
│   ├── products_mexora.json
│   └── regions_maroc.csv
├── extract/
│   └── extractor.py         # read raw files, no transformation
├── transform/
│   ├── clean_commandes.py   # 7 cleaning rules for orders
│   ├── clean_clients.py     # 5 cleaning rules + segmentation
│   ├── clean_produits.py    # 3 cleaning rules for products
│   └── build_dimensions.py  # build all 5 dimensions + fact table
├── load/
│   └── loader.py            # write to PostgreSQL
├── utils/
│   └── logger.py            # timestamped file + terminal logging
├── sql/
│   ├── create_dwh.sql       # run ONCE before the pipeline
│   └── check_integrity.sql  # run AFTER the pipeline to verify
├── logs/                    # auto-created; one .log file per run
├── docs/                    # step deliverables
├── main.py                  # pipeline entry point
├── generate_data.py         # generates the 4 raw data files
├── requirements.txt
└── README.md
```

---

## Setup — Step by Step

### Step 1 — Clone the repository

```bash
git clone https://github.com/<your-username>/mexora_etl.git
cd mexora_etl
```

### Step 2 — Install Python dependencies

```bash
pip install -r requirements.txt
```

### Step 3 — Create your .env file

Create a file named `.env` at the project root:

```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mexora_dwh
DB_USER=postgres
DB_PASS=your_postgres_password
```

> ⚠️ Never commit this file to Git. It is already in `.gitignore`.

### Step 4 — Create the PostgreSQL database

Open DBeaver (or psql) and run:

```sql
CREATE DATABASE mexora_dwh;
```

### Step 5 — Create the DWH schema

In DBeaver, connect to `mexora_dwh` and run the full contents of:

```
sql/create_dwh.sql
```

This creates 3 schemas, 5 dimension tables, 1 fact table, all indexes,
and 3 materialized views. It is safe to re-run (uses DROP IF EXISTS).

### Step 6 — Generate the raw data files

```bash
python generate_data.py
```

This creates the 4 files in `data/` with intentional quality issues.

### Step 7 — Run the ETL pipeline

```bash
python main.py
```

Expected output:

```
DÉMARRAGE PIPELINE ETL MEXORA
--- PHASE 1 : EXTRACT ---
--- PHASE 2 : TRANSFORM ---
--- PHASE 3 : LOAD ---
--- PHASE 4 : REFRESH VUES MATÉRIALISÉES ---
PIPELINE TERMINÉ EN XX secondes
Lignes dans fait_ventes : ~49,000
```

### Step 8 — Verify integrity

In DBeaver, run `sql/check_integrity.sql`.  
All CHECK queries should return **0 problems**.

---

## Data Quality Issues Handled

| File | Issue | Rule | Action |
|---|---|---|---|
| orders | ~3% duplicate order IDs | R1 | Keep last occurrence |
| orders | 3 mixed date formats | R2 | Normalise to YYYY-MM-DD |
| orders | Dirty city names | R3 | Map via regions_maroc reference |
| orders | Non-standard statuses (OK, KO, DONE…) | R4 | Map to 4 canonical values |
| orders | Negative quantities | R5 | Remove rows |
| orders | Zero unit prices (test orders) | R6 | Remove rows |
| orders | 7% missing delivery person ID | R7 | Replace with '-1' |
| clients | Duplicate emails (migration error) | R1 | Keep most recent inscription |
| clients | Gender encoded 5 different ways | R2 | Normalise to m/f/inconnu |
| clients | Invalid birth dates (age < 16 or > 120) | R3 | Nullify |
| clients | Malformed emails | R4 | Nullify |
| products | Inconsistent category capitalisation | R1 | Title case |
| products | Null catalogue prices | R3 | Fill with subcategory median |

---

## Connecting Power BI

1. Open Power BI Desktop
2. **Get Data → PostgreSQL database**
3. Server: `localhost`, Database: `mexora_dwh`
4. Import these tables:
   - `dwh_mexora.dim_temps`
   - `dwh_mexora.dim_produit`
   - `dwh_mexora.dim_client`
   - `dwh_mexora.dim_region`
   - `dwh_mexora.dim_livreur`
   - `dwh_mexora.fait_ventes`
   - `reporting_mexora.mv_ca_mensuel`
   - `reporting_mexora.mv_top_produits`

---

## ETL Pipeline Summary

| Phase | Input | Output | Rows |
|---|---|---|---|
| Extract | 4 raw files | 4 raw DataFrames | 51,500 orders |
| Transform | Raw DataFrames | Cleaned DataFrames | 49,567 orders |
| Build dims | Cleaned data | 5 dimension tables | see table |
| Build facts | Dims + orders | fait_ventes | ~47,700 rows |
| Load | DataFrames | PostgreSQL tables | all tables |

**Dimension row counts after pipeline:**

| Table | Rows |
|---|---|
| dim_temps | 2,192 (6 years daily) |
| dim_produit | 50 |
| dim_region | 25 |
| dim_livreur | 16 |
| dim_client | ~2,869 |
| fait_ventes | ~49,567 |
