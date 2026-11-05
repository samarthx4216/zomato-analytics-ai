# Zomato AI Data Engineering Pipeline

> Built by **Samarth Marathe** · [LinkedIn](https://www.linkedin.com/in/samarthmarathe/) · [GitHub](https://github.com/samarthx4216) · [Email](samarthmarathe39@gmail.com)

I built an end-to-end batch data pipeline that takes Zomato-style food delivery data from raw CSVs all the way to AI-powered analytics:

**Food Delivery Dataset → Amazon S3 → Snowflake → dbt → Airflow → AI (Groq)**

The data lands in an S3 data lake and flows into Snowflake through a storage integration. dbt then transforms it through medallion layers: RAW (Bronze) tables loaded with `COPY INTO`, cleaned STAGING (Silver) views, and business-ready MARTS (Gold) with dimensions, incremental facts, and aggregate marts. Apache Airflow orchestrates everything as one daily DAG. On top of the warehouse I added an AI layer powered by Groq's fast LLM inference: LLM enrichment turns free-text reviews into structured columns, RAG lets me chat with the reviews, and text-to-SQL lets me query the warehouse in plain English. Streamlit serves the AI apps.

![Architecture](docs/architecture.png)

## Why I built this

[Write 2-3 sentences in your own words. Example: "I wanted hands-on experience with the modern data stack used in real companies, instead of only learning each tool in isolation. This project let me connect cloud storage, a warehouse, transformations, orchestration, and AI in one working pipeline."]

## What I built

| Layer | Where | What |
|---|---|---|
| **Source** | `data/` (local) | 4 real dimension CSVs (restaurants, users, food, menu) + 3 generated fact files: **10M orders**, **~23M order items**, **300K free-text reviews** |
| **Lake** | Amazon S3 | One bucket, `raw/<table>/` folder per CSV |
| **Bronze** | Snowflake `ZOMATO.RAW` | `COPY INTO` from S3 via a keyless storage integration |
| **Silver** | Snowflake `ZOMATO.STAGING` | dbt staging views that clean, type, and rename every source |
| **Gold** | Snowflake `ZOMATO.MARTS` | Dimensions, **incremental** facts (MERGE), business marts, and an SCD2 snapshot |
| **AI** | Snowflake `ZOMATO.AI` | LLM-enriched reviews (sentiment/topic), RAG chat, text-to-SQL |
| **Orchestration** | Airflow (Docker) | One daily DAG: load → transform → enrich → AI mart |

## Tech stack

Python · Pandas · Amazon S3 · Snowflake · dbt (dbt-snowflake) · Apache Airflow 3 (Docker) · Groq API (`llama-3.3-70b-versatile`, via the OpenAI-compatible Python SDK) · Streamlit

## Repository structure

```
├── airflow/                  # Airflow 3 on Docker
│   ├── Dockerfile            #   Snowflake + OpenAI-compatible SDK, dbt in its own venv
│   ├── docker-compose.yaml   #   postgres + api-server + scheduler; creds via env vars
│   ├── example.env           #   template for SNOWFLAKE_* / GROQ_API_KEY
│   └── dags/zomato_batch.py  #   the pipeline DAG (4 tasks)
├── zomato/                   # dbt project
│   ├── models/staging/       #   7 staging views (Silver) + sources + tests
│   ├── models/marts/         #   dims, incremental facts, business marts (Gold)
│   └── macros/               #   custom schema-name macro
├── ai/                       # AI layer
│   ├── enrich_reviews.py     #   LLM enrichment → ZOMATO.AI.REVIEW_ENRICHED
│   ├── rag_chat.py           #   RAG: "chat with your reviews" (Streamlit)
│   ├── text_to_sql.py        #   text-to-SQL: "chat with your warehouse" (Streamlit)
│   └── example.env           #   template for the AI credentials
├── snowflake/                # Snowflake setup SQL (run in Snowsight, in order)
│   ├── 01_setup.sql          #   warehouse ZOMATO_WH, database ZOMATO, schemas, role
│   ├── 02_storage_integration.sql  # keyless S3 link (pairs with aws/iam/)
│   ├── 03_stage_and_formats.sql    # external stage + CSV file format
│   ├── 04_raw_tables.sql     #   RAW (Bronze) table DDL, column order matches the CSVs
│   └── 05_copy_into.sql      #   COPY INTO RAW from the stage
├── aws/iam/                  # IAM policy + role trust policies for the S3 ↔ Snowflake handshake
└── docs/architecture.png     # architecture diagram
```

> The `data/` folder (~2.3 GB of CSVs), logs, and dbt `target/` artifacts are not committed. See the [Dataset](#dataset) section for the download link.

## How the pipeline works

### 1. Data lands in S3

I uploaded the seven CSVs to `s3://<BUCKET>/raw/<table>/`, with one folder per table (`restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`).

### 2. S3 → Snowflake with a keyless handshake

Snowflake reads the bucket with **no stored keys**, using a storage integration and an IAM role. The Snowflake side is [`snowflake/02_storage_integration.sql`](snowflake/02_storage_integration.sql); the AWS JSON documents live in [`aws/iam/`](aws/iam/):

| File | Used for |
|---|---|
| [`s3-read-policy.json`](aws/iam/s3-read-policy.json) | IAM **policy** `zomato-s3-read`: read-only access to the bucket |
| [`snowflake-role-trust-policy-initial.json`](aws/iam/snowflake-role-trust-policy-initial.json) | IAM **role** `snowflake-s3-role`: placeholder trust used at creation time |
| [`snowflake-role-trust-policy-final.json`](aws/iam/snowflake-role-trust-policy-final.json) | Final trust: Snowflake's IAM user ARN + external ID from `DESC INTEGRATION` |

The order matters: create the AWS policy and role, create the Snowflake `STORAGE INTEGRATION` pointing at the role ARN, run `DESC INTEGRATION` to get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`, then paste both into the role's trust policy.

Two things I learned the hard way: the trust `Principal` must be Snowflake's IAM user ARN (not `:root`), and re-running `CREATE OR REPLACE` on the integration regenerates the external ID and breaks the trust.

### 3. Load with `COPY INTO`

The table DDL ([`snowflake/04_raw_tables.sql`](snowflake/04_raw_tables.sql)) matches each CSV's column order. [`snowflake/05_copy_into.sql`](snowflake/05_copy_into.sql) then loads each file from the stage into `ZOMATO.RAW`: 10M orders, ~23M order items, and 300K reviews.

### 4. Transform with dbt (medallion architecture)

- **Staging (Silver):** one view per source. This step parses the messy restaurant dimension (`--` → null, `₹ 200` → 200), lowercases emails, derives `is_delivered`, and so on.
- **Dimensions (Gold):** `dim_restaurants`, `dim_customer` (with age segments), `dim_food`, and a generated `dim_date` calendar.
- **Facts (Gold, incremental):** `fct_orders` and `fact_order_items` use `materialized='incremental'` with a MERGE strategy, so a re-run processes only new rows instead of rebuilding 10M+.
- **Marts (Gold):** one table per business question: daily city revenue (GMV/AOV/cancel rate), restaurant performance, delivery SLA (p50/p90 by city and hour), and review insights.
- **Tests:** `unique`, `not_null`, `relationships`, `accepted_values`, plus a singular reconciliation test. `dbt build` runs models and tests in dependency order.

### 5. Orchestrate with Airflow

One daily DAG, [`zomato_batch`](airflow/dags/zomato_batch.py), runs the whole pipeline as a single graph:

```
reload_raw  →  dbt_build_core  →  enrich_reviews  →  dbt_build_ai
(COPY from S3)  (dbt build + tests)  (Groq enrichment)   (AI mart)
```

Credentials never touch the code. Docker Compose injects `SNOWFLAKE_*` environment variables (read by dbt's `profiles.yml` via `env_var()`) and an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection for the COPY task.

### 6. AI layer

1. **LLM enrichment** (`ai/enrich_reviews.py`): uses the LLM as a transformation step. It reads review text, asks Groq (`llama-3.3-70b-versatile`) for structured JSON (sentiment + topic), and writes it to `ZOMATO.AI.REVIEW_ENRICHED`, which dbt then models into `mart_review_insights` like any other table. It is idempotent and sample-capped (`SAMPLE_N`), so the same review is never paid for twice.
2. **RAG** (`ai/rag_chat.py`): chat with the reviews. It embeds reviews using [your embedding model, e.g. a local sentence-transformers model], retrieves the most similar ones for a question, and generates an answer grounded in real reviews, with sources.
3. **Text-to-SQL** (`ai/text_to_sql.py`): chat with the warehouse. The LLM gets the marts' schema and writes Snowflake SQL for an English question. A SELECT-only guard validates it before it runs as `DBT_ROLE`.

## Key learnings

[Fill this in with your own experience. A few prompts to get you started:]

- [What was the hardest part to set up, and how did you fix it? (e.g., the S3 ↔ Snowflake trust policy, Airflow in Docker, dbt incremental models)]
- [What did you learn about incremental models vs full refresh on 10M+ rows?]
- [What did you learn about using an LLM inside a data pipeline?]
- [What would you improve next?]

> **Note on the LLM provider:** Groq's API is OpenAI-compatible, so the scripts use the standard `openai` Python SDK with `base_url="https://api.groq.com/openai/v1"` and my `GROQ_API_KEY`. Any OpenAI-compatible provider can be swapped in by changing the base URL, key, and model name.

## Running it

```bash
# Snowflake objects (warehouse ZOMATO_WH, database ZOMATO, schemas RAW/STAGING/MARTS/SNAPSHOTS/AI, role DBT_ROLE)
# + the S3 storage integration: run snowflake/01→05 in Snowsight. See aws/iam/ for the AWS side.

# dbt
cd zomato
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai

# Airflow
cd airflow
cp example.env .env          # fill SNOWFLAKE_*, GROQ_API_KEY, SAMPLE_N
docker compose build && docker compose up -d
# http://localhost:8080 → un-pause zomato_batch → Trigger

# AI apps
export GROQ_API_KEY=gsk_...
python ai/enrich_reviews.py
streamlit run ai/rag_chat.py      # chat with reviews
streamlit run ai/text_to_sql.py   # chat with the warehouse
```

## Future improvements

- [Add your own ideas, e.g., a BI dashboard on top of the marts, CI for dbt tests, data quality alerts, cost monitoring]

## Dataset

Download the CSVs from this [Google Drive folder](https://drive.google.com/drive/folders/1FEnGWMHhHzzTUCZOw1-YnH2v3DMuM-rs?usp=sharing) and place them under `data/` (they are too large to commit).
