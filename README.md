# CryptoLens 🕯️📊
### A Medallion Architecture Lakehouse for Crypto Market Data (Binance Vision Klines)

[![Spark](https://img.shields.io/badge/Apache%20Spark-Databricks-orange)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion%20Architecture-blue)](https://delta.io/)
[![Status](https://img.shields.io/badge/status-Phase%203-brightgreen)]()
[![License](https://img.shields.io/badge/license-MIT-lightgrey)]()

> An end-to-end, automated data pipeline that ingests real historical and daily OHLCV (Open, High, Low, Close, Volume) candlestick data for the top 50 Binance USDT trading pairs, processes it through **Bronze → Silver → Gold** layers on Apache Spark / Delta Lake, and serves a Power BI dashboard for market analysts.

---

## 📌 Project Overview

| | |
|---|---|
| **Domain** | Financial Markets / Cryptocurrency Trading Analytics |
| **Data Source** | [Binance Vision](https://data.binance.vision) (historical klines) + [Binance REST API](https://binance-docs.github.io/apidocs/spot/en/) (`/ticker/24hr`, `/exchangeInfo`) |
| **Dataset** | Spot Klines (1-minute candles) for the **top 50 USDT pairs** by 24h quote volume, selected dynamically — not a hardcoded symbol list |
| **Platform** | Databricks Free Edition (Serverless SQL/Compute) — Apache Spark + Delta Lake + Unity Catalog Volumes |
| **BI Tool** | Power BI |
| **Team** | Abdullah Saeed & Arqum Umais |
| **Course** | Data Analysis and Visualization — Semester Project |

CryptoLens tracks price action and trading activity across the 50 most actively traded Binance USDT spot pairs so analysts can study volatility, trend, and cross-asset correlation — the kind of daily-cadence analysis a trading desk or research team relies on.

---

## 🏗️ Architecture

```mermaid
flowchart LR
    A[Binance Vision<br/>data.binance.vision<br/>monthly + daily kline ZIPs] -->|Download + SHA256<br/>checksum verify| L(Local machine<br/>binance_bronze_ingest.py)
    X[Binance REST API<br/>ticker/24hr + exchangeInfo] -->|Top-50 symbol ranking<br/>+ live symbol status| L
    L -->|databricks fs cp<br/>CLI upload| B(Bronze Layer<br/>Unity Catalog Volume + Delta<br/>explicit schema, MERGE-idempotent)
    B -->|Cast, dedupe, MERGE| C(Silver Layer<br/>silver_klines + dim_symbol)
    C -->|Daily roll-up, indicators,<br/>MERGE + OPTIMIZE| D(Gold Layer<br/>Star schema: 2 dimensions + 4 facts)
    D -->|Databricks connector<br/>SQL warehouse, Import mode| E[Power BI Dashboard<br/>4 pages]
    J{{Databricks Job<br/>cryptolens_daily}} -.->|task 1| B
    J -.->|task 2| C
    J -.->|task 3| D
    B & C & D -.->|one log row per step| G[(pipeline_execution_logs)]
    G --> E

    style B fill:#cd7f32,color:#fff
    style C fill:#c0c0c0,color:#000
    style D fill:#ffd700,color:#000
```

**Ingestion pattern:**
- **Full Load** — one monthly 1-minute kline file per tracked symbol (~43,200 rows/symbol/month) across all 50 tracked pairs (~200 MB total).
- **Incremental Load** — one daily 1-minute kline file per tracked symbol (~1,440 rows/symbol/day), refreshed on every pipeline run (~10 MB/day total).
- **Checksum verification** — every downloaded ZIP is SHA256-hashed and compared against Binance's own `.CHECKSUM` sidecar file before being trusted and extracted.
- **Network workaround** — confirmed via direct DNS resolution testing that Databricks Free Edition's serverless compute cannot resolve `api.binance.com` or `data.binance.vision` (while `pypi.org`/`github.com` resolve fine) — an environment-specific domain allowlist, not a full internet block. Downloading therefore runs on a local machine (`binance_bronze_ingest.py`), and verified files are pushed into the Bronze Unity Catalog Volume via the Databricks CLI (`databricks fs cp`).
- **`dim_symbol`** — Binance's `/exchangeInfo` endpoint is snapshotted and merged into `dim_symbol` in Silver, since trading-pair status, listings, and delistings genuinely change over time. This gives the pipeline a real `MERGE` (insert + update + soft-delete), instead of the append-only kline data alone.

---

## 📂 Repository Structure

```
cryptolens/
├── docs/                                       # Proposals, data dictionary, phase deliverables
│   ├── Phase1_Project_Proposal_Crypto_Pipeline.docx
│   ├── Phase2_CryptoLens.pdf
│   ├── CryptoLens_Data_Dictionary.pdf          # Bronze + Silver
│   └── gold_data_dictionary.md                 # Gold: ER diagram, keys, grain, load strategy
├── notebooks/                                  # One notebook per pipeline stage
│   ├── 00_layers_verification.ipynb            # Validation: counts, idempotency, audit-log checks
│   ├── 01.1_bronze_ingest_data extractor.ipynb # Local-machine download + SHA256 verification
│   ├── 01.2_bronze_ingest.ipynb                # Databricks: Raw -> Bronze
│   ├── 02_silver_transform.ipynb               # Databricks: Bronze -> Silver
│   └── 03_gold_transform.ipynb                 # Databricks: Silver -> Gold (star schema)
├── dashboards/
│   └── CryptoLens_Dashboard.pbix               # Power BI report (4 pages)
├── .gitignore                                  # excludes bulk full_load/ and incremental_load/ data
└── README.md
```

**Notebook roles:**
- `01.1_bronze_ingest_data extractor` runs **locally** (not in Databricks): downloads monthly/daily kline ZIPs, verifies each SHA256 checksum, and extracts CSVs ready for upload to the Volume.
- `01.2_bronze_ingest` and `02_silver_transform` run in Databricks Serverless and implement the Raw → Bronze and Bronze → Silver stages.
- `03_gold_transform` runs in Databricks Serverless as the **last task** of the `cryptolens_daily` Job: it rolls the 1-minute Silver candles up to daily facts, derives volatility / moving-average / correlation tables, optimises them, and logs every step.
- `00_layers_verification` is a read-only validation notebook: table existence, per-layer row/symbol counts, idempotency re-run check, audit-log review, quarantine and quality-filter counts.

**Still to add before final submission:**
- `samples/` — one real, checksum-verified symbol (`BTCUSDT`, monthly + daily) with a schema README
- `docs/` — the final Phase 3 architecture & FinOps report (PDF)

The bulk 50-symbol working dataset (`full_load/`, `incremental_load/`) is intentionally **not** committed to git — it's excluded via `.gitignore` and instead lives in the Databricks Unity Catalog Volume (`workspace.cryptolens.bronze_raw`), keeping the repository lean while the real analytical dataset still meets the required size thresholds.

---

## 🗃️ Data Model

Full column-level definitions, types, and keys are in **[`docs/CryptoLens_Data_Dictionary.pdf`](docs/CryptoLens_Data_Dictionary.pdf)**. Summary:

### Bronze (`01.2_bronze_ingest.ipynb`)
- **`bronze_klines`** — raw kline rows, explicit schema (`StructType`, no `inferSchema`), tagged with `symbol`, `interval`, `load_type`, `source_file`, `load_timestamp`. Idempotent `MERGE` on `(symbol, interval, load_type, open_time)`.
- **`bronze_exchange_info`** — raw `/exchangeInfo` JSON snapshots, one per run, feeding `dim_symbol`.
- **`bronze_quarantine`** — rows that fail schema-on-read (PERMISSIVE mode + `_corrupt_record`) are routed here instead of crashing the batch.

### Silver (`02_silver_transform.ipynb`)
- **`silver_klines`** — one row per `(symbol, interval, open_time)`. Casts epoch-ms longs to `TIMESTAMP`, price columns (`open`/`high`/`low`/`close`) to `DECIMAL(18,8)`, and volume columns (`volume`, `quote_asset_volume`, `taker_buy_*_vol`) to the wider `DECIMAL(28,8)` — high-supply symbols can trade volumes that overflow `DECIMAL(18,8)`. Idempotent `MERGE` (`WHEN MATCHED THEN UPDATE` / `WHEN NOT MATCHED THEN INSERT`) handles both re-runs and a full-load file later correcting an incremental-load value. Quality rules drop rows where `high < low` or `close`/`open` fall outside `[low, high]`.
- **`dim_symbol`** — maintained with a real three-branch `MERGE`: `UPDATE` on status/filter change, `INSERT` for new listings, soft-delete (`is_active = false`) for delistings — history is preserved, never hard-deleted.

### Audit (all layers)
- **`pipeline_execution_logs`** — one row per file/batch processed at every layer, every run: `layer`, `parameter_processed`, `load_type`, `start_time`/`end_time`, `status` (`SUCCESS`/`FAILURE`/`SKIPPED`), `rows_inserted`, `rows_updated`, `error_message`.

### Gold (`03_gold_transform.ipynb`) — star schema in `workspace.gold`

Full column definitions and the ER diagram are in **[`docs/gold_data_dictionary.md`](docs/gold_data_dictionary.md)**.

| Table | Type | Grain | Key | Load strategy |
|---|---|---|---|---|
| `dim_symbol` | Dimension | one USDT trading pair (all pairs in the `exchangeInfo` snapshot) | `symbol_key` | `MERGE`: status changes recorded (`previous_status`, `status_changed_date`); pairs missing from the latest snapshot marked `DELISTED` |
| `dim_date` | Dimension | one calendar day | `date_key` (`yyyyMMdd`) | Overwrite (tiny, regenerated from the fact date range) |
| `fact_daily_ohlcv` | Fact | symbol × day, rolled up from 1-minute Silver | `(symbol_key, date_key)` | `MERGE`, incremental (reprocesses the latest 2 days) |
| `volatility_daily` | Derived fact | symbol × day | `(symbol_key, date_key)` | Overwrite from the fact table: daily / log returns, intraday range, 7-day rolling volatility (annualised) |
| `moving_averages` | Derived fact | symbol × day | `(symbol_key, date_key)` | Overwrite: short / long SMA, `BULLISH` / `BEARISH` trend, `GOLDEN_CROSS` / `DEATH_CROSS` flags |
| `symbol_correlation` | Derived fact | symbol pair × day | `(symbol_a_key, symbol_b_key, date_key)` | Overwrite: rolling 14-day correlation of daily log returns, top 15 symbols by quote volume |

- **Keys:** `symbol_key = abs(xxhash64(symbol))`, identical in every table, so Power BI joins on a single integer column. `date_key` is an `int` in `yyyyMMdd` form.
- **Relationships:** every fact table is many-to-one to `dim_symbol` and `dim_date` (eight active relationships in the Power BI model).
- **Read optimisation:** `OPTIMIZE … ZORDER BY (symbol_key, date_key)` runs after every load on the four fact tables (`symbol_a_key, date_key` for the correlation table).
- **Audit:** every Gold step writes to `pipeline_execution_logs` with `layer = 'Silver-to-Gold'`, including start/end time, source rows read, and rows inserted / updated (taken from Delta commit metrics for `MERGE` steps).

---

## 📐 Schema Reference

Column-level schemas for every table, extracted from the notebooks. Bronze and Silver tables live in `workspace.cryptolens`; Gold tables live in `workspace.gold`. Types for Bronze/Silver come from the explicit `StructType` / `cast` calls; types for Gold derived columns are what Spark produces from the transformation expressions (confirm with `DESCRIBE TABLE` if exact precision matters).

### 🥉 Bronze (`workspace.cryptolens`)

#### `bronze_klines`
Merge key: `(symbol, interval, load_type, open_time)` — insert-only on match.

| Column | Type | Description |
|---|---|---|
| `open_time` | `BIGINT` | Candle open, raw epoch (see *Known limitations* on ms vs µs) |
| `open` | `DOUBLE` | Open price |
| `high` | `DOUBLE` | High price |
| `low` | `DOUBLE` | Low price |
| `close` | `DOUBLE` | Close price |
| `volume` | `DOUBLE` | Base-asset volume |
| `close_time` | `BIGINT` | Candle close, raw epoch |
| `quote_asset_volume` | `DOUBLE` | Quote-asset (USDT) volume |
| `number_of_trades` | `BIGINT` | Trades in the candle |
| `taker_buy_base_asset_volume` | `DOUBLE` | Taker-buy base volume |
| `taker_buy_quote_asset_volume` | `DOUBLE` | Taker-buy quote volume |
| `ignore` | `STRING` | Unused Binance field |
| `symbol` | `STRING` | Trading pair, e.g. `BTCUSDT` (from filename) |
| `interval` | `STRING` | Candle interval, e.g. `1m` |
| `load_type` | `STRING` | `full` or `incremental` |
| `source_file` | `STRING` | Volume path of the source CSV |
| `ingestion_date` | `DATE` | Date the row was ingested |
| `load_timestamp` | `TIMESTAMP` | Ingestion timestamp |

#### `bronze_quarantine`
Rows that fail schema-on-read (PERMISSIVE mode). Same 12 raw columns as `bronze_klines` (as parsed), plus:

| Column | Type | Description |
|---|---|---|
| `_corrupt_record` | `STRING` | Original unparseable line |
| `source_file` | `STRING` | Volume path of the source CSV |
| `load_timestamp` | `TIMESTAMP` | Quarantine timestamp |

Written with `mergeSchema=true`, so columns may be added over time.

#### `bronze_exchange_info`
One row per `/exchangeInfo` snapshot, landed as raw JSON. Skipped if `source_file` already exists (idempotent).

| Column | Type | Description |
|---|---|---|
| `raw_json` | `STRING` | Entire snapshot file as one string |
| `source_file` | `STRING` | Volume path of the snapshot |
| `load_timestamp` | `TIMESTAMP` | Landing timestamp |

### 🥈 Silver (`workspace.cryptolens`)

#### `silver_klines`
Merge key: `(symbol, interval, open_time)`. Matched rows are updated only if a business value changed. `load_type` and the `ignore` column are dropped in Silver.

| Column | Type | Description |
|---|---|---|
| `symbol` | `STRING` | Trading pair |
| `interval` | `STRING` | Candle interval |
| `open_time` | `TIMESTAMP` | Candle open (raw value / 1000) |
| `close_time` | `TIMESTAMP` | Candle close (raw value / 1000) |
| `open` | `DECIMAL(18,8)` | Open price |
| `high` | `DECIMAL(18,8)` | High price |
| `low` | `DECIMAL(18,8)` | Low price |
| `close` | `DECIMAL(18,8)` | Close price |
| `volume` | `DECIMAL(28,8)` | Base-asset volume |
| `quote_asset_volume` | `DECIMAL(28,8)` | Quote-asset (USDT) volume |
| `number_of_trades` | `INT` | Trades in the candle |
| `taker_buy_base_vol` | `DECIMAL(28,8)` | Taker-buy base volume (renamed from Bronze) |
| `taker_buy_quote_vol` | `DECIMAL(28,8)` | Taker-buy quote volume (renamed from Bronze) |
| `load_timestamp` | `TIMESTAMP` | Silver load timestamp |

Quality filters: `high >= low`, `close` and `open` within `[low, high]`, de-duplicated on the merge key.

#### `dim_symbol` (Silver)
Three-branch `MERGE` on `symbol`: update on status/filter change, insert new listings, soft-delete (`is_active = false`, `status = 'DELISTED'`) for symbols missing from the latest snapshot.

| Column | Type | Description |
|---|---|---|
| `symbol` | `STRING` | Trading pair (merge key) |
| `base_asset` | `STRING` | Base asset, e.g. `BTC` |
| `quote_asset` | `STRING` | Quote asset, e.g. `USDT` |
| `status` | `STRING` | Binance trading status (`TRADING`, `BREAK`, `DELISTED`, …) |
| `is_spot_trading_allowed` | `BOOLEAN` | Spot trading flag from `exchangeInfo` |
| `load_timestamp` | `TIMESTAMP` | Last change timestamp |
| `is_active` | `BOOLEAN` | `false` once the symbol is soft-deleted |

### 🧾 Audit (`workspace.cryptolens.pipeline_execution_logs`)

Shared by all three layers. `layer` is `Raw-to-Bronze`, `Bronze-to-Silver`, or `Silver-to-Gold`.

| Column | Type | Nullable | Description |
|---|---|---|---|
| `run_id` | `STRING` | no | UUID per log row |
| `layer` | `STRING` | no | Pipeline stage |
| `parameter_processed` | `STRING` | no | File path, `symbol-interval-loadtype`, or `table [operation] source_rows=N` |
| `load_type` | `STRING` | no | `full`, `incremental`, or `snapshot` |
| `start_time` | `TIMESTAMP` | no | Step start |
| `end_time` | `TIMESTAMP` | no | Step end |
| `status` | `STRING` | no | `SUCCESS`, `FAILURE`, or `SKIPPED` |
| `rows_inserted` | `BIGINT` | yes | Rows inserted (from Delta commit metrics on `MERGE`) |
| `rows_updated` | `BIGINT` | yes | Rows updated |
| `error_message` | `STRING` | yes | Error text (truncated to 500 chars in Gold) |

### 🥇 Gold (`workspace.gold`)

`symbol_key = abs(xxhash64(symbol))` and `date_key` (`yyyyMMdd`) are identical in every table.

#### `dim_symbol` (Gold) — key: `symbol_key`
USDT pairs only (`quoteAsset = 'USDT'`).

| Column | Type | Description |
|---|---|---|
| `symbol_key` | `BIGINT` | Surrogate key |
| `symbol` | `STRING` | Trading pair |
| `base_asset` | `STRING` | Base asset |
| `quote_asset` | `STRING` | Quote asset (always `USDT`) |
| `status` | `STRING` | Current status; `DELISTED` if missing from latest snapshot |
| `is_spot_trading_allowed` | `BOOLEAN` | Spot trading flag |
| `previous_status` | `STRING` | Status before the last change (null if never changed) |
| `status_changed_date` | `DATE` | Date of the last status change |
| `first_seen_date` | `DATE` | First snapshot containing the pair |
| `last_seen_date` | `DATE` | Latest snapshot containing the pair |

#### `dim_date` — key: `date_key`

| Column | Type | Description |
|---|---|---|
| `date_key` | `INT` | `yyyyMMdd` |
| `full_date` | `DATE` | Calendar date |
| `year` | `INT` | Year |
| `quarter` | `INT` | Quarter (1–4) |
| `month` | `INT` | Month number |
| `month_name` | `STRING` | e.g. `August` |
| `day` | `INT` | Day of month |
| `day_of_week` | `INT` | 1 = Sunday … 7 = Saturday |
| `day_name` | `STRING` | e.g. `Monday` |
| `week_of_year` | `INT` | Week number |
| `is_weekend` | `BOOLEAN` | Saturday or Sunday |

#### `fact_daily_ohlcv` — key: `(symbol_key, date_key)`
Daily roll-up of 1-minute Silver candles.

| Column | Type | Description |
|---|---|---|
| `symbol_key` | `BIGINT` | FK → `dim_symbol` |
| `date_key` | `INT` | FK → `dim_date` |
| `symbol` | `STRING` | Trading pair |
| `trade_date` | `DATE` | Trading day |
| `open_price` | `DECIMAL(18,8)` | Open of the first candle of the day |
| `high_price` | `DECIMAL(18,8)` | Highest high |
| `low_price` | `DECIMAL(18,8)` | Lowest low |
| `close_price` | `DECIMAL(18,8)` | Close of the last candle of the day |
| `volume` | `DECIMAL(38,8)` | Sum of base-asset volume |
| `quote_volume` | `DECIMAL(38,8)` | Sum of USDT volume |
| `vwap` | `DECIMAL` | `quote_volume / volume` (`try_divide`, null when volume = 0) |
| `trade_count` | `BIGINT` | Sum of trades |
| `candle_count` | `BIGINT` | 1-minute candles in the day (max 1,440) |
| `_loaded_at` | `TIMESTAMP` | Gold load timestamp |

#### `volatility_daily` — key: `(symbol_key, date_key)`

| Column | Type | Description |
|---|---|---|
| `symbol_key` | `BIGINT` | FK → `dim_symbol` |
| `date_key` | `INT` | FK → `dim_date` |
| `symbol` | `STRING` | Trading pair |
| `trade_date` | `DATE` | Trading day |
| `close_price` | `DECIMAL(18,8)` | Daily close |
| `daily_return` | `DECIMAL` | `close / prev_close − 1` |
| `log_return` | `DOUBLE` | `ln(close / prev_close)` |
| `intraday_range_pct` | `DECIMAL` | `(high − low) / open` |
| `rolling_vol` | `DOUBLE` | Std. dev. of `log_return` over `VOL_WINDOW` (7) days |
| `rolling_vol_annualized` | `DOUBLE` | `rolling_vol × √365` |

#### `moving_averages` — key: `(symbol_key, date_key)`

| Column | Type | Description |
|---|---|---|
| `symbol_key` | `BIGINT` | FK → `dim_symbol` |
| `date_key` | `INT` | FK → `dim_date` |
| `symbol` | `STRING` | Trading pair |
| `trade_date` | `DATE` | Trading day |
| `close_price` | `DECIMAL(18,8)` | Daily close |
| `ma_short` | `DECIMAL(22,12)` | Short SMA (5-day), null until the window is full |
| `ma_long` | `DECIMAL(22,12)` | Long SMA (20-day), null until the window is full |
| `cross_signal` | `STRING` | `GOLDEN_CROSS`, `DEATH_CROSS`, or null |
| `trend` | `STRING` | `BULLISH` (short > long), `BEARISH` (short < long), or null |
| `ma_short_window` | `INT` | Short window length used |
| `ma_long_window` | `INT` | Long window length used |

#### `symbol_correlation` — key: `(symbol_a_key, symbol_b_key, date_key)`
Top 15 symbols by total quote volume; pairs are ordered so `symbol_a < symbol_b`.

| Column | Type | Description |
|---|---|---|
| `symbol_a_key` | `BIGINT` | FK → `dim_symbol` |
| `symbol_b_key` | `BIGINT` | FK → `dim_symbol` |
| `date_key` | `INT` | FK → `dim_date` |
| `symbol_a` | `STRING` | First symbol |
| `symbol_b` | `STRING` | Second symbol |
| `trade_date` | `DATE` | Trading day |
| `rolling_corr` | `DOUBLE` | Rolling 14-day correlation of daily log returns (−1 to 1) |

> **Note:** `dim_symbol` exists in both schemas — `workspace.cryptolens.dim_symbol` (Silver, keyed on `symbol`, soft-delete via `is_active`) and `workspace.gold.dim_symbol` (star-schema dimension, keyed on `symbol_key`). Power BI uses the Gold one.

---

## ⚙️ Engineering Practices (Phase 2)

- **Strict schema-on-read** — every CSV/JSON read uses an explicit `StructType`; `inferSchema` is never used.
- **Idempotency** — every write path uses Delta `MERGE INTO`; re-running either notebook with identical parameters produces zero duplicate rows (verified by re-running twice and comparing row counts).
- **Parameterized backfills** — Bronze exposes `interval`/`load_type`/`year`/`month`/`day` as Databricks widgets and Silver exposes `interval`/`load_type`; no function hardcodes "today," so any historical batch can be reprocessed on demand. Symbols are auto-discovered (from files in the Volume for Bronze, from `bronze_klines` for Silver) rather than typed in, so there is no manual list to drift out of sync.
- **Schema drift handling** — PERMISSIVE CSV mode routes unparseable rows to `bronze_quarantine` rather than failing the batch; Delta writes use `mergeSchema=true` to tolerate newly-added columns.
- **Serverless-compatible code** — no RDD APIs (including `DataFrame.isEmpty()`, which wraps `.rdd` internally); emptiness checks use `limit(1).count()`.
- **Audit logging** — every file/batch processed, across both Full and Incremental loads and both layers, writes a row to `pipeline_execution_logs`.

**Phase 3 additions**
- **Pipeline integration** — a Databricks Job, `cryptolens_daily`, runs `01.2_bronze_ingest` → `02_silver_transform` → `03_gold_transform` as three dependent serverless tasks, so Gold refreshes automatically after Silver (a repeat run leaves every Gold row count unchanged: idempotent `MERGE`).
- **Safe arithmetic** — ratios in Gold (`vwap`, returns, range %) use `try_divide`, because zero-volume days otherwise raise `DIVIDE_BY_ZERO` under ANSI mode.
- **Timestamp-unit drift handled in Gold** — see *Known limitations*.
- **Parameterised Job** — Job parameters map to the existing widgets (`load_type`, `interval`, `year`, `month`, `day`), so no notebook is edited by hand to run a different batch.

Full writeup: **[`docs/Phase2_CryptoLens.pdf`](docs/Phase2_CryptoLens.pdf)** (includes a log of issues hit on Serverless and how each was resolved).

---

## ⚠️ Known Limitations

- **Timestamp unit drift (found in Phase 3).** The raw schema assumes `open_time` / `close_time` are epoch **milliseconds**, but Binance's 2026 files use **microseconds**. The values still parse as `LongType`, so nothing is quarantined, but Silver's `/ 1000` conversion yields dates around the year 58,000. Gold corrects this when it reads Silver (divides by 1000 again for any timestamp beyond year 2200); the proper fix is to detect the unit in Silver by the raw value's magnitude. Until then, **query Gold, not `silver_klines`, for dates**.
- **Gap between loads.** The data covers 1–31 August 2026 plus 25 September 2026, so Gold's row-based windows treat 25 September as the day after 31 August. Moving averages and correlations on that date are computed across the gap.
- **Short history.** Moving-average windows are 5 / 20 days (not 50 / 200) and the correlation window is 14 days, because only about a month is loaded. They are configurable in the notebook's config cell.
- **One `exchangeInfo` snapshot.** `dim_symbol` status-change tracking is built, but with a single snapshot every `status_changed_date` equals the snapshot date.
- **Per-symbol loop.** Bronze and Silver process one symbol at a time (see FinOps).

---

## 🔐 Security & Compliance

**No PII is present in this dataset.** Every field — OHLC, volume, trade count, and symbol status — is an aggregated, anonymous market statistic with no usernames, account IDs, wallet addresses, or IP addresses. Binance publishes both the historical klines and `exchangeInfo` as fully public, aggregated market data. Standard data-quality and governance controls (schema enforcement, immutable Bronze audit trail, workspace-level access control) are applied through the Medallion layers.

---

## 📊 Dashboard (Power BI)

The report (`dashboards/CryptoLens_Dashboard.pbix`) connects to the `workspace.gold` schema (plus `pipeline_execution_logs`) through the Power BI **Databricks connector** and a Serverless SQL warehouse, in **Import** mode. The model mirrors the star schema: every fact table is many-to-one to `dim_symbol` and `dim_date`; the audit table stands alone.

| Page | Visuals | Business question answered |
|---|---|---|
| **Price & Volatility** | Coin slicer; close-price line; annualised-volatility line; KPI cards (total trades, USDT volume) | How have price and volatility trended per asset? |
| **Signals & Status** | Price with short / long moving averages; golden / death crosses per day; current bullish vs bearish split (all symbols); Binance USDT pairs by trading status | Which assets are bullish or bearish? Which pairs have which trading status? |
| **Correlation** | Rolling-correlation heatmap (top 15 symbols by volume, colour scale −1 to 1); average correlation over time | How correlated are the assets, and is it rising or falling? |
| **Pipeline Audit** | Log-entry and failure counts; rows inserted / updated by layer; average step duration by layer; activity over time; error log | Is the pipeline healthy, and what did each layer do? |

**Reading the charts:** the loaded data covers August 2026 (full load) plus 25 September 2026 (incremental load), so trend lines are drawn only over August, and values on 25 September follow a gap in the data (see *Known limitations*).

---

## 💰 FinOps & Cost Awareness

- **Platform:** Databricks Free Edition — serverless compute, no idle cluster cost.
- **Download cost:** the heavy top-50 symbol download runs locally (not on billed Databricks compute); only the already-verified CSVs are uploaded into the Unity Catalog Volume.
- Scope is fixed at the top 50 USDT pairs × 1-minute interval for the semester, sized to comfortably clear reviewer-set thresholds (~200 MB full load, ≥1 MB daily incremental) without ballooning further.
- `OPTIMIZE` / `Z-ORDER` used sparingly, only on Gold tables (Phase 3).
- Incremental job scheduled once daily (`cryptolens_daily`, 02:00 UTC trigger configured), matching Binance's own publication cadence — no unnecessary cluster spin-ups.
- **Measured run time** (one daily incremental batch, 50 symbols × 1 day, serverless, Free Edition): Bronze ≈ 5 min 52 s, Silver ≈ 7 min 40 s, Gold ≈ 1 min 8 s — **≈ 14 min 41 s end to end**. Bronze and Silver dominate because they loop over symbols with one `MERGE` per symbol; Gold works on whole tables.
- **Main optimisation identified:** batch all symbols into a single read and one `MERGE` per layer, which would cut the bulk of the run time (and compute cost on a paid platform) roughly in proportion. Derived Gold tables currently rebuild from the fact table each run, which is fine at this scale but would need to become incremental for an hourly enterprise run.
- Free Edition has no per-run billing, so cost projections (daily vs hourly, per year) are estimates, set out in the final report.
- Fallback: Azure for Students credits if more headroom is needed for Phase 3 dashboard testing.

---

## 🗓️ Roadmap

- [x] **Phase 1** — Proposal, real checksum-verified sample data, architecture design, network-workaround documented *(26 Sep 2026)*
- [x] **Phase 2** — Bronze & Silver notebooks implemented: explicit schemas, idempotent MERGE, parameterized backfills, schema-drift quarantine, audit logging *(completed; verified via `00_layers_verification.ipynb`)*
- [x] **Phase 3** — Gold star schema, Job-based automation, Power BI dashboard, final report *(due 24 Oct 2026)*

---

## 👥 Team

| Name | Role |
|---|---|
| Abdullah Saeed (24L-2529) | Pipeline Engineering (Bronze layer) |
| Arqum Umais (24L-2591) | Data Modeling & BI (Silver layer, dashboarding) |

---

## 📄 License

This project is for academic purposes as part of a "Data Analysis and Visualization" semester course. Source data © [Binance](https://www.binance.com/), published via [Binance Vision](https://data.binance.vision) for public research and analytical use.