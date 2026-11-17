# CryptoLens — Corrected Databricks notebook deployment and one-time repair

## Scope (important)

This bundle replaces **only** the following files in `notebooks/`:

- `01.2_bronze_ingest.ipynb` — Databricks Raw/Volume → Bronze
- `02_silver_transform.ipynb` — Databricks Bronze → Silver
- `03_gold_transform.ipynb` — Databricks Silver → Gold
- `00_layers_verification.ipynb` — Databricks validation and optional idempotency demonstration

**Not modified:** `01.1_bronze_ingest_data extractor.ipynb` (runs **locally**). The local script still downloads Binance files, verifies ZIP checksums and uploads CSV/JSON into the Unity Catalog Volume. No local extraction logic is part of these replacement notebooks.

The existing database and table names are preserved:
`workspace.cryptolens.bronze_klines`, `bronze_exchange_info`, `silver_klines`, `dim_symbol`, `pipeline_execution_logs` and the six `workspace.gold` tables. The existing Power BI tables/relationships should remain compatible, but data needs to be refreshed after the rebuild.

## A. Before running anything: verify the actual Volume upload folder

The *unchanged* extractor's example commands upload under:

`/Volumes/workspace/cryptolens/bronze_raw/l242529@lhr.nu.edu.pk`

The **old Databricks Bronze/Gold code** was configured for:

`/Volumes/workspace/cryptolens/bronze_raw/arqumumais@gmail.com`

The new Bronze notebook keeps the **old Databricks default** so an existing workspace using that folder is not silently switched. Its `volume_root` widget is configurable. **Set it to the actual subfolder where the CSV files were uploaded.** You should see:

```text
<volume_root>/full_load/*.csv
<volume_root>/incremental_load/*.csv
<volume_root>/exchange_info/exchangeInfo-YYYY-MM-DD.json
```

You do **not** need to edit the extractor. Set the Databricks `volume_root` widget or keep uploading files to the root your running Databricks notebooks use.

## B. Back up the existing Delta tables

Before repair, keep a rollback point. Delta `RESTORE` can use a prior table version while the Delta log is retained, or use `CREATE TABLE ... DEEP CLONE ...` if your workspace permits it. At minimum, record the original table versions:

```sql
DESCRIBE HISTORY workspace.cryptolens.silver_klines;
DESCRIBE HISTORY workspace.gold.fact_daily_ohlcv;
DESCRIBE HISTORY workspace.gold.volatility_daily;
DESCRIBE HISTORY workspace.gold.moving_averages;
DESCRIBE HISTORY workspace.gold.symbol_correlation;
```

Do not delete your Bronze source tables or execution logs. The following repair applies **only to bad Silver timestamps**.

## C. One-time repair of already-loaded Silver timestamps

**Reason:** earlier code mistakenly treated microsecond epoch values as milliseconds, so old Silver records had invalid dates. Because `open_time` is the MERGE key, re-running with the corrected conversion alone would add corrected rows but leave the bad old keys behind.

Verify how many invalid rows exist:

```sql
SELECT COUNT(*) AS invalid_timestamp_rows
FROM workspace.cryptolens.silver_klines
WHERE open_time < TIMESTAMP '2010-01-01 00:00:00'
   OR open_time >= TIMESTAMP '2100-01-01 00:00:00';
```

After backing up your table, delete **only** these invalid rows:

```sql
DELETE FROM workspace.cryptolens.silver_klines
WHERE open_time < TIMESTAMP '2010-01-01 00:00:00'
   OR open_time >= TIMESTAMP '2100-01-01 00:00:00';
```

If this table has never contained incorrect timestamps, the DELETE affects 0 rows and a cleanup is unnecessary. Do not delete otherwise valid historical Silver data.

## D. Rerun Silver using the existing Bronze Delta rows

The raw files should already have been loaded into Bronze. **No re-download or re-upload is required** for the timestamp repair if Bronze contains the originally uploaded full and incremental data.

1. Import/replace the provided `02_silver_transform.ipynb` in Databricks.
2. Run with: `interval=1m`, `load_type=full`, `year=2026`, `month=08`, `day=` (blank). This reprocesses the August monthly Bronze files into corrected Silver.
3. Run again with: `interval=1m`, `load_type=incremental`, `year=2026`, `month=09`, `day=25`. This processes the September 25 daily data.
4. If you have more dates in Bronze, process each desired full/incremental period. Or clear `year/month/day` to process every Bronze row for the selected `load_type`.
5. This Silver notebook also refreshes the Silver `dim_symbol` from the latest **landed Bronze** exchangeInfo snapshot.

Check:

```sql
SELECT MIN(open_time) AS first_open, MAX(open_time) AS last_open, COUNT(*) AS rows
FROM workspace.cryptolens.silver_klines;
```

The dates should be in 2026, **not year 58,000**. The first reprocessing after cleanup inserts the corrected keys; a second identical re-run should have zero Silver matched updates and no duplicate keys.

## E. Full rebuild of Gold from corrected Silver

1. Replace `03_gold_transform.ipynb` in Databricks.
2. Run it once with `full_refresh=true` and empty `backfill_start` / `backfill_end`.
3. This recreates the daily fact table from all Silver and then rebuilds the date, volatility, moving-average and correlation outputs. **This overwrites the existing Gold fact and derived metrics**, so do this only after the backups and Silver checks above.
4. Restore `full_refresh=false` for normal incremental runs.

The Gold dim_symbol dimension is maintained with MERGE and is not dropped. Old historical rows may have a misleading initial `status_changed_date` from the earlier code; **after checking your snapshot history**, this one-time normalization is appropriate if they are all from the single initial snapshot:

```sql
UPDATE workspace.gold.dim_symbol
SET status_changed_date = NULL
WHERE previous_status IS NULL
  AND status_changed_date = first_seen_date;
```

The corrected Gold notebook sets `status_changed_date` to NULL on an initial insert and populates it only when a status actually changes.

## F. How to run the existing Databricks Job correctly

Your workflow remains:

1. **Local computer** — unchanged extractor downloads/validates the requested Binance data.
2. **Local computer** — uploads the files into an existing Unity Catalog Volume through the Databricks CLI.
3. **Databricks** — run the `bronze → silver → gold` Job after the upload.
4. **Power BI Desktop** — refresh the imported Gold and audit tables when you want the dashboard to show the new data.

The three Databricks tasks should depend on one another in that order. Recommended widgets for a September 25 batch:

| Job task | Notebook | Parameters |
|---|---|---|
| bronze | `01.2_bronze_ingest` | `volume_root=<actual uploaded Volume folder>`, `load_type=incremental`, `interval=1m`, `year=2026`, `month=09`, `day=25` |
| silver | `02_silver_transform` | `load_type=incremental`, `interval=1m`, `year=2026`, `month=09`, `day=25` |
| gold | `03_gold_transform` | `full_refresh=false`, optional `lookback_days=2`, `run_optimize=true` |

For **full August** use `load_type=full`, `month=08`, `day=` blank for Bronze and Silver. The local extractor remains independent: **the Databricks Job does not automatically download or upload new Binance files.** If the input files were not uploaded, Bronze fails clearly rather than making Gold appear successfully refreshed from stale data.

A historic Gold correction can also be run with `backfill_start=YYYY-MM-DD`, `backfill_end=YYYY-MM-DD` after processing the relevant Bronze/Silver dates. For a comprehensive rebuild or if data has been removed, use `full_refresh=true`.

### Calendar-gap behavior

Your current known loaded dates are August 1–31 and September 25, 2026. A rolling window should **not** treat September 25 as adjacent to August 31. The corrected Gold sets unavailable returns/moving averages/volatility to NULL at that gap. September 25 may have fewer trend/correlation indicators than the older Power BI screenshots; **this is expected** until sufficient consecutive missing dates are loaded. Refresh Power BI and explain the limitation honestly.

## G. Verify in Databricks before presenting

Run `00_layers_verification.ipynb` after the rebuild. It checks timestamps, duplicate keys, fact-dimension matches, rolling indicator gaps, logs and quarantine.

Optional rigorous idempotency demonstration:

- Set `silver_notebook_path` to the full Databricks workspace path of your uploaded `02_silver_transform` notebook.
- Set `run_silver_rerun_proof=true` and choose a symbol/day with data.
- It captures that symbol/day's business values **and load_timestamp**, actually executes the Silver notebook again, and compares the results. This is stronger than simply printing equal before/after counts with no rerun.

To prove **schema drift** to the TA, do a controlled test in an isolated, copied batch with an extra CSV column or malformed numeric value, then show `bronze_quarantine`. Do not deliberately corrupt your real source files. To prove **new listing/status change**, use two legitimate exchangeInfo snapshots; the original project report had only one, so a real historical status change has not yet been demonstrated.

## H. Changes not in this bundle

- The local extractor notebook is **untouched** and is not included.
- The `.pbix` is **untouched**; refresh it after data changes.
- The existing Phase 1 proposal promised hourly and year-over-year metrics; these additions are **not** implemented here because the request was to correct existing layers while retaining the existing table schema and Power BI compatibility. An additional year of historical data would be needed for a meaningful year-over-year measure.
- No GitHub branch or file was modified remotely; these are replacement files to import/commit after review.

## I. Run-status limitation

These notebooks were assembled and structurally validated as `.ipynb` files, and their Python source passes local syntax validation. They **were not executed against your Databricks workspace**, so confirm runtime SQL/API compatibility and results by following the validation above before assessment.
