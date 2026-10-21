# Lab 01: Lakehouse Medallion on the LAB Sandbox

**Goal:** Run Microsoft's Wide World Importers (WWI) Lakehouse tutorial through the LAB sandbox's own medallion layers, `lh_bronze` → `lh_silver` → `wh_gold`, using the service principal for the setup steps.

**Built on (already set up):**
- Sandbox tenant with Fabric trial capacity
- Workspaces `LAB-Platform-Gold-DEV` / `LAB-Platform-Gold-PROD` and deployment pipeline `LAB Gold Pipeline`
- DEV items: `lh_bronze`, `lh_silver`, `wh_gold`, `nb_bronze_to_silver`, `pl_daily_load`
- Service principal `fabric-lab-automation` (group `FAB-Automation`), Admin on both LAB workspaces

**Files in this lab** (`learning/lab01/`)

| File | Layer | Adapted from |
|---|---|---|
| `WWI-01-bronze-to-silver.ipynb` | Bronze → Silver (Spark) | `docs-samples/.../01 - Create Delta Tables.ipynb` |
| `WWI-02-silver-to-gold.sql` | Silver → Gold (Warehouse T-SQL) | `docs-samples/.../02 - Data Transformation - Business Aggregates.ipynb` |
| `Import-WwiNotebook.ps1` | Setup | Imports the notebook through the API as the service principal |

**What changed vs. Microsoft's version**
- Raw files land in **`lh_bronze`**, not a single `wwilakehouse`.
- Delta tables are written to **`lh_silver`** with `saveAsTable("lh_silver.<table>")`. Audit columns `SourceSystem` and `LoadedAt` are added, matching the NTFB Gold standard's lineage columns.
- Gold is built in **`wh_gold`** with T-SQL CTAS over `lh_silver`, instead of Spark writing more lakehouse tables.

**Time:** about 60 minutes

---

## Step 1. Land raw data in bronze with `pl_daily_load` (≈10 min, portal)

1. Open `LAB-Platform-Gold-DEV` → `pl_daily_load`.
2. Select **Pipeline activity → Copy data** and name the activity `Copy WWI to Bronze`.
3. On the **Source** tab, open **Connection → Browse all → Azure blobs**:
   - URL: `https://fabrictutorialdata.blob.core.windows.net/sampledata/`
   - Connection name: `wwisampledata` · Authentication: **Anonymous**
   - Container: `sampledata` · Directory: `WideWorldImportersDW/parquet`
   - Recursively: ✅ · File format: **Binary**
4. On the **Destination** tab, set:
   - Connection: **`lh_bronze`** · Root folder: **Files**
   - Directory: `wwi-raw-data` · File format: **Binary**
5. Select **Save → Run** and wait for **Succeeded**.
6. Open `lh_bronze`, refresh **Files**, and confirm that `wwi-raw-data/full/` holds folders such as `fact_sale_1y_full` and `dimension_city`.

> **Medallion lens:** bronze holds the files as received, in their source format and untouched.

## Step 2. Import the notebook as the service principal (≈5 min, PowerShell)

From your Downloads folder, or wherever you saved `learning/lab01/` from the fork:

```powershell
cd $env:USERPROFILE\Downloads\lab01
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force
.\Import-WwiNotebook.ps1 -TenantId '<sandbox tenant id>' -AppId '<app client id>' -Secret '<client secret value>'
```

You should see the workspace and both lakehouse IDs, then `created nb_wwi_bronze_to_silver`. Re-running it updates the notebook in place.

*Manual fallback:* go to **Import → Notebook → From this computer**, choose the `.ipynb`, then in the Explorer pane add `lh_bronze` (set as default) and `lh_silver`.

## Step 3. Run bronze → silver (≈10 min, portal)

Open `nb_wwi_bronze_to_silver`. In the Explorer pane, check that **`lh_bronze` is the default** (pinned) and that `lh_silver` is listed. Then run the cells in order.

| Cell | What it does | Why it matters |
|---|---|---|
| 1 · Spark config | Turns on V-Order and Optimize Write | Fast Direct Lake and SQL reads, and fewer small files. At NTFB, set these once in a Fabric **Environment**. |
| 2 · Fact Sale | Reads `Files/wwi-raw-data/full/fact_sale_1y_full`, adds Year/Quarter/Month and audit columns, and writes `lh_silver.fact_sale` partitioned by Year/Quarter | A relative path reads from the **default** lakehouse, and a two-part name writes to the **other** one. That's the core cross-lakehouse pattern. |
| 3 · Dimensions | One function loops over 5 dimension folders, drops `Photo`, and writes to silver | This is the seed of a metadata-driven loader. |
| 4 · Checks | Row counts by Year/Quarter, plus `DESCRIBE HISTORY` | Delta keeps a version log, which gives you time travel and auditing. |

The first cell takes 1–3 minutes while Spark starts.

**Verify:** open `lh_silver` → **Tables**. There should be 6 tables.

> **Medallion lens:** silver means typed Delta tables with lineage columns that SQL, Spark and Power BI can all query.

## Step 4. Build gold in the Warehouse (≈10 min, portal)

1. Open `wh_gold`. In the Explorer pane, select **+ Warehouses**, tick **`lh_silver`** and confirm. This lets `wh_gold` query it by name.
2. Select **New SQL query**, paste all of `WWI-02-silver-to-gold.sql`, and run it.
3. The final `SELECT` returns the top 10 cities by profit.

| Statement | What it does |
|---|---|
| `DROP TABLE IF EXISTS` + `CREATE TABLE … AS SELECT` | A full rebuild of each gold table, so it's safe to rerun |
| `lh_silver.dbo.fact_sale` | A cross-database query from the Warehouse into the Lakehouse SQL endpoint, with no data copy |
| Two aggregates | `aggregate_sale_by_date_city` and `aggregate_sale_by_date_employee`, the same outputs as Microsoft's notebook 02 |

> **Medallion lens:** gold means pre-joined, business-named tables served from the Warehouse, the same pattern as your `fact_monthly_lbs`.

## Step 5. Report and promote (≈10 min, portal)

1. In `wh_gold`, select **New semantic model** and pick both `aggregate_*` tables. This creates a **Direct Lake** model.
2. Build a quick report showing Profit by SalesTerritory and month.
3. Open **LAB Gold Pipeline** and select **Deploy** DEV → PROD. Note which items deploy and which need their data rebuilt in PROD. That gap is the lesson.

---

## Stretch exercises (NTFB-shaped)

| # | Exercise | Teaches |
|---|---|---|
| A | In cell 2, swap overwrite for a Delta **MERGE** on `SaleKey` | Upserts, which is how a D365 → silver sync behaves |
| B | Add a step to `pl_daily_load` after the copy that runs `nb_wwi_bronze_to_silver`, then a **Script** activity that runs the gold SQL against `wh_gold` | End-to-end orchestration |
| C | Rebuild gold as a proper star with `lab01/WWI-03-gold-star-schema.sql`: `dim.Date`, `dim.City`, `dim.Customer`, `dim.Employee`, `dim.StockItem` (SCD2, unknown member -1), plus `fact.Sale` | NTFB Gold naming standard (PascalCase, `dim.`/`fact.` schemas, `<Entity>Key`/`<Entity>Id`, lineage) |
| D | Move the dimension list into a `lh_silver.etl_control` table and loop over it | Metadata-driven ingestion (see `community-samples/Metadata-driven Ingestion Framework/`) |
| E | Extend `Import-WwiNotebook.ps1` to also run the notebook through the Jobs API (`POST …/items/{id}/jobs/instances?jobType=RunNotebook`) | Fully programmatic runs as the service principal |

**Next lab:** `docs-samples/data-engineering/Notebook/GreenTripData/DataEngineeringStarterKit.ipynb`
