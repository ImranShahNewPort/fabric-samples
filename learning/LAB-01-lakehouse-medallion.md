# Lab 01: Lakehouse Medallion in Miniature

**Goal:** Run Microsoft's Wide World Importers (WWI) Lakehouse tutorial end to end, and understand each cell well enough to rebuild the pattern on NTFB-style data.

**Notebooks used** (in `docs-samples/data-engineering/Lakehouse Tutorial Source Code/`)
- `01 - Create Delta Tables.ipynb`: raw parquet → Delta tables (bronze → silver)
- `02 - Data Transformation - Business Aggregates.ipynb`: Delta tables → reporting aggregates (silver → gold)

**Time:** about 60–90 minutes

---

## 0. One-time setup (≈15 min)

| Step | Action |
|---|---|
| 1 | Go to **app.fabric.microsoft.com** with a personal Microsoft account. Start the free **Fabric trial** (profile icon → *Start trial*). |
| 2 | Create a workspace named `fabric-learning` and assign it to the trial capacity. |
| 3 | In the workspace, select **New item → Lakehouse** and name it **`wwilakehouse`**. Notebook 02 hard-codes this name. |
| 4 | Keep *Lakehouse schemas* **off** for this lab. The notebooks write to plain `Tables/<name>` paths. |

## 1. Land the raw data, which is the "source system" (≈10 min)

Both notebooks assume `Files/wwi-raw-data/` already exists. Create it with a Copy pipeline.

1. Select **New item → Pipeline** and name it `IngestDataFromSourceToLakehouse`.
2. Select **Pipeline activity → Copy data**.
3. On the **Source** tab, open **Connection → Browse all → Azure blobs**:
   - URL: `https://fabrictutorialdata.blob.core.windows.net/sampledata/`
   - Connection name: `wwisampledata` · Authentication: **Anonymous**
   - Container: `sampledata` · Directory: `WideWorldImportersDW/parquet`
   - Recursively: ✅ · File format: **Binary**
4. On the **Destination** tab, set:
   - Connection: `wwilakehouse` · Root folder: **Files**
   - Directory: `wwi-raw-data` · File format: **Binary**
5. Select **Save → Run**. Wait for **Succeeded**, then refresh **Files** in the lakehouse and confirm `wwi-raw-data/full/...` exists.

> **Medallion lens:** `Files/wwi-raw-data` is your **bronze** landing zone. The files are untouched and in their source format.

## 2. Import the notebooks (≈5 min)

1. Download the two `.ipynb` files from your fork on GitHub (open the file → **Download raw file**).
2. In the workspace, select **Import → Notebook → From this computer** and upload both.
3. Open each notebook. In the left **Explorer** pane, select **Add data items → Existing lakehouse → `wwilakehouse`** and make it the **default** lakehouse. Relative paths like `Files/` and `Tables/` resolve against the default lakehouse.

---

## 3. Notebook 01: Create Delta Tables

### Cell 1: Spark session config
```python
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")
```
- **V-Order** is Fabric-specific parquet sorting and compression. It makes Power BI **Direct Lake** and SQL endpoint reads much faster.
- **Optimize Write** packs output into fewer, larger files (target about 1 GB) instead of thousands of small ones.
- *Takeaway:* set these in every write notebook. At the COE level, put them in an **Environment** so nobody has to remember them.

The first cell takes about 1–3 minutes because the Spark session starts up. Later cells run fast.

### Cell 2: `fact_sale`
- Reads raw parquet, derives `Year`, `Quarter`, and `Month` from `InvoiceDateKey`, and writes a Delta table **partitioned by Year/Quarter**.
- `mode("overwrite")` makes this a full reload. It's idempotent, so you can rerun it safely.

**Check your work** with a new cell:
```python
df = spark.read.table("fact_sale")
print(df.count())
display(df.groupBy("Year","Quarter").count().orderBy("Year","Quarter"))
```

### Cell 3: Dimensions loop
- One reusable function, `loadFullDataFromSource`, reads, drops the `Photo` blob column, and writes Delta.
- It loops over 5 dimensions: city, customer, date, employee, and stock_item.
- *Takeaway:* this is the seed of a **metadata-driven loader**. Replace the hard-coded list with a control table and you have a framework. See `community-samples/Metadata-driven Ingestion Framework/`.

**Check:** refresh **Tables** in the lakehouse. You should see 6 tables. Click one to preview it.

> **Medallion lens:** these Delta tables are **silver**. They're typed, cleaned (the unused column is dropped), and queryable by SQL, Spark, and Power BI.

---

## 4. Notebook 02: Business Aggregates

### Cell 1: Same Spark config as notebook 01.

### Cells 2–3: Approach #1 (PySpark DataFrame API)
- Loads `fact_sale`, `dimension_date`, and `dimension_city` as DataFrames.
- Joins fact to dimensions on their keys, groups by date and city, sums the four money columns, renames `sum(...)` columns to friendly names, and writes `aggregate_sale_by_date_city`.
- `overwriteSchema=true` lets you change the shape of a gold table without dropping it first.

### Cells 4–5: Approach #2 (Spark SQL)
- `%%sql` builds a temp view joining fact, date, and employee with a `GROUP BY`.
- The next cell reads the view with `spark.sql(...)` and writes `aggregate_sale_by_date_employee`.
- *Takeaway:* both approaches produce the same kind of output. Choose by team skill. SQL-first analysts (most BI teams) can work in `%%sql`, and engineers can use PySpark. Both compile to the same Spark plan.

> **Medallion lens:** the two `aggregate_*` tables are **gold**. They're pre-joined, business-named, and sized for reporting.

---

## 5. See it end to end (≈10 min)

1. Open the lakehouse and switch to the **SQL analytics endpoint** in the top-right toggle. Run:
   ```sql
   SELECT TOP 10 City, SUM(SumOfProfit) AS Profit
   FROM aggregate_sale_by_date_city
   GROUP BY City ORDER BY Profit DESC;
   ```
2. Select **New semantic model**, pick the two gold tables, and create a quick Power BI report. It runs in **Direct Lake** mode with no import refresh.

---

## 6. Stretch exercises: make it NTFB-shaped

| # | Exercise | What it teaches |
|---|---|---|
| A | Change notebook 01 to **append** with a `LoadDate` column instead of overwriting. | Incremental loads and audit columns |
| B | Replace the overwrite in cell 2 with a Delta **MERGE** keyed on `SaleKey`. | Upserts, which is how a D365 → lakehouse sync would behave |
| C | Upload a small CSV of your own (for example, mock agency or partner distributions) to `Files/ntfb-raw/` and build bronze → silver → gold for it. | Applying the pattern to your domain |
| D | Move the `full_tables` list into a Delta control table and loop over it. | Metadata-driven ingestion |
| E | Run `OPTIMIZE fact_sale` and `DESCRIBE HISTORY fact_sale` in `%%sql`. | Delta maintenance and time travel |
| F | Create a second workspace `fabric-learning-prod` and try **Deployment pipelines**. | Dev → Prod promotion for the COE |

## 7. Clean up
Trial capacity costs nothing and can't be paused. When you finish the series, delete the `fabric-learning` workspace to remove everything.

---

**Next lab:** `docs-samples/data-engineering/Notebook/GreenTripData/DataEngineeringStarterKit.ipynb`, a 52-cell deeper PySpark tour.
