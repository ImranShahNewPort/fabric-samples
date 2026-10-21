# =====================================================================
# Lab 01 · Stretch D: Metadata-driven bronze -> silver loader
# Paste into nb_wwi_bronze_to_silver (lh_bronze default, lh_silver attached).
# Cell 1 seeds the control table once. Cell 2 is the loader the pipeline runs.
# =====================================================================

# ---------------- CELL 1 · Seed control table (run once) ----------------
from pyspark.sql import Row

control = [
    # source_folder, target_table, load_type, merge_keys, partition_cols, date_col, drop_cols, is_active
    Row("fact_sale_1y_full",    "fact_sale",            "merge", "SaleKey",      "Year,Quarter", "InvoiceDateKey", "",      True),
    Row("dimension_city",       "dimension_city",       "full",  "CityKey",      "",             "",               "",      True),
    Row("dimension_customer",   "dimension_customer",   "full",  "CustomerKey",  "",             "",               "",      True),
    Row("dimension_date",       "dimension_date",       "full",  "Date",         "",             "",               "",      True),
    Row("dimension_employee",   "dimension_employee",   "full",  "EmployeeKey",  "",             "",               "Photo", True),
    Row("dimension_stock_item", "dimension_stock_item", "full",  "StockItemKey", "",             "",               "Photo", True),
]
cols = ["source_folder", "target_table", "load_type", "merge_keys", "partition_cols", "date_col", "drop_cols", "is_active"]
spark.createDataFrame(control, cols).write.mode("overwrite").format("delta").saveAsTable("lh_silver.etl_control")
display(spark.read.table("lh_silver.etl_control"))


# ---------------- CELL 2 · Loader (pipeline runs this) ----------------
from delta.tables import DeltaTable
from pyspark.sql.functions import col, year, month, quarter, current_timestamp, lit
from datetime import datetime, timezone

spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")

RAW = "Files/wwi-raw-data/full/"
SILVER = "lh_silver"
run_at = datetime.now(timezone.utc)
log = []

def split(s):
    return [x.strip() for x in (s or "").split(",") if x.strip()]

for r in spark.read.table(f"{SILVER}.etl_control").filter("is_active").collect():
    tgt = f"{SILVER}.{r.target_table}"
    try:
        df = spark.read.parquet(RAW + r.source_folder)
        df = df.drop(*[c for c in split(r.drop_cols) if c in df.columns])
        if r.date_col:
            df = (df.withColumn("Year", year(col(r.date_col)))
                    .withColumn("Quarter", quarter(col(r.date_col)))
                    .withColumn("Month", month(col(r.date_col))))
        df = df.withColumn("SourceSystem", lit("WWI")).withColumn("LoadedAt", current_timestamp())
        rows = df.count()

        if r.load_type == "merge" and spark.catalog.tableExists(tgt):
            cond = " AND ".join(f"t.{k} = s.{k}" for k in split(r.merge_keys))
            (DeltaTable.forName(spark, tgt).alias("t")
                .merge(df.alias("s"), cond)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
            action = "merge"
        else:
            w = df.write.mode("overwrite").format("delta").option("overwriteSchema", "true")
            if split(r.partition_cols):
                w = w.partitionBy(*split(r.partition_cols))
            w.saveAsTable(tgt)
            action = "overwrite"

        log.append((run_at, r.target_table, action, rows, "OK", None))
        print(f"OK    {r.target_table:<22} {action:<9} {rows:>10,} rows")
    except Exception as e:
        log.append((run_at, r.target_table, r.load_type, None, "FAILED", str(e)[:500]))
        print(f"FAIL  {r.target_table:<22} {str(e)[:120]}")

(spark.createDataFrame(log, "RunAt timestamp, TargetTable string, Action string, SourceRows long, Status string, Error string")
      .write.mode("append").format("delta").saveAsTable(f"{SILVER}.etl_run_log"))

failed = [x for x in log if x[4] == "FAILED"]
if failed:
    raise Exception(f"{len(failed)} table(s) failed: {[x[1] for x in failed]}")   # makes the pipeline activity fail


# ---------------- CELL 3 · Check ----------------
display(spark.sql("SELECT * FROM lh_silver.etl_run_log ORDER BY RunAt DESC, TargetTable LIMIT 20"))
