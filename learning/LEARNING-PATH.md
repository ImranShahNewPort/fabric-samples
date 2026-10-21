# Fabric Samples: Learning Path

This path follows Microsoft's samples in this repo **as written**. They're ordered so each one builds on the last, and each is checked against the LAB sandbox. Every module lists what it teaches, what it needs, the fixes it requires (several samples are out of date), and the run steps.

**Legend**
- ✅ Runs on the sandbox as-is (or with the listed fix)
- 🟡 Runs on the sandbox, with some setup
- 💰 Needs **paid** capacity with Copilot/AI tenant settings. The trial doesn't include these. Park until NTFB has F-capacity.
- ☁️ Needs outside Azure services (Databricks, Snowflake, Azure OpenAI, AI Search, ADX). Read the code; don't run it.
- ⏭ Skip. It's outdated, broken, or too heavy for the sandbox.

---

## Ground rules for the sandbox

1. **Use one sample workspace.** Create a workspace named **`LAB-Samples`** on the trial. Keep `LAB-Platform-Gold-DEV` for your own medallion work. Several samples register the same table and model names (`df_clean`, `lgbm_sm`), so give each module its own lakehouse.
2. **Run one Spark session at a time.** Your capacity shows `8/8` with one session, so a second notebook gets error **430**. After every run, click **Connect → Stop session**. Use **Monitor** to cancel anything left running.
3. **Re-attach the lakehouse every time.** Imported notebooks point to Microsoft's lakehouse IDs. Open **Explorer → + Lakehouses**, pick yours, and pin it as the default.
4. **Match the lakehouse type to the sample.** Older samples write `Tables/<name>` and need **schemas OFF**. Newer ones (Materialized Lake Views, "Prepare and transform – Path 1") need **schemas ON**. Each module says which.
5. **Run `%pip` cells first.** Each one restarts the Python kernel.
6. **Import notebooks this way:** download the `.ipynb` from your fork on GitHub, then in the workspace go to **Import → Notebook → From this computer**.

---

## Phase 1 · Data Engineering (Lakehouse, Spark, Warehouse)

### 1.1 Lakehouse tutorial: modern version ✅ *(Lab 01 covered the legacy notebooks)*
| | |
|---|---|
| **Path** | `docs-samples/data-engineering/Lakehouse Tutorial Source Code/Prepare and transform data - PySpark.ipynb` (and the `- Spark SQL` twin) |
| **Teaches** | Delta tables and aggregates. The current tutorial runs on a **schemas-enabled** lakehouse (`Tables/dbo/...`). |
| **Needs** | Lakehouse `wwilakehouse` with **schemas ON**. Raw data copied to `Files/wwi-raw-data` (same copy as Lab 01). |
| **Fixes** | Run **only the Path 1 cells** and skip Path 2. The PySpark and Spark SQL versions produce aggregates with different column names, so pick one. |
| **Time** | 30 min |

**Steps**
1. In `LAB-Samples`, create a lakehouse named **`wwilakehouse`** with *Lakehouse schemas* ticked.
2. Copy the raw data with the Lab 01 notebook copy cell, pointed at this lakehouse:
   ```python
   notebookutils.fs.cp("wasbs://sampledata@fabrictutorialdata.blob.core.windows.net/WideWorldImportersDW/parquet/full", "Files/wwi-raw-data/full", recurse=True)
   ```
3. Import the PySpark notebook and attach `wwilakehouse`. Run the Path 1 cells.
4. Compare the result with Lab 01: `Tables/dbo/fact_sale` here versus `lh_silver.fact_sale` there.

**NTFB tie-in:** schemas-enabled lakehouses (`dbo`, `bronze`, `silver`) are now the default. Decide early which one NTFB will use.

### 1.2 Data Engineering Starter Kit ✅
| | |
|---|---|
| **Path** | `docs-samples/data-engineering/Notebook/GreenTripData/DataEngineeringStarterKit.ipynb` + `green_tripdata_2022-08.parquet` |
| **Teaches** | DataFrames, Delta writes, cleansing and filtering, EDA charts, aggregations, and joins to a lookup table. It's the broadest PySpark tour in the repo. |
| **Needs** | Lakehouse **`sample_lakehouse`**, schemas **OFF**. Upload the bundled parquet, plus `taxi+_zone_lookup.csv` from `https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv`. |
| **Fixes** | The images in the markdown are broken links (OneDrive). Cell 48's `withColumnRenamed("AVG Fare Amount","Zone")` does nothing, so ignore its description. |
| **Weight** | Light (1.3 MB) · **Time** 45 min |

**Steps**
1. Create `sample_lakehouse` with schemas off.
2. **Files → Upload**: the parquet file from the repo and the zone CSV.
3. Import the notebook, attach the lakehouse, and click **Run all**.

### 1.3 Materialized Lake Views ✅
| | |
|---|---|
| **Path** | `docs-samples/data-engineering/MaterializedLakeViews/tutorial/Notebook.ipynb` + 5 CSV folders (`agent`, `agent_commissions`, `location`, `orders`, `sales`) |
| **Teaches** | **Declarative silver and gold layers** in Spark SQL: `CREATE MATERIALIZED LAKE VIEW`, data-quality constraints (`CHECK … ON MISMATCH DROP / FAIL`), lineage view, and scheduled refresh. |
| **Needs** | Lakehouse **`SalesLakehouse`**, schemas **ON**, Runtime 1.3+. |
| **Fixes** | The sample has **no bronze-load step**. Add this cell at the top and run it first: |

```python
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")
for t in ["orders", "sales", "location", "agent", "agent_commissions"]:
    (spark.read.option("header", True).option("inferSchema", True).csv(f"Files/{t}")
          .write.mode("overwrite").saveAsTable(f"bronze.{t}"))
```

**Steps**
1. Create `SalesLakehouse` with schemas on.
2. Upload the 5 folders into **Files**.
3. Import the notebook, attach the lakehouse, add the bronze cell above, and run all.
4. Open the lakehouse, go to **Manage materialized lake views**, and look at the lineage graph. Schedule a refresh.

**Watch:** `gold.fact_sales` joins on non-unique keys and can multiply rows. Check its row count against `silver`, because this exact bug happens in real gold layers.

**NTFB tie-in:** this is the declarative alternative to your Lab 01 notebook and stored-procedure approach. DQ rules live in the view definition, and lineage is automatic.

### 1.4 Warehouse: OPENROWSET ✅
| | |
|---|---|
| **Path** | `features-samples/data-warehouse/openrowset.ipynb` |
| **Teaches** | Querying parquet, CSV and JSONL files with T-SQL directly, without loading them. Covers schema discovery (`sp_describe_first_result_set`) and explicit schemas with `WITH (...)`. |
| **Needs** | A Warehouse (use `wh_gold`, or create `wh_samples`). Public Bing COVID files. |
| **Fixes** | Re-attach your warehouse. The dataset is frozen (2022) but still served. |
| **Time** | 15 min · Uses no Spark |

**Steps:** import the notebook, go to **Explorer → + Data sources → your warehouse**, then run each cell.

### 1.5 Spark Job Definition + custom library ✅
| | |
|---|---|
| **Path** | `docs-samples/data-engineering/createTablefromParquet.py`, `docs-samples/data-engineering/SampleCustomJAR/MyLib.jar` |
| **Teaches** | Running Spark as a **job** (not a notebook), and attaching a custom JAR through an **Environment**. |
| **Needs** | A lakehouse with schemas off. `yellow_tripdata_2022-01.parquet` from nyc.gov TLC, uploaded to `Files/`. |
| **Fixes** | Use the **Parquet** script. `createTablefromCSV.py` is broken (`spark` is undefined). |

**Steps**
1. Create a **Spark Job Definition**, upload `createTablefromParquet.py` as the main file, attach the lakehouse, and click **Run**. Check that `yellowtrip` appears.
2. Create an **Environment**, add `MyLib.jar` under **Custom libraries**, and **Publish**. Attach it to a notebook, then run this in a `%%spark` cell: `MyLib.main(Array("hello","fabric"))`.

**NTFB tie-in:** scheduled production loads usually run as SJDs or pipelines, and shared code lives in Environments.

### 1.6 PII SparkShield 🟡 *(ties to AI governance)*
| | |
|---|---|
| **Path** | `community-samples/PII SparkShield/notebooks/` (01, 02, 03) + `data/customer-profile-sample-data/customers-sampledata.csv` |
| **Teaches** | **03:** masking and SHA-256 hashing with or without a salt. **01:** Presidio PII detection and redaction. **02:** Presidio + Faker synthetic replacement. |
| **Needs** | A lakehouse with the CSV at `Files/data/customer-profile-sample-data/`. **01/02 only:** an Environment with `presidio-analyzer`, `presidio-anonymizer` and `spacy`, plus the `en_core_web_lg` wheel (about 800 MB) uploaded to `Files/presidio/models/`. |
| **Fixes** | 03 runs as-is. 01/02: if the `numpy==1.26.3` pin fails on Runtime 1.3, remove it. |
| **Order** | **03 first** (light). Then 01/02 if you're willing to spend about 30 minutes building the Environment. |

**NTFB tie-in:** this is how neighbor and household PII would be handled before data reaches silver. It's good material for the AI Governance Committee.

---

## Phase 2 · Semantic Link: Power BI from notebooks ✅ *(highest value for BI governance)*

All are light, need no keys, and need no lakehouse unless noted. Use a **Python** or **PySpark** notebook.

**Setup once:** upload these semantic models to `LAB-Samples` with **Upload → Browse**:
- `docs-samples/data-science/datasets/Customer Profitability Sample.pbix`
- `docs-samples/data-science/datasets/Customer Profitability Sample (auto).pbix`
- *Retail Analysis Sample PBIX*, downloaded from Microsoft: `https://download.microsoft.com/download/9/6/D/96DDC2FF-2568-491D-AAFA-AFDD6F763AE3/Retail%20Analysis%20Sample%20PBIX.pbix`. Keep the model name **"Retail Analysis Sample PBIX"**.

| # | Notebook (`docs-samples/data-science/semantic-link-samples/`) | Teaches | Needs |
|---|---|---|---|
| 2.1 | `powerbi_measures_tutorial.ipynb` | `list_datasets`, `list_measures`, and `evaluate_measure` with group-bys and filters, which lets you query DAX measures from Python | Retail Analysis |
| 2.2 | `powerbi_relationships_tutorial.ipynb` | Listing and plotting relationships, **finding missing relationships**, and relationship violations | Both Customer Profitability models |
| 2.3 | `powerbi_dependencies_tutorial.ipynb` | Functional dependencies and data-quality violations (e.g. Postal Code → City) | Customer Profitability |
| 2.4 | `relationships_detection_tutorial.ipynb` | Discovering relationships on raw CSVs (Synthea data, downloaded by the notebook) | — |
| 2.5 | `data_cleaning_functional_dependencies_tutorial.ipynb` | Cleaning ZIP → City violations | — |
| 2.6 | `features-samples/semantic-link/Semantic Link all-in-one demo.ipynb` | `%%dax`, **DMVs**, `list_columns(extended=True)` (column size and "temperature"), `list_capacities`, `list_apps`, the Spark `pbi` catalog, writing to a lakehouse | Retail Analysis + a lakehouse |

**Fixes for 2.6:** skip three cells.
- The `add_measure(... dataset="Sales Dataset")` cell, because that model doesn't exist.
- The OpenAI cell, because `text-davinci-003` is retired.
- The "Train an ML Model" placeholder.

⏭ `great_expectations_tutorial.ipynb` pins Great Expectations < 1.0, a legacy API. Read it for the pattern, but don't run it.

**NTFB tie-in:** items 2.2, 2.3 and 2.6 make an **automated Power BI housekeeping and quality audit**: unused or cold columns, missing relationships, and DQ violations. That maps directly onto your Power BI governance standard.

---

## Phase 3 · Data Science (ML lifecycle, light versions)

### 3.1 Bank churn tutorial (1 → 4) ✅
| | |
|---|---|
| **Path** | `docs-samples/data-science/data-science-tutorial/1-ingest-data.ipynb`, `2-explore-cleanse-data.ipynb`, `3-train-evaluate.ipynb`, `4-predict.ipynb` |
| **Teaches** | The full ML lifecycle: ingest, Data Wrangler cleaning, MLflow experiments, registered models, and batch scoring with `PREDICT`. |
| **Needs** | One lakehouse (e.g. `churnLakehouse`) attached to all four. The data (10k rows) is downloaded by notebook 1. |
| **Fixes** | In 3, if pip conflicts on `scikit-learn==1.6.1` / `mlflow==2.12.2`, remove those pins. In 4, `modelVersion=1` assumes this is the first `lgbm_sm`. |
| **Time** | 60–90 min |

**Steps:** run 1 → 2 → 3 → 4 in order. After 3, open the **experiment** and **ML model** items in the workspace to see the runs. After 4, select the predictions table, then **New semantic model**, then a quick report.

⏭ Skip `01-…` to `05-…` (NYC Taxi). That series is heavy (tens of millions of rows), and notebook 02 breaks on pandas 2. The churn series teaches the same things.

### 3.2 Business-friendly AI samples ✅
Path: `docs-samples/data-science/ai-samples/python/`. Each stands alone. Attach a fresh lakehouse each time.

| Notebook | Teaches | Weight |
|---|---|---|
| `AIsample - Predictive Maintenance.ipynb` | Comparing RF, LR and XGBoost, PREDICT, and handoff to Power BI | Light |
| `AIsample - Superstore Forecast.ipynb` | SARIMAX demand forecast logged to MLflow | Light |
| `AIsample - Fraud Detection.ipynb` | Imbalanced data with SMOTE + LightGBM | Medium. **Set `IS_SAMPLE=True`.** |

**NTFB tie-in:** Superstore Forecast shows the forecasting pattern for demand and distribution.

### 3.3 Put ML predictions into Power BI 🟡
| | |
|---|---|
| **Path** | `docs-samples/data-science/enrich-powerbi-report-with-machine-learning/Bank Customer Churn Analysis/` |
| **Teaches** | The end-to-end pattern: a governed semantic model, read through semantic link into features, then an MLflow model, batch predictions, and a Direct Lake report. |
| **Format** | **Git-format item folders** (`.platform`, `notebook-content.py`), not `.ipynb`. Load them through **Git integration** (Phase 4.1), or copy the notebook code into new notebooks. |
| **Fixes** | (1) Notebook 00 reads hard-coded `abfss://f5d4f720-…/Files/churn.csv`. Change it to `Files/churn.csv`. (2) `active.csv` isn't in the repo. Make one from `churn.csv` minus the `Exited` column. (3) Skip the cell that shows `CreateSemanticModel.png`. (4) Skip the **Dataflow**, which needs a real-time ML endpoint and a service principal. |
| **Order** | Upload CSVs → 00 → create semantic model "Bank Customer Churn Analysis" on table `churn` → 01 → 02 → 03 → report |

**Do it after Phase 4.1**, so you can pull the folders in with Git.

---

## Phase 4 · Platform: Git, deployment, CI/CD, monitoring

All of these use PowerShell or Python on your PC, plus your `fabric-lab-automation` service principal or your user sign-in.

**Common fix for every script in `features-samples/`:** set `$global:baseUrl = "https://api.fabric.microsoft.com/v1"`.

### 4.1 Git integration ✅
Path: `features-samples/git-integration/`. Run the scripts in this order:

| Order | Script | Teaches | Fix |
|---|---|---|---|
| 1 | `GitIntegration-StoreGitProviderCredentials.ps1` | Saving a GitHub PAT as a Fabric connection | Create a **fine-grained PAT** scoped to `ImranShahNewPort/fabric-samples` with Contents read/write |
| 2 | `GitIntegration-ConnectAndUpdateFromGit.ps1` | Connecting a workspace to a repo, branch and folder, then syncing | Set `$gitProviderDetails = $gitHubDetails`. Use a **new branch** `lab-git` and folder `/fabric/lab-samples`. Connect **`LAB-Samples`**, not Gold-DEV. |
| 3 | Change something in the UI | — | — |
| 4 | `GitIntegration-CommitAll.ps1` | Committing every change | — |
| 5 | `GitIntegration-CommitSelective.ps1` | Committing chosen items | Change `ItemType -eq "dataset"` → `"SemanticModel"` (the API renamed it) |
| 6 | `GitIntegration-UpdateFromGit.ps1` | Pulling repo changes into the workspace | — |
| 7 | `features-samples/fabric-apis/LongRunningOperation-Polling.ps1` | Polling the operations that 4–6 print | — |

**Tenant settings** (Admin portal): turn on *Users can synchronize workspace items with their Git repositories* and *…with GitHub repositories*.

**Then:** do Phase 3.3. Copy the `Bank Customer Churn Analysis` item folders into `/fabric/lab-samples` on the `lab-git` branch, and run **UpdateFromGit**.

### 4.2 Deployment pipelines by API ✅
Path: `features-samples/fabric-apis/`

| Script | Teaches | Values to set |
|---|---|---|
| `DeploymentPipelines-DeployAll.ps1` | Deploying every item from one stage to the next, with polling | Pipeline `LAB Gold Pipeline`, stages `DEV` → `PROD` |
| `DeploymentPipelines-SelectiveDeploy.ps1` | Deploying one item | Item name and type (`Notebook`, `SemanticModel`, `Report`). Poll with `LongRunningOperation-Polling.ps1`. |
| ⏭ `…AssignToNewDeploymentPipelineAndDeploy.ps1` | Creating a pipeline and a new PROD workspace | Broken: it points at an **internal staging URL**. Fix the base URL and use a **new** DEV workspace, because Gold-DEV is already in a pipeline. |

Upload `docs-samples/cicd/FoodSales.pbix` to Gold-DEV to have a report to deploy.

### 4.3 CI/CD with `fabric-cicd` ✅
| | |
|---|---|
| **Path** | `docs-samples/cicd/fabric-cicd-local/fabric_items/deploy.py` |
| **Teaches** | Publishing Git-format item folders to a workspace with Microsoft's `fabric-cicd` Python library. This is the code-first alternative to deployment pipelines. |
| **Needs** | Python 3.9–3.12, `pip install fabric-cicd azure-identity`, Azure CLI (`az login`) or service-principal environment variables |
| **Fixes** | Set `workspace_id`. `Notebook_1.Notebook` has no content file, so publish only the lakehouse first (`item_type_in_scope=["Lakehouse"]`), or copy a real notebook folder in from your Git sync (4.1). |
| **Target** | A scratch workspace `LAB-CICD-Scratch`. Never a pipeline-managed one. |

**Next level (optional):** `docs-samples/cicd/fabric-cicd-devops/` is the same thing run from an **Azure DevOps** pipeline, with `parameter.yml` swapping IDs per environment. It needs an ADO organization. Port it to **GitHub Actions** to match your fork.

### 4.4 Workspace monitoring ✅
| | |
|---|---|
| **Path** | `workspace-monitoring/` (KQL snippets, files have no extension) |
| **Teaches** | Querying operational logs (semantic model queries, refreshes, Eventhouse operations) with KQL |
| **Needs** | Tenant setting *Workspace admins can turn on monitoring*. Then **Workspace settings → Monitoring → Add Eventhouse** on `LAB-Samples`. |
| **Fixes** | Some snippets are halves of one query and need joining: *Extract application name* + *Top applications by used CPU time*, and *Number of bar chart requests* + *List start times…*. `IngestionVolumeInGb` actually returns MB. |
| **Cost** | The monitoring Eventhouse uses capacity continuously. **Turn it off when you're done.** |

**Steps:** after Phase 2 has created semantic-model activity, open the **Monitoring KQL database → New KQL queryset**, then paste the `Semantic model operations` queries.

---

## Phase 5 · Real-Time Intelligence (Eventhouse, KQL)

Create an **Eventhouse** in `LAB-Samples` for this phase.

| # | Sample | Teaches | Fit |
|---|---|---|---|
| 5.1 | `docs-samples/real-time-intelligence/tutorial-commands-script.kql` | Medallion **inside an Eventhouse**: raw table → update policy → silver table → **materialized view** (gold), on London bikes streaming data | ✅ Create an **Eventstream** with the *Bicycles* sample, sending to table `RawData`. Run the script in a KQL queryset. Build a Real-Time Dashboard. |
| 5.2 | `community-samples/Real-Time-Intelligence-L400/` (`SalesOrderHeader.csv`, `SalesOrderDetail.csv`) | KQL joins, update policies and materialized views on 150k rows | ✅ **Get data → Local file** into the Eventhouse. There's no notebook, so write your own KQL. |
| 5.3 | `docs-samples/real-time-intelligence/fabric-events/` (`Sales.csv`) | Event-driven pipelines: a file landing in OneLake triggers a pipeline through Activator | ✅ Follow the Learn tutorial "Fabric events → event-driven pipeline" |
| 5.4 | `NYC_GreenTaxi_KQL_notebook.ipynb` | Spark → Eventhouse with the Kusto connector | 🟡 **Heavy.** Add `df = df.filter("puYear = 2016").limit(500000)` before the write. Use the Fabric URI format `https://<id>.<region>.kusto.fabric.microsoft.com`. |
| ⏭ | `IngestNYCTaxi2014.kql` + `NYC_Taxi_KQL_Notebook.ipynb` | A full year of taxi data plus Kqlmagic | Too heavy, and Kqlmagic is outdated |
| ⏭ | `multivariate-anomaly-detection-tutorial.ipynb` | Anomaly detection with an Eventhouse Python plugin | Needs a custom Environment plus the plugin. Advanced; do it later. |
| ☁️ | `vector-database-eventhouse-notebook.ipynb` | Vector search in an Eventhouse | Needs Azure OpenAI, and the code uses the pre-1.0 openai API |
| ☁️ | `MigrateFromAzureToEventhouse.ps1` | Migrating from ADX | Needs an existing ADX cluster |

**NTFB tie-in:** 5.1 is the pattern for live warehouse and inventory telemetry. 5.3 is the pattern for "agency file lands → validate → load", the event-driven version of the agency intake pipeline.

---

## Phase 6 · Preview features (explore when curious)

| Sample | Teaches | Notes |
|---|---|---|
| `docs-samples/graph/` | Graph model + GQL over node and edge Delta tables (AdventureWorks, LDBC) | Preview. **Unzip on your PC first**, then upload the table folders to `Tables/`. |
| `docs-samples/iq/ontology/new-experience/` | Ontology over cold-chain retail tables | Preview. Directly relevant to your **NTFB business ontology and knowledge graph** work. |
| `docs-samples/iq/plan/` | Planning / PowerTable semantic models | Preview. `Enterprise Dataset.pbix` uses a paid custom visual. |
| `docs-samples/real-time-intelligence/digital-twin-builder/` | Digital Twin Builder + Eventhouse projections | Preview tenant switch. The notebook's last cell has a bug: remove `.encode('utf-8')`. |

---

## Parked: paid capacity required 💰

Run these when NTFB has an F-SKU with **Copilot / Azure OpenAI tenant settings** on. They're the most governance-relevant AI samples in the repo.

| Sample | Why it matters |
|---|---|
| `docs-samples/data-science/ai-functions/starter-notebooks/AIFunctions-pandas-starter-notebook.ipynb` | `ai.classify`, `ai.extract`, `ai.summarize` and `ai.analyze_sentiment` as one-line calls on tables. This is the modern replacement for the LangChain samples. |
| `docs-samples/data-science/ai-functions/eval-notebooks/AIFunctions-pandas-eval-classify.ipynb` and `…-extract.ipynb` | **LLM-as-judge quality evaluation**: precision, recall and F1 for an AI feature. It's the evidence an AI council should require before approving an AI use case. |
| `docs-samples/data-science/data-agent-sdk/` | Building, publishing and **evaluating** a Fabric Data Agent. Order: Automation → `responses-api/` → Evaluation → Few-Shot Validator. Skip the OpenAI-Client notebook, which uses the older Assistants API. |

---

## Skip list ⏭ / ☁️

| Sample | Reason |
|---|---|
| `docs-samples/onelake/unity-catalog/`, `sf-iceberg-shortcuts/` | ☁️ Needs Databricks or Snowflake plus ADLS. Read them for the shortcut-sync pattern. |
| `docs-samples/data-engineering/SparkMonitoring/` (YAML emitters) | ☁️ Needs Azure Monitor and Log Analytics |
| `docs-samples/data-engineering/GraphQL/*` | Client apps that need an app registration. Optional developer track. |
| `community-samples/Metadata-driven Ingestion Framework/` | ☁️ Needs Azure SQL + PostgreSQL. **Read the control-table design**: it's the pipeline version of your Lab 01 Exercise D. |
| `docs-samples/data-science/genai-guidance/` | ☁️ Needs Azure AI Search + Azure OpenAI. It uses retired `gpt-4-32k` and a 2024 snapshot JAR. |
| `docs-samples/data-science/llm-samples/` | Retired `gpt-35-turbo`, LangChain 0.0.x. Replaced by AI functions. |
| `docs-samples/data-science/fabric-conference-2024/` | Lab 2 relies on a private API, which breaks labs 3–5. 4B/4C use a Python 3.10 wheel. Read the `ANSWER_KEY` files instead. |
| `ai-samples/python/AIsample - Uplift Modelling.ipynb` | 13M rows from an external host |
| `ai-samples/r/*` | R-only. Skip unless R matters to the team. |
| `e2e-samples/` | An empty stub; the content moved to `features-samples/` |
| `docs-samples/data-life-cycle-architecture/` | Diagram files only |

---

## Suggested pace

| Week | Modules |
|---|---|
| 1 | 1.1 → 1.3 (Lakehouse modern, Starter Kit, Materialized Lake Views) |
| 2 | 1.4 → 1.6, Phase 2 (semantic link) |
| 3 | Phase 3.1–3.2, Phase 4.1 (Git) |
| 4 | Phase 3.3 (via Git), 4.2 → 4.4 |
| 5 | Phase 5 |
| Later | Phase 6, and the parked 💰 samples once paid capacity exists |

Track progress in [README.md](README.md).
