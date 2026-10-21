# Fabric Learning Track

Personal learning notes on a fork of `microsoft/fabric-samples`. Labs run in the **LAB sandbox** (`LAB-Platform-Gold-DEV`: `lh_bronze` → `lh_silver` → `wh_gold`, automated through the `fabric-lab-automation` service principal), not locally. They depend on Fabric's Spark session, Lakehouse `Files/` and `Tables/` paths, and `sempy`.

| Lab | Topic | Sample folder | Status |
|---|---|---|---|
| [01](LAB-01-lakehouse-medallion.md) | Lakehouse medallion: raw → Delta → aggregates | `docs-samples/data-engineering/Lakehouse Tutorial Source Code/` | ✅ silver · gold · star · A merge · B pipeline · D metadata (deploy + E open) |
| 02 | Data Engineering Starter Kit (PySpark deep dive) | `docs-samples/data-engineering/Notebook/GreenTripData/` | ⬜ |
| 03 | Materialized Lake Views (declarative silver/gold) | `docs-samples/data-engineering/MaterializedLakeViews/` | ⬜ |
| 04 | Semantic Link: notebooks ↔ Power BI models | `features-samples/semantic-link/` | ⬜ |
| 05 | CI/CD with `fabric-cicd` | `docs-samples/cicd/fabric-cicd-local/` | ⬜ |
| 06 | PII detection and masking in Spark | `community-samples/PII SparkShield/` | ⬜ |

## Keeping this fork current
On GitHub, open the fork and select **Sync fork** to pull Microsoft's latest samples into `main`. Then merge `main` into this `learning` branch.
