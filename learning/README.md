# Fabric Learning Track

Personal learning notes on a fork of `microsoft/fabric-samples`. Labs run in the **LAB sandbox** (`LAB-Platform-Gold-DEV`: `lh_bronze` → `lh_silver` → `wh_gold`, automated through the `fabric-lab-automation` service principal), not locally. They depend on Fabric's Spark session, Lakehouse `Files/` and `Tables/` paths, and `sempy`.

**Start here → [LEARNING-PATH.md](LEARNING-PATH.md)**: every Microsoft sample in this repo, in order, with sandbox fit, fixes and run steps.

| Phase | Topic | Status |
|---|---|---|
| [Lab 01](LAB-01-lakehouse-medallion.md) | Own medallion build on LAB sandbox: bronze → silver → gold star, MERGE, pipeline, metadata loader | ✅ (deploy + E open) |
| 1 | Data Engineering: Lakehouse (modern), Starter Kit, Materialized Lake Views, OPENROWSET, SJD + JAR, PII | ⬜ |
| 2 | Semantic Link: Power BI from notebooks | ⬜ |
| 3 | Data Science: churn tutorial, AI samples, ML → Power BI | ⬜ |
| 4 | Platform: Git integration, deployment API, fabric-cicd, workspace monitoring | ⬜ |
| 5 | Real-Time Intelligence: Eventhouse, KQL, Fabric events | ⬜ |
| 6 | Preview: Graph, IQ ontology, Digital Twin | ⬜ |
| 💰 | Paid capacity: AI functions, Data Agent | parked |

## Keeping this fork current
On GitHub, open the fork and select **Sync fork** to pull Microsoft's latest samples into `main`. Then merge `main` into this `learning` branch.
