# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Generate GPU telemetry
# MAGIC Synthesizes a realistic DGX GPU telemetry fact table at scale S / M / L and builds per-scenario source tables.

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "dgx_benchmark")
dbutils.widgets.dropdown("scale", "S", ["S", "M", "L"])

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 2)[0])

from src.common.config import BenchmarkConfig
from src.generate.generate_telemetry import generate

cfg = BenchmarkConfig(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    scale=dbutils.widgets.get("scale"),
)
print(f"Generating scale={cfg.scale} into {cfg.catalog}.{cfg.schema}")

# COMMAND ----------
generate(spark, cfg)

# COMMAND ----------
display(spark.sql(f"SHOW TABLES IN {cfg.catalog}.{cfg.schema}"))
