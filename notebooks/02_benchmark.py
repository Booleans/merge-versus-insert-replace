# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Run benchmark matrix
# MAGIC Runs MERGE / REPLACE WHERE / REPLACE USING / REPLACE ON across four scenarios and appends results.

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "gpu_bench")
dbutils.widgets.dropdown("scale", "S", ["S", "M", "L"])
dbutils.widgets.dropdown("compute_flavor", "photon_cluster", ["photon_cluster", "serverless_sql"])
dbutils.widgets.text("repeats", "3")

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 2)[0])

from src.common.config import BenchmarkConfig
from src.benchmark.run_benchmark import run_matrix, summarize

cfg = BenchmarkConfig(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
    scale=dbutils.widgets.get("scale"),
    compute_flavor=dbutils.widgets.get("compute_flavor"),
    repeats=int(dbutils.widgets.get("repeats")),
)
print(cfg)

# COMMAND ----------
run_matrix(spark, cfg)

# COMMAND ----------
summarize(spark, cfg)
