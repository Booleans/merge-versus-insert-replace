# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Build report
# MAGIC Generates `report/results.md`, `report/results.html`, and `report/findings_template.md`.

# COMMAND ----------
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "gpu_bench")

# COMMAND ----------
import sys
sys.path.insert(0, "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 2)[0])

from src.common.config import BenchmarkConfig
from src.report.build_report import build, REPORT_DIR

cfg = BenchmarkConfig(
    catalog=dbutils.widgets.get("catalog"),
    schema=dbutils.widgets.get("schema"),
)

# COMMAND ----------
build(spark, cfg)
print(f"Wrote report to {REPORT_DIR}")

# COMMAND ----------
# Surface the HTML inline
from pathlib import Path
html = Path(REPORT_DIR / "results.html").read_text()
displayHTML(html)
