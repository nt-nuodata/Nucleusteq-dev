# Databricks notebook source
# MAGIC %run ./Env

# COMMAND ----------

dbutils.notebook.run("./WorkflowExecutor", 0, {"workflowName":"wf_WMS_To_SCDS_Every_4_Hrs"})

# COMMAND ----------

