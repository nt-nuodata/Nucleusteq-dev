# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

# COMMAND ----------
mainWorkflowId = dbutils.widgets.get("mainWorkflowId")
mainWorkflowRunId = dbutils.widgets.get("mainWorkflowRunId")
parentName = dbutils.widgets.get("parentName")
preVariableAssignment = dbutils.widgets.get("preVariableAssignment")
postVariableAssignment = dbutils.widgets.get("postVariableAssignment")
truncTargetTableOptions = dbutils.widgets.get("truncTargetTableOptions")
variablesTableName = dbutils.widgets.get("variablesTableName")

# COMMAND ----------
#Truncate Target Tables
truncateTargetTables(truncTargetTableOptions)

# COMMAND ----------
#Pre presession variable updation
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Sales_Plan_Variance_Validation")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Sales_Plan_Variance_Validation", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_BATCH_LOAD_AUD_LOG_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  BATCH_DATE AS BATCH_DATE,
  AOS AS AOS,
  ISPU AS ISPU,
  SFS AS SFS,
  STR AS STR,
  WEB AS WEB,
  STX_COUNT AS STX_COUNT,
  EDW_COUNT AS EDW_COUNT,
  EDW_SALES AS EDW_SALES,
  STX_SALES AS STX_SALES,
  PLAN_SALES AS PLAN_SALES,
  ACTUAL_SALES AS ACTUAL_SALES,
  SALES_VARIANCE AS SALES_VARIANCE,
  PLAN_VARIANCE AS PLAN_VARIANCE
FROM
  BATCH_LOAD_AUD_LOG"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_BATCH_LOAD_AUD_LOG_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_BATCH_LOAD_AUD_LOG_1


query_1 = f"""select
  SALES_VARIANCE as VARIANCE,
  1 as Title
FROM
  Shortcut_to_BATCH_LOAD_AUD_LOG_0
WHERE
  Shortcut_to_BATCH_LOAD_AUD_LOG_0.BATCH_DATE = CURRENT_DATE
  and (
    Shortcut_to_BATCH_LOAD_AUD_LOG_0.SALES_VARIANCE > {Threshold_Variance_3}
    or Shortcut_to_BATCH_LOAD_AUD_LOG_0.SALES_VARIANCE < {Threshold_Variance_4}
  )
UNION
select
  PLAN_VARIANCE as VARIANCE,
  2 as Title
FROM
  Shortcut_to_BATCH_LOAD_AUD_LOG_0
WHERE
  Shortcut_to_BATCH_LOAD_AUD_LOG_0.BATCH_DATE = CURRENT_DATE
  AND (
    Shortcut_to_BATCH_LOAD_AUD_LOG_0.PLAN_VARIANCE > {Threshold_Variance_1}
    or Shortcut_to_BATCH_LOAD_AUD_LOG_0.PLAN_VARIANCE < {Threshold_Variance_2}
  )"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_BATCH_LOAD_AUD_LOG_1")

# COMMAND ----------
# DBTITLE 1, Variance_Validation


spark.sql("""INSERT INTO
  Variance_Validation
SELECT
  SALES_VARIANCE AS VARIANCE,
  PLAN_VARIANCE AS TITLE
FROM
  SQ_Shortcut_to_BATCH_LOAD_AUD_LOG_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Sales_Plan_Variance_Validation")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Sales_Plan_Variance_Validation", mainWorkflowId, parentName)
