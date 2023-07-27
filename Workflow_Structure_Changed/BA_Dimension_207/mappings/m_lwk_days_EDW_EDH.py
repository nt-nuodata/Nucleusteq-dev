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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_lwk_days_EDW_EDH")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_lwk_days_EDW_EDH", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_LwkDays_0


query_0 = f"""SELECT
  LwkDayDt AS LwkDayDt,
  DayDt AS DayDt
FROM
  LwkDays"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_LwkDays_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LwkDays_1


query_1 = f"""SELECT
  ld.LwkDayDt AS LwkDayDt,
  ld.DayDt AS DayDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_LwkDays_0 ld,
  Days d
WHERE
  ld.DayDt = d.DayDt
  AND d.FiscalYr = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_LwkDays_1")

# COMMAND ----------
# DBTITLE 1, LWK_DAYS


spark.sql("""INSERT INTO
  LWK_DAYS
SELECT
  LwkDayDt AS LWK_DAY_DT,
  DayDt AS DAY_DT
FROM
  SQ_Shortcut_to_LwkDays_1""")

# COMMAND ----------
# DBTITLE 1, LWK_DAYS


spark.sql("""INSERT INTO
  LWK_DAYS
SELECT
  LwkDayDt AS LWK_DAY_DT,
  DayDt AS DAY_DT
FROM
  SQ_Shortcut_to_LwkDays_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_lwk_days_EDW_EDH")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_lwk_days_EDW_EDH", mainWorkflowId, parentName)
