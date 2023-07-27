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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_mtd_weeks_EDW_EDH")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_mtd_weeks_EDW_EDH", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MtdWeeks_0


query_0 = f"""SELECT
  WeekDt AS WeekDt,
  MtdWeekDt AS MtdWeekDt,
  LyrMtdWeekDt AS LyrMtdWeekDt
FROM
  MtdWeeks"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_MtdWeeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MtdWeeks_1


query_1 = f"""SELECT
  mtd.WeekDt AS WeekDt,
  mtd.MtdWeekDt AS MtdWeekDt,
  mtd.LyrMtdWeekDt AS LyrMtdWeekDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_MtdWeeks_0 mtd,
  Weeks w
WHERE
  mtd.WeekDt = w.WeekDt
  AND w.FiscalYr = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_MtdWeeks_1")

# COMMAND ----------
# DBTITLE 1, MTD_WEEKS


spark.sql("""INSERT INTO
  MTD_WEEKS
SELECT
  WeekDt AS WEEK_DT,
  MtdWeekDt AS MTD_WEEK_DT,
  LyrMtdWeekDt AS LYR_MTD_WEEK_DT
FROM
  SQ_Shortcut_to_MtdWeeks_1""")

# COMMAND ----------
# DBTITLE 1, MTD_WEEKS


spark.sql("""INSERT INTO
  MTD_WEEKS
SELECT
  WeekDt AS WEEK_DT,
  MtdWeekDt AS MTD_WEEK_DT,
  LyrMtdWeekDt AS LYR_MTD_WEEK_DT
FROM
  SQ_Shortcut_to_MtdWeeks_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_mtd_weeks_EDW_EDH")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_mtd_weeks_EDW_EDH", mainWorkflowId, parentName)
