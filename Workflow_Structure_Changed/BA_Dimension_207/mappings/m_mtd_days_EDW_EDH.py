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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_mtd_days_EDW_EDH")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_mtd_days_EDW_EDH", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MtdDays_0


query_0 = f"""SELECT
  DayDt AS DayDt,
  MtdDayDt AS MtdDayDt,
  LyrMtdDayDt AS LyrMtdDayDt
FROM
  MtdDays"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_MtdDays_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MtdDays_1


query_1 = f"""SELECT
  mtd.DayDt AS DayDt,
  mtd.MtdDayDt AS MtdDayDt,
  mtd.LyrMtdDayDt AS LyrMtdDayDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_MtdDays_0 mtd,
  Days d
WHERE
  mtd.DayDt = d.DayDt
  AND d.FiscalYr = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_MtdDays_1")

# COMMAND ----------
# DBTITLE 1, MTD_DAYS


spark.sql("""INSERT INTO
  MTD_DAYS
SELECT
  DayDt AS DAY_DT,
  MtdDayDt AS MTD_DAY_DT,
  LyrMtdDayDt AS LYR_MTD_DAY_DT
FROM
  SQ_Shortcut_to_MtdDays_1""")

# COMMAND ----------
# DBTITLE 1, MTD_DAYS


spark.sql("""INSERT INTO
  MTD_DAYS
SELECT
  DayDt AS DAY_DT,
  MtdDayDt AS MTD_DAY_DT,
  LyrMtdDayDt AS LYR_MTD_DAY_DT
FROM
  SQ_Shortcut_to_MtdDays_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_mtd_days_EDW_EDH")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_mtd_days_EDW_EDH", mainWorkflowId, parentName)