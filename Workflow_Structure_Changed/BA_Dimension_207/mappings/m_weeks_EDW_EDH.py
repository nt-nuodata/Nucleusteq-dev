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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_weeks_EDW_EDH")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_weeks_EDW_EDH", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Weeks_0


query_0 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  CAL_WK AS CAL_WK,
  CAL_WK_NBR AS CAL_WK_NBR,
  CAL_MO AS CAL_MO,
  CAL_MO_NBR AS CAL_MO_NBR,
  CAL_MO_NAME AS CAL_MO_NAME,
  CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
  CAL_QTR AS CAL_QTR,
  CAL_QTR_NBR AS CAL_QTR_NBR,
  CAL_HALF AS CAL_HALF,
  CAL_YR AS CAL_YR,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_WK_NBR AS FISCAL_WK_NBR,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_MO_NBR AS FISCAL_MO_NBR,
  FISCAL_MO_NAME AS FISCAL_MO_NAME,
  FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
  FISCAL_HALF AS FISCAL_HALF,
  FISCAL_YR AS FISCAL_YR,
  LYR_WEEK_DT AS LYR_WEEK_DT,
  LWK_WEEK_DT AS LWK_WEEK_DT
FROM
  WEEKS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_Weeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Weeks_1


query_1 = f"""SELECT
  WeekDt AS WeekDt,
  CalWk AS CalWk,
  CalWkNbr AS CalWkNbr,
  CalMo AS CalMo,
  CalMoNbr AS CalMoNbr,
  CalMoName AS CalMoName,
  CalMoNameAbbr AS CalMoNameAbbr,
  CalQtr AS CalQtr,
  CalQtrNbr AS CalQtrNbr,
  CalHalf AS CalHalf,
  CalYr AS CalYr,
  FiscalWk AS FiscalWk,
  FiscalWkNbr AS FiscalWkNbr,
  FiscalMo AS FiscalMo,
  FiscalMoNbr AS FiscalMoNbr,
  FiscalMoName AS FiscalMoName,
  FiscalMoNameAbbr AS FiscalMoNameAbbr,
  FiscalQtr AS FiscalQtr,
  FiscalQtrNbr AS FiscalQtrNbr,
  FiscalHalf AS FiscalHalf,
  FiscalYr AS FiscalYr,
  LyrWeekDt AS LyrWeekDt,
  LwkWeekDt AS LwkWeekDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Weeks
WHERE
  FiscalYr = DATEPART(YY, GETDATE()) + 2"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_Weeks_1")

# COMMAND ----------
# DBTITLE 1, WEEKS


spark.sql("""INSERT INTO
  WEEKS
SELECT
  WeekDt AS WEEK_DT,
  CalWk AS CAL_WK,
  CalWkNbr AS CAL_WK_NBR,
  CalMo AS CAL_MO,
  CalMoNbr AS CAL_MO_NBR,
  CalMoName AS CAL_MO_NAME,
  CalMoNameAbbr AS CAL_MO_NAME_ABBR,
  CalQtr AS CAL_QTR,
  CalQtrNbr AS CAL_QTR_NBR,
  CalHalf AS CAL_HALF,
  CalYr AS CAL_YR,
  FiscalWk AS FISCAL_WK,
  FiscalWkNbr AS FISCAL_WK_NBR,
  FiscalMo AS FISCAL_MO,
  FiscalMoNbr AS FISCAL_MO_NBR,
  FiscalMoName AS FISCAL_MO_NAME,
  FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
  FiscalQtr AS FISCAL_QTR,
  FiscalQtrNbr AS FISCAL_QTR_NBR,
  FiscalHalf AS FISCAL_HALF,
  FiscalYr AS FISCAL_YR,
  LyrWeekDt AS LYR_WEEK_DT,
  LwkWeekDt AS LWK_WEEK_DT
FROM
  SQ_Shortcut_to_Weeks_1""")

# COMMAND ----------
# DBTITLE 1, WEEKS


spark.sql("""INSERT INTO
  WEEKS
SELECT
  WeekDt AS WEEK_DT,
  CalWk AS CAL_WK,
  CalWkNbr AS CAL_WK_NBR,
  CalMo AS CAL_MO,
  CalMoNbr AS CAL_MO_NBR,
  CalMoName AS CAL_MO_NAME,
  CalMoNameAbbr AS CAL_MO_NAME_ABBR,
  CalQtr AS CAL_QTR,
  CalQtrNbr AS CAL_QTR_NBR,
  CalHalf AS CAL_HALF,
  CalYr AS CAL_YR,
  FiscalWk AS FISCAL_WK,
  FiscalWkNbr AS FISCAL_WK_NBR,
  FiscalMo AS FISCAL_MO,
  FiscalMoNbr AS FISCAL_MO_NBR,
  FiscalMoName AS FISCAL_MO_NAME,
  FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
  FiscalQtr AS FISCAL_QTR,
  FiscalQtrNbr AS FISCAL_QTR_NBR,
  FiscalHalf AS FISCAL_HALF,
  FiscalYr AS FISCAL_YR,
  LyrWeekDt AS LYR_WEEK_DT,
  LwkWeekDt AS LWK_WEEK_DT
FROM
  SQ_Shortcut_to_Weeks_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_weeks_EDW_EDH")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_weeks_EDW_EDH", mainWorkflowId, parentName)
