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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_lyr_weeks_EDW_EDH")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_lyr_weeks_EDW_EDH", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_LyrWeeks_0


query_0 = f"""SELECT
  WeekDt AS WeekDt,
  CalHalf AS CalHalf,
  CalMo AS CalMo,
  CalMoName AS CalMoName,
  CalMoNameAbbr AS CalMoNameAbbr,
  CalMoNbr AS CalMoNbr,
  CalQtr AS CalQtr,
  CalQtrNbr AS CalQtrNbr,
  CalWk AS CalWk,
  CalYr AS CalYr,
  FiscalHalf AS FiscalHalf,
  FiscalMoName AS FiscalMoName,
  FiscalMoNameAbbr AS FiscalMoNameAbbr,
  FiscalMoNbr AS FiscalMoNbr,
  FiscalQtr AS FiscalQtr,
  FiscalQtrNbr AS FiscalQtrNbr,
  FiscalWk AS FiscalWk,
  FiscalYr AS FiscalYr,
  LwkWeekDt AS LwkWeekDt,
  FiscalMo AS FiscalMo,
  FiscalMoDt AS FiscalMoDt,
  FiscalQtrDt AS FiscalQtrDt,
  FiscalYrDt AS FiscalYrDt,
  LyrWeekDt AS LyrWeekDt,
  LyrFiscalWk AS LyrFiscalWk,
  LyrFiscalMoDt AS LyrFiscalMoDt,
  LyrFiscalQtrDt AS LyrFiscalQtrDt,
  LyrFiscalYrDt AS LyrFiscalYrDt,
  PyrWeekDt AS PyrWeekDt,
  PyrFiscalWk AS PyrFiscalWk,
  PyrFiscalMoDt AS PyrFiscalMoDt,
  PyrFiscalQtrDt AS PyrFiscalQtrDt,
  PyrFiscalYrDt AS PyrFiscalYrDt
FROM
  LyrWeeks"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_LyrWeeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LyrWeeks_1


query_1 = f"""SELECT
  WeekDt AS WeekDt,
  CalHalf AS CalHalf,
  CalMo AS CalMo,
  CalMoName AS CalMoName,
  CalMoNameAbbr AS CalMoNameAbbr,
  CalMoNbr AS CalMoNbr,
  CalQtr AS CalQtr,
  CalQtrNbr AS CalQtrNbr,
  CalWk AS CalWk,
  CalYr AS CalYr,
  FiscalHalf AS FiscalHalf,
  FiscalMoName AS FiscalMoName,
  FiscalMoNameAbbr AS FiscalMoNameAbbr,
  FiscalMoNbr AS FiscalMoNbr,
  FiscalQtr AS FiscalQtr,
  FiscalQtrNbr AS FiscalQtrNbr,
  FiscalWk AS FiscalWk,
  FiscalYr AS FiscalYr,
  LwkWeekDt AS LwkWeekDt,
  FiscalMo AS FiscalMo,
  FiscalMoDt AS FiscalMoDt,
  FiscalQtrDt AS FiscalQtrDt,
  FiscalYrDt AS FiscalYrDt,
  LyrWeekDt AS LyrWeekDt,
  LyrFiscalWk AS LyrFiscalWk,
  LyrFiscalMoDt AS LyrFiscalMoDt,
  LyrFiscalQtrDt AS LyrFiscalQtrDt,
  LyrFiscalYrDt AS LyrFiscalYrDt,
  PyrWeekDt AS PyrWeekDt,
  PyrFiscalWk AS PyrFiscalWk,
  PyrFiscalMoDt AS PyrFiscalMoDt,
  PyrFiscalQtrDt AS PyrFiscalQtrDt,
  PyrFiscalYrDt AS PyrFiscalYrDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_LyrWeeks_0
WHERE
  FiscalYr = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_LyrWeeks_1")

# COMMAND ----------
# DBTITLE 1, LYR_WEEKS


spark.sql("""INSERT INTO
  LYR_WEEKS
SELECT
  WeekDt AS WEEK_DT,
  CalHalf AS CAL_HALF,
  CalMo AS CAL_MO,
  CalMoName AS CAL_MO_NAME,
  CalMoNameAbbr AS CAL_MO_NAME_ABBR,
  CalMoNbr AS CAL_MO_NBR,
  CalQtr AS CAL_QTR,
  CalQtrNbr AS CAL_QTR_NBR,
  CalWk AS CAL_WK,
  CalYr AS CAL_YR,
  FiscalHalf AS FISCAL_HALF,
  FiscalMoName AS FISCAL_MO_NAME,
  FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
  FiscalMoNbr AS FISCAL_MO_NBR,
  FiscalQtr AS FISCAL_QTR,
  FiscalQtrNbr AS FISCAL_QTR_NBR,
  FiscalWk AS FISCAL_WK,
  FiscalYr AS FISCAL_YR,
  LwkWeekDt AS LWK_WEEK_DT,
  FiscalMo AS FISCAL_MO,
  FiscalMoDt AS FISCAL_MO_DT,
  FiscalQtrDt AS FISCAL_QTR_DT,
  FiscalYrDt AS FISCAL_YR_DT,
  LyrWeekDt AS LYR_WEEK_DT,
  LyrFiscalWk AS LYR_FISCAL_WK,
  LyrFiscalMoDt AS LYR_FISCAL_MO_DT,
  LyrFiscalQtrDt AS LYR_FISCAL_QTR_DT,
  LyrFiscalYrDt AS LYR_FISCAL_YR_DT,
  PyrWeekDt AS PYR_WEEK_DT,
  PyrFiscalWk AS PYR_FISCAL_WK,
  PyrFiscalMoDt AS PYR_FISCAL_MO_DT,
  PyrFiscalQtrDt AS PYR_FISCAL_QTR_DT,
  PyrFiscalYrDt AS PYR_FISCAL_YR_DT
FROM
  SQ_Shortcut_to_LyrWeeks_1""")

# COMMAND ----------
# DBTITLE 1, LYR_WEEKS


spark.sql("""INSERT INTO
  LYR_WEEKS
SELECT
  WeekDt AS WEEK_DT,
  CalHalf AS CAL_HALF,
  CalMo AS CAL_MO,
  CalMoName AS CAL_MO_NAME,
  CalMoNameAbbr AS CAL_MO_NAME_ABBR,
  CalMoNbr AS CAL_MO_NBR,
  CalQtr AS CAL_QTR,
  CalQtrNbr AS CAL_QTR_NBR,
  CalWk AS CAL_WK,
  CalYr AS CAL_YR,
  FiscalHalf AS FISCAL_HALF,
  FiscalMoName AS FISCAL_MO_NAME,
  FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
  FiscalMoNbr AS FISCAL_MO_NBR,
  FiscalQtr AS FISCAL_QTR,
  FiscalQtrNbr AS FISCAL_QTR_NBR,
  FiscalWk AS FISCAL_WK,
  FiscalYr AS FISCAL_YR,
  LwkWeekDt AS LWK_WEEK_DT,
  FiscalMo AS FISCAL_MO,
  FiscalMoDt AS FISCAL_MO_DT,
  FiscalQtrDt AS FISCAL_QTR_DT,
  FiscalYrDt AS FISCAL_YR_DT,
  LyrWeekDt AS LYR_WEEK_DT,
  LyrFiscalWk AS LYR_FISCAL_WK,
  LyrFiscalMoDt AS LYR_FISCAL_MO_DT,
  LyrFiscalQtrDt AS LYR_FISCAL_QTR_DT,
  LyrFiscalYrDt AS LYR_FISCAL_YR_DT,
  PyrWeekDt AS PYR_WEEK_DT,
  PyrFiscalWk AS PYR_FISCAL_WK,
  PyrFiscalMoDt AS PYR_FISCAL_MO_DT,
  PyrFiscalQtrDt AS PYR_FISCAL_QTR_DT,
  PyrFiscalYrDt AS PYR_FISCAL_YR_DT
FROM
  SQ_Shortcut_to_LyrWeeks_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_lyr_weeks_EDW_EDH")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_lyr_weeks_EDW_EDH", mainWorkflowId, parentName)
