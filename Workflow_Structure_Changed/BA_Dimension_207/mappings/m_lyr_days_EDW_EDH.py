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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_lyr_days_EDW_EDH")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_lyr_days_EDW_EDH", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_LyrDays_0


query_0 = f"""SELECT
  DayDt AS DayDt,
  BusinessDayFlag AS BusinessDayFlag,
  CalDayOfMoNbr AS CalDayOfMoNbr,
  CalDayOfYrNbr AS CalDayOfYrNbr,
  CalHalf AS CalHalf,
  CalMo AS CalMo,
  CalMoName AS CalMoName,
  CalMoNameAbbr AS CalMoNameAbbr,
  CalMoNbr AS CalMoNbr,
  CalQtr AS CalQtr,
  CalQtrNbr AS CalQtrNbr,
  CalWk AS CalWk,
  CalYr AS CalYr,
  DayOfWkName AS DayOfWkName,
  DayOfWkNameAbbr AS DayOfWkNameAbbr,
  DayOfWkNbr AS DayOfWkNbr,
  FiscalDayOfMoNbr AS FiscalDayOfMoNbr,
  FiscalDayOfYrNbr AS FiscalDayOfYrNbr,
  FiscalHalf AS FiscalHalf,
  FiscalMoName AS FiscalMoName,
  FiscalMoNameAbbr AS FiscalMoNameAbbr,
  FiscalMoNbr AS FiscalMoNbr,
  FiscalQtr AS FiscalQtr,
  FiscalQtrNbr AS FiscalQtrNbr,
  FiscalWk AS FiscalWk,
  HolidayFlag AS HolidayFlag,
  LwkWeekDt AS LwkWeekDt,
  WeekDt AS WeekDt,
  FiscalMoDt AS FiscalMoDt,
  FiscalQtrDt AS FiscalQtrDt,
  FiscalYrDt AS FiscalYrDt,
  LyrDayDt AS LyrDayDt,
  LyrWeekDt AS LyrWeekDt,
  LyrFiscalWk AS LyrFiscalWk,
  LyrFiscalMoDt AS LyrFiscalMoDt,
  LyrFiscalQtrDt AS LyrFiscalQtrDt,
  LyrFiscalYrDt AS LyrFiscalYrDt,
  PyrDayDt AS PyrDayDt,
  PyrWeekDt AS PyrWeekDt,
  PyrFiscalWk AS PyrFiscalWk,
  PyrFiscalMoDt AS PyrFiscalMoDt,
  PyrFiscalQtrDt AS PyrFiscalQtrDt,
  PyrFiscalYrDt AS PyrFiscalYrDt
FROM
  LyrDays"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_LyrDays_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LyrDays_1


query_1 = f"""SELECT
  DayDt AS DayDt,
  BusinessDayFlag AS BusinessDayFlag,
  CalDayOfMoNbr AS CalDayOfMoNbr,
  CalDayOfYrNbr AS CalDayOfYrNbr,
  CalHalf AS CalHalf,
  CalMo AS CalMo,
  CalMoName AS CalMoName,
  CalMoNameAbbr AS CalMoNameAbbr,
  CalMoNbr AS CalMoNbr,
  CalQtr AS CalQtr,
  CalQtrNbr AS CalQtrNbr,
  CalWk AS CalWk,
  CalYr AS CalYr,
  DayOfWkName AS DayOfWkName,
  DayOfWkNameAbbr AS DayOfWkNameAbbr,
  DayOfWkNbr AS DayOfWkNbr,
  FiscalDayOfMoNbr AS FiscalDayOfMoNbr,
  FiscalDayOfYrNbr AS FiscalDayOfYrNbr,
  FiscalHalf AS FiscalHalf,
  FiscalMoName AS FiscalMoName,
  FiscalMoNameAbbr AS FiscalMoNameAbbr,
  FiscalMoNbr AS FiscalMoNbr,
  FiscalQtr AS FiscalQtr,
  FiscalQtrNbr AS FiscalQtrNbr,
  FiscalWk AS FiscalWk,
  HolidayFlag AS HolidayFlag,
  LwkWeekDt AS LwkWeekDt,
  WeekDt AS WeekDt,
  FiscalMoDt AS FiscalMoDt,
  FiscalQtrDt AS FiscalQtrDt,
  FiscalYrDt AS FiscalYrDt,
  LyrDayDt AS LyrDayDt,
  LyrWeekDt AS LyrWeekDt,
  LyrFiscalWk AS LyrFiscalWk,
  LyrFiscalMoDt AS LyrFiscalMoDt,
  LyrFiscalQtrDt AS LyrFiscalQtrDt,
  LyrFiscalYrDt AS LyrFiscalYrDt,
  PyrDayDt AS PyrDayDt,
  PyrWeekDt AS PyrWeekDt,
  PyrFiscalWk AS PyrFiscalWk,
  PyrFiscalMoDt AS PyrFiscalMoDt,
  PyrFiscalQtrDt AS PyrFiscalQtrDt,
  PyrFiscalYrDt AS PyrFiscalYrDt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_LyrDays_0
WHERE
  DATEPART(YY, FiscalYrDt) = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_LyrDays_1")

# COMMAND ----------
# DBTITLE 1, LYR_DAYS


spark.sql("""INSERT INTO
  LYR_DAYS
SELECT
  DayDt AS DAY_DT,
  BusinessDayFlag AS BUSINESS_DAY_FLAG,
  CalDayOfMoNbr AS CAL_DAY_OF_MO_NBR,
  CalDayOfYrNbr AS CAL_DAY_OF_YR_NBR,
  CalHalf AS CAL_HALF,
  CalMo AS CAL_MO,
  CalMoName AS CAL_MO_NAME,
  CalMoNameAbbr AS CAL_MO_NAME_ABBR,
  CalMoNbr AS CAL_MO_NBR,
  CalQtr AS CAL_QTR,
  CalQtrNbr AS CAL_QTR_NBR,
  CalWk AS CAL_WK,
  CalYr AS CAL_YR,
  DayOfWkName AS DAY_OF_WK_NAME,
  DayOfWkNameAbbr AS DAY_OF_WK_NAME_ABBR,
  DayOfWkNbr AS DAY_OF_WK_NBR,
  FiscalDayOfMoNbr AS FISCAL_DAY_OF_MO_NBR,
  FiscalDayOfYrNbr AS FISCAL_DAY_OF_YR_NBR,
  FiscalHalf AS FISCAL_HALF,
  FiscalMoName AS FISCAL_MO_NAME,
  FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
  FiscalMoNbr AS FISCAL_MO_NBR,
  FiscalQtr AS FISCAL_QTR,
  FiscalQtrNbr AS FISCAL_QTR_NBR,
  FiscalWk AS FISCAL_WK,
  HolidayFlag AS HOLIDAY_FLAG,
  LwkWeekDt AS LWK_WEEK_DT,
  WeekDt AS WEEK_DT,
  FiscalMoDt AS FISCAL_MO_DT,
  FiscalQtrDt AS FISCAL_QTR_DT,
  FiscalYrDt AS FISCAL_YR_DT,
  LyrDayDt AS LYR_DAY_DT,
  LyrWeekDt AS LYR_WEEK_DT,
  LyrFiscalWk AS LYR_FISCAL_WK,
  LyrFiscalMoDt AS LYR_FISCAL_MO_DT,
  LyrFiscalQtrDt AS LYR_FISCAL_QTR_DT,
  LyrFiscalYrDt AS LYR_FISCAL_YR_DT,
  PyrDayDt AS PYR_DAY_DT,
  PyrWeekDt AS PYR_WEEK_DT,
  PyrFiscalWk AS PYR_FISCAL_WK,
  PyrFiscalMoDt AS PYR_FISCAL_MO_DT,
  PyrFiscalQtrDt AS PYR_FISCAL_QTR_DT,
  PyrFiscalYrDt AS PYR_FISCAL_YR_DT
FROM
  SQ_Shortcut_to_LyrDays_1""")

# COMMAND ----------
# DBTITLE 1, LYR_DAYS


spark.sql("""INSERT INTO
  LYR_DAYS
SELECT
  DayDt AS DAY_DT,
  BusinessDayFlag AS BUSINESS_DAY_FLAG,
  CalDayOfMoNbr AS CAL_DAY_OF_MO_NBR,
  CalDayOfYrNbr AS CAL_DAY_OF_YR_NBR,
  CalHalf AS CAL_HALF,
  CalMo AS CAL_MO,
  CalMoName AS CAL_MO_NAME,
  CalMoNameAbbr AS CAL_MO_NAME_ABBR,
  CalMoNbr AS CAL_MO_NBR,
  CalQtr AS CAL_QTR,
  CalQtrNbr AS CAL_QTR_NBR,
  CalWk AS CAL_WK,
  CalYr AS CAL_YR,
  DayOfWkName AS DAY_OF_WK_NAME,
  DayOfWkNameAbbr AS DAY_OF_WK_NAME_ABBR,
  DayOfWkNbr AS DAY_OF_WK_NBR,
  FiscalDayOfMoNbr AS FISCAL_DAY_OF_MO_NBR,
  FiscalDayOfYrNbr AS FISCAL_DAY_OF_YR_NBR,
  FiscalHalf AS FISCAL_HALF,
  FiscalMoName AS FISCAL_MO_NAME,
  FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
  FiscalMoNbr AS FISCAL_MO_NBR,
  FiscalQtr AS FISCAL_QTR,
  FiscalQtrNbr AS FISCAL_QTR_NBR,
  FiscalWk AS FISCAL_WK,
  HolidayFlag AS HOLIDAY_FLAG,
  LwkWeekDt AS LWK_WEEK_DT,
  WeekDt AS WEEK_DT,
  FiscalMoDt AS FISCAL_MO_DT,
  FiscalQtrDt AS FISCAL_QTR_DT,
  FiscalYrDt AS FISCAL_YR_DT,
  LyrDayDt AS LYR_DAY_DT,
  LyrWeekDt AS LYR_WEEK_DT,
  LyrFiscalWk AS LYR_FISCAL_WK,
  LyrFiscalMoDt AS LYR_FISCAL_MO_DT,
  LyrFiscalQtrDt AS LYR_FISCAL_QTR_DT,
  LyrFiscalYrDt AS LYR_FISCAL_YR_DT,
  PyrDayDt AS PYR_DAY_DT,
  PyrWeekDt AS PYR_WEEK_DT,
  PyrFiscalWk AS PYR_FISCAL_WK,
  PyrFiscalMoDt AS PYR_FISCAL_MO_DT,
  PyrFiscalQtrDt AS PYR_FISCAL_QTR_DT,
  PyrFiscalYrDt AS PYR_FISCAL_YR_DT
FROM
  SQ_Shortcut_to_LyrDays_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_lyr_days_EDW_EDH")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_lyr_days_EDW_EDH", mainWorkflowId, parentName)
