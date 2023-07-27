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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_days_EDW_EDH")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_days_EDW_EDH", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Days_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
  HOLIDAY_FLAG AS HOLIDAY_FLAG,
  DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
  DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
  CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
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
  FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
  FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
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
  LWK_WEEK_DT AS LWK_WEEK_DT,
  WEEK_DT AS WEEK_DT,
  EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
  EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
  ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
  ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
  CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
  CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
  CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
  CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
  MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
  MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
  MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
  MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
  PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
  PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS
FROM
  DAYS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_Days_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Days_1


query_1 = f"""SELECT
  DayDt AS DayDt,
  BusinessDayFlag AS BusinessDayFlag,
  HolidayFlag AS HolidayFlag,
  DayOfWkName AS DayOfWkName,
  DayOfWkNameAbbr AS DayOfWkNameAbbr,
  DayOfWkNbr AS DayOfWkNbr,
  CalDayOfMoNbr AS CalDayOfMoNbr,
  CalDayOfYrNbr AS CalDayOfYrNbr,
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
  FiscalDayOfMoNbr AS FiscalDayOfMoNbr,
  FiscalDayOfYrNbr AS FiscalDayOfYrNbr,
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
  WeekDt AS WeekDt,
  EstTimeConvAmt AS EstTimeConvAmt,
  EstTimeConvHrs AS EstTimeConvHrs,
  Es0TimeConvAmt AS Es0TimeConvAmt,
  Es0TimeConvHrs AS Es0TimeConvHrs,
  CstTimeConvAmt AS CstTimeConvAmt,
  CstTimeConvHrs AS CstTimeConvHrs,
  Cs0TimeConvAmt AS Cs0TimeConvAmt,
  Cs0TimeConvHrs AS Cs0TimeConvHrs,
  MstTimeConvAmt AS MstTimeConvAmt,
  MstTimeConvHrs AS MstTimeConvHrs,
  Ms0TimeConvAmt AS Ms0TimeConvAmt,
  Ms0TimeConvHrs AS Ms0TimeConvHrs,
  PstTimeConvAmt AS PstTimeConvAmt,
  PstTimeConvHrs AS PstTimeConvHrs,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Days
WHERE
  FiscalYr = DATEPART(YY, GETDATE()) + 2"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_Days_1")

# COMMAND ----------
# DBTITLE 1, DAYS


spark.sql("""INSERT INTO
  DAYS
SELECT
  DayDt AS DAY_DT,
  BusinessDayFlag AS BUSINESS_DAY_FLAG,
  HolidayFlag AS HOLIDAY_FLAG,
  DayOfWkName AS DAY_OF_WK_NAME,
  DayOfWkNameAbbr AS DAY_OF_WK_NAME_ABBR,
  DayOfWkNbr AS DAY_OF_WK_NBR,
  CalDayOfMoNbr AS CAL_DAY_OF_MO_NBR,
  CalDayOfYrNbr AS CAL_DAY_OF_YR_NBR,
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
  FiscalDayOfMoNbr AS FISCAL_DAY_OF_MO_NBR,
  FiscalDayOfYrNbr AS FISCAL_DAY_OF_YR_NBR,
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
  LwkWeekDt AS LWK_WEEK_DT,
  WeekDt AS WEEK_DT,
  EstTimeConvAmt AS EST_TIME_CONV_AMT,
  EstTimeConvHrs AS EST_TIME_CONV_HRS,
  Es0TimeConvAmt AS ES0_TIME_CONV_AMT,
  Es0TimeConvHrs AS ES0_TIME_CONV_HRS,
  CstTimeConvAmt AS CST_TIME_CONV_AMT,
  CstTimeConvHrs AS CST_TIME_CONV_HRS,
  Cs0TimeConvAmt AS CS0_TIME_CONV_AMT,
  Cs0TimeConvHrs AS CS0_TIME_CONV_HRS,
  MstTimeConvAmt AS MST_TIME_CONV_AMT,
  MstTimeConvHrs AS MST_TIME_CONV_HRS,
  Ms0TimeConvAmt AS MS0_TIME_CONV_AMT,
  Ms0TimeConvHrs AS MS0_TIME_CONV_HRS,
  PstTimeConvAmt AS PST_TIME_CONV_AMT,
  PstTimeConvHrs AS PST_TIME_CONV_HRS
FROM
  SQ_Shortcut_to_Days_1""")

# COMMAND ----------
# DBTITLE 1, DAYS


spark.sql("""INSERT INTO
  DAYS
SELECT
  DayDt AS DAY_DT,
  BusinessDayFlag AS BUSINESS_DAY_FLAG,
  HolidayFlag AS HOLIDAY_FLAG,
  DayOfWkName AS DAY_OF_WK_NAME,
  DayOfWkNameAbbr AS DAY_OF_WK_NAME_ABBR,
  DayOfWkNbr AS DAY_OF_WK_NBR,
  CalDayOfMoNbr AS CAL_DAY_OF_MO_NBR,
  CalDayOfYrNbr AS CAL_DAY_OF_YR_NBR,
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
  FiscalDayOfMoNbr AS FISCAL_DAY_OF_MO_NBR,
  FiscalDayOfYrNbr AS FISCAL_DAY_OF_YR_NBR,
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
  LwkWeekDt AS LWK_WEEK_DT,
  WeekDt AS WEEK_DT,
  EstTimeConvAmt AS EST_TIME_CONV_AMT,
  EstTimeConvHrs AS EST_TIME_CONV_HRS,
  Es0TimeConvAmt AS ES0_TIME_CONV_AMT,
  Es0TimeConvHrs AS ES0_TIME_CONV_HRS,
  CstTimeConvAmt AS CST_TIME_CONV_AMT,
  CstTimeConvHrs AS CST_TIME_CONV_HRS,
  Cs0TimeConvAmt AS CS0_TIME_CONV_AMT,
  Cs0TimeConvHrs AS CS0_TIME_CONV_HRS,
  MstTimeConvAmt AS MST_TIME_CONV_AMT,
  MstTimeConvHrs AS MST_TIME_CONV_HRS,
  Ms0TimeConvAmt AS MS0_TIME_CONV_AMT,
  Ms0TimeConvHrs AS MS0_TIME_CONV_HRS,
  PstTimeConvAmt AS PST_TIME_CONV_AMT,
  PstTimeConvHrs AS PST_TIME_CONV_HRS
FROM
  SQ_Shortcut_to_Days_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_days_EDW_EDH")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_days_EDW_EDH", mainWorkflowId, parentName)
