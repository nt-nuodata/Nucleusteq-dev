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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_days")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_days", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Days_Pre2_0


query_0 = f"""SELECT
  DayDt AS DayDt,
  BusinessDayFlag AS BusinessDayFlag,
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
  LyrWeekDt AS LyrWeekDt,
  LwkWeekDt AS LwkWeekDt,
  WeekDt AS WeekDt
FROM
  Days_Pre2"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Days_Pre2_0")

# COMMAND ----------
# DBTITLE 1, SQ_Days_Pre2_1


query_1 = f"""SELECT
  DISTINCT DayDt AS DayDt,
  BusinessDayFlag AS BusinessDayFlag,
  CASE
    WHEN CALMONBR = 11
    AND DAYOFWKNBR = 4
    AND DENSE_RANK() OVER (
      PARTITION BY
        CALMO,
        DAYOFWKNBR
      ORDER BY
        WEEKDT
    ) = 4 THEN 'Y'
    WHEN CALDAYOFMONBR = 25
    AND CALMONBR = 12
    AND (
      DAYOFWKNBR BETWEEN 1 AND 5
    ) THEN 'Y'
    WHEN CALDAYOFMONBR = 24
    AND CALMONBR = 12
    AND DAYOFWKNBR = 5 THEN 'Y'
    WHEN CALDAYOFMONBR = 26
    AND CALMONBR = 12
    AND DAYOFWKNBR = 1 THEN 'Y'
    ELSE 'N'
  END AS HolidayFlag,
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
  D.CalYr AS CalYr,
  RANK() OVER (
    PARTITION BY
      FISCALMO
    ORDER BY
      DAYDT
  ) AS FiscalDayOfMoNbr,
  RANK() OVER (
    PARTITION BY
      FISCALYR
    ORDER BY
      DAYDT
  ) AS FiscalDayOfYrNbr,
  CAST(FISCALYR AS VARCHAR (4)) + REPLICATE(
    '0',
    2 - LEN(
      DENSE_RANK() OVER (
        PARTITION BY
          FISCALYR
        ORDER BY
          WEEKDT
      )
    )
  ) + CAST(
    DENSE_RANK() OVER (
      PARTITION BY
        FISCALYR
      ORDER BY
        WEEKDT
    ) AS VARCHAR (2)
  ) AS FiscalWk,
  DENSE_RANK() OVER (
    PARTITION BY
      FISCALYR
    ORDER BY
      WEEKDT
  ) AS FiscalWkNbr,
  FiscalMo AS FiscalMo,
  FiscalMoNbr AS FiscalMoNbr,
  FiscalMoName AS FiscalMoName,
  FiscalMoNameAbbr AS FiscalMoNameAbbr,
  CAST(FISCALYR AS VARCHAR (4)) + CASE
    WHEN FISCALMONBR BETWEEN 1 AND 3 THEN '01'
    WHEN FISCALMONBR BETWEEN 4 AND 6 THEN '02'
    WHEN FISCALMONBR BETWEEN 7 AND 9 THEN '03'
    ELSE '04'
  END AS FiscalQtr,
  CASE
    WHEN FISCALMONBR BETWEEN 1 AND 3 THEN 1
    WHEN FISCALMONBR BETWEEN 4 AND 6 THEN 2
    WHEN FISCALMONBR BETWEEN 7 AND 9 THEN 3
    ELSE 4
  END AS FiscalQtrNbr,
  CAST(FISCALYR AS VARCHAR (4)) + CASE
    WHEN FISCALMONBR BETWEEN 1 AND 6 THEN '01'
    ELSE '02'
  END AS FiscalHalf,
  FiscalYr AS FiscalYr,
  LyrWeekDt AS LyrWeekDt,
  LwkWeekDt AS LwkWeekDt,
  WeekDt AS WeekDt,
  CASE
    WHEN D.DAYDT BETWEEN S.StartDayDt
    AND S.EndDayDt THEN 0.124995
    ELSE 0.083333
  END AS EstTimeConvAmt,
  CASE
    WHEN D.DAYDT BETWEEN S.StartDayDt
    AND S.EndDayDt THEN 3
    ELSE 2
  END AS EstTimeConvHrs,
  0.083333 AS Es0TimeConvAmt,
  2 AS Es0TimeConvHrs,
  CASE
    WHEN D.DAYDT BETWEEN S.StartDayDt
    AND S.EndDayDt THEN 0.083333
    ELSE 0.041665
  END AS CstTimeConvAmt,
  CASE
    WHEN D.DAYDT BETWEEN S.StartDayDt
    AND S.EndDayDt THEN 2
    ELSE 1
  END AS CstTimeConvHrs,
  0.041665 AS Cs0TimeConvAmt,
  1 AS Cs0TimeConvHrs,
  CASE
    WHEN D.DAYDT BETWEEN S.StartDayDt
    AND S.EndDayDt THEN 0.041665
    ELSE 0
  END AS MstTimeConvAmt,
  CASE
    WHEN D.DAYDT BETWEEN S.StartDayDt
    AND S.EndDayDt THEN 1
    ELSE 0
  END AS MstTimeConvHrs,
  0 AS Ms0TimeConvAmt,
  0 AS Ms0TimeConvHrs,
  CASE
    WHEN D.DAYDT BETWEEN S.StartDayDt
    AND S.EndDayDt THEN 0
    ELSE -0.041665
  END AS PstTimeConvAmt,
  CASE
    WHEN D.DAYDT BETWEEN S.StartDayDt
    AND S.EndDayDt THEN 0
    ELSE -1
  END AS PstTimeConvHrs,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  dbo.Days_Pre2_0 D,
  dbo.FiscalPeriod F,
  dbo.DAYLIGHT_SAVING_TIME S
WHERE
  D.WEEKDT BETWEEN F.StartWkDt
  AND F.EndWkDt
  AND D.CALYR = S.CalYr
ORDER BY
  DAYDT"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Days_Pre2_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


query_2 = f"""SELECT
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
  to_char(ADD_TO_DATE (sysdate, 'YY', {year}), 'YYYY') AS o_FiscalYear,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Days_Pre2_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, FILTRANS_3


query_3 = f"""SELECT
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
  o_FiscalYear AS o_FiscalYear,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXPTRANS_2
WHERE
  FiscalYr = o_FiscalYear"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("FILTRANS_3")

# COMMAND ----------
# DBTITLE 1, DAYS


spark.sql("""INSERT INTO
  DAYS
SELECT
  NULL AS DAY_DT,
  NULL AS BUSINESS_DAY_FLAG,
  NULL AS HOLIDAY_FLAG,
  NULL AS DAY_OF_WK_NAME,
  NULL AS DAY_OF_WK_NAME_ABBR,
  NULL AS DAY_OF_WK_NBR,
  NULL AS CAL_DAY_OF_MO_NBR,
  NULL AS CAL_DAY_OF_YR_NBR,
  NULL AS CAL_WK,
  NULL AS CAL_WK_NBR,
  NULL AS CAL_MO,
  NULL AS CAL_MO_NBR,
  NULL AS CAL_MO_NAME,
  NULL AS CAL_MO_NAME_ABBR,
  NULL AS CAL_QTR,
  NULL AS CAL_QTR_NBR,
  NULL AS CAL_HALF,
  NULL AS CAL_YR,
  NULL AS FISCAL_DAY_OF_MO_NBR,
  NULL AS FISCAL_DAY_OF_YR_NBR,
  NULL AS FISCAL_WK,
  NULL AS FISCAL_WK_NBR,
  NULL AS FISCAL_MO,
  NULL AS FISCAL_MO_NBR,
  NULL AS FISCAL_MO_NAME,
  NULL AS FISCAL_MO_NAME_ABBR,
  NULL AS FISCAL_QTR,
  NULL AS FISCAL_QTR_NBR,
  NULL AS FISCAL_HALF,
  NULL AS FISCAL_YR,
  NULL AS LYR_WEEK_DT,
  NULL AS LWK_WEEK_DT,
  NULL AS WEEK_DT,
  NULL AS EST_TIME_CONV_AMT,
  NULL AS EST_TIME_CONV_HRS,
  NULL AS ES0_TIME_CONV_AMT,
  NULL AS ES0_TIME_CONV_HRS,
  NULL AS CST_TIME_CONV_AMT,
  NULL AS CST_TIME_CONV_HRS,
  NULL AS CS0_TIME_CONV_AMT,
  NULL AS CS0_TIME_CONV_HRS,
  NULL AS MST_TIME_CONV_AMT,
  NULL AS MST_TIME_CONV_HRS,
  NULL AS MS0_TIME_CONV_AMT,
  NULL AS MS0_TIME_CONV_HRS,
  NULL AS PST_TIME_CONV_AMT,
  NULL AS PST_TIME_CONV_HRS
FROM
  FILTRANS_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_days")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_days", mainWorkflowId, parentName)
