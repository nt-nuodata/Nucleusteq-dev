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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_earned_labor_CA_HOTEL")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_earned_labor_CA_HOTEL", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_CA_HTL_EARN_HRS_PRE_0


query_0 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  PAY_WEEK_DT AS PAY_WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  STATE_CD AS STATE_CD,
  BUSINESS_TYPE AS BUSINESS_TYPE,
  TTL_HOTEL_SALES_AMT AS TTL_HOTEL_SALES_AMT,
  EARNED_HRS AS EARNED_HRS
FROM
  PS2_CA_HTL_EARN_HRS_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PS2_CA_HTL_EARN_HRS_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_CA_HTL_EARN_WAGE_PRE_1


query_1 = f"""SELECT
  PAY_WEEK_DT AS PAY_WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BUSINESS_TYPE AS BUSINESS_TYPE,
  TTL_HRS AS TTL_HRS,
  TTL_AMT AS TTL_AMT
FROM
  PS2_CA_HTL_EARN_WAGE_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PS2_CA_HTL_EARN_WAGE_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CURRENCY_DAY_2


query_2 = f"""SELECT
  DAY_DT AS DAY_DT,
  CURRENCY_ID AS CURRENCY_ID,
  DATE_RATE_START AS DATE_RATE_START,
  CURRENCY_TYPE AS CURRENCY_TYPE,
  DATE_RATE_ENDED AS DATE_RATE_ENDED,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  RATIO_TO AS RATIO_TO,
  RATIO_FROM AS RATIO_FROM,
  STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
  CURRENCY_NBR AS CURRENCY_NBR
FROM
  CURRENCY_DAY"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_CURRENCY_DAY_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DAYS_3


query_3 = f"""SELECT
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

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_DAYS_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PS2_CA_HTL_EARN_WAGE_PRE_4


query_4 = f"""SELECT
  h.week_dt AS WEEK_DT,
  h.location_id AS LOCATION_ID,
  4 AS WFA_BUSN_AREA_ID,
  CASE
    WHEN w.business_type = 'H' THEN 21
    ELSE 4
  END AS WFA_DEPT_ID,
  CASE
    WHEN w.business_type = 'H' THEN 30
    ELSE 11
  END AS WFA_TASK_ID,
  CASE
    WHEN w.business_type = 'H' THEN 'PetsHotel'
    ELSE 'Day Camp'
  END AS TYPE,
  'C' AS SOURCE,
  0 AS EARNED_OT_HRS,
  0 AS EARNED_LOC_OT_AMT,
  h.earned_hrs AS EARNED_TTL_HRS,
  ROUND(h.earned_hrs * ((NVL(w.ttl_amt, 0)) / ttl_hrs), 2) AS EARNED_LOC_TTL_AMT,
  c.exchange_rate_pcnt AS ACTUAL_EXCH_RATE,
  d.fiscal_wk AS FISCAL_WK,
  d.fiscal_mo AS FISCAL_MO,
  d.fiscal_yr AS FISCAL_YR,
  w.store_nbr AS STORE_NBR,
  'PetsHotel' AS WFA_BUSN_AREA_DESC,
  CASE
    WHEN w.business_type = 'H' THEN 'Pets Hotel Guest Services'
    ELSE 'PetsHotel Day Camp'
  END AS WFA_DEPT_DESC,
  CASE
    WHEN w.business_type = 'H' THEN 'Guest Services'
    ELSE 'Hotel Day Camp'
  END AS WFA_TASK_DESC,
  CURRENT_DATE AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PS2_CA_HTL_EARN_WAGE_PRE_1 w,
  Shortcut_to_PS2_CA_HTL_EARN_HRS_PRE_0 h,
  Shortcut_to_DAYS_3 d,
  Shortcut_to_CURRENCY_DAY_2 c
WHERE
  w.pay_week_dt = h.pay_week_dt
  AND w.location_id = h.location_id
  AND w.pay_week_dt = c.day_dt
  AND c.currency_id = 'CAD'
  AND w.business_type = h.business_type
  AND h.week_dt = d.day_dt
  AND w.ttl_hrs <> 0
ORDER BY
  h.week_dt"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_Shortcut_to_PS2_CA_HTL_EARN_WAGE_PRE_4")

# COMMAND ----------
# DBTITLE 1, PS2_EARNED_LABOR


spark.sql("""INSERT INTO
  PS2_EARNED_LABOR
SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  WFA_DEPT_ID AS WFA_DEPT_ID,
  WFA_TASK_ID AS WFA_TASK_ID,
  TYPE AS TYPE,
  SOURCE AS SOURCE,
  EARNED_OT_HRS AS EARNED_OT_HRS,
  EARNED_LOC_OT_AMT AS EARNED_LOC_OT_AMT,
  EARNED_TTL_HRS AS EARNED_TTL_HRS,
  EARNED_LOC_TTL_AMT AS EARNED_LOC_TTL_AMT,
  ACTUAL_EXCH_RATE AS ACTUAL_EXCH_RATE,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_YR AS FISCAL_YR,
  STORE_NBR AS STORE_NBR,
  WFA_BUSN_AREA_DESC AS WFA_BUSN_AREA_DESC,
  WFA_DEPT_DESC AS WFA_DEPT_DESC,
  WFA_TASK_DESC AS WFA_TASK_DESC,
  LOAD_DT AS LOAD_DT
FROM
  SQ_Shortcut_to_PS2_CA_HTL_EARN_WAGE_PRE_4""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_earned_labor_CA_HOTEL")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_earned_labor_CA_HOTEL", mainWorkflowId, parentName)
