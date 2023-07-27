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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Last_CALENDER_DATE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Last_CALENDER_DATE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CALENDAR_0


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
  CALENDAR"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_CALENDAR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_CALENDAR_1


query_1 = f"""SELECT
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
  PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_CALENDAR_0
WHERE
  DAY_DT = CURRENT_DATE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_CALENDAR_1")

# COMMAND ----------
# DBTITLE 1, EXP_LAST_RUN_DT_2


query_2 = f"""SELECT
  SETVARIABLE(
    'Last_Run_Dt',
    date_trunc('DAY', current_timestamp())
  ) AS SET_LAST_RUN_DT,
  DAY_DT AS DAY_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_CALENDAR_1"""

df_2 = spark.sql(query_2)

if df_2.count() > 0 :
  Last_Run_Dt = df_2.agg({'SET_LAST_RUN_DT' : 'max'}).collect()[0][0]

df_2.createOrReplaceTempView("EXP_LAST_RUN_DT_2")

# COMMAND ----------
# DBTITLE 1, FIL_FALSE_3


query_3 = f"""SELECT
  DAY_DT AS DAY_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_LAST_RUN_DT_2
WHERE
  FALSE"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("FIL_FALSE_3")

# COMMAND ----------
# DBTITLE 1, TEST_COURT


spark.sql("""INSERT INTO
  TEST_COURT
SELECT
  NULL AS LOCATION_ID,
  NULL AS WM_YARD_ID,
  NULL AS WM_TC_COMPANY_ID,
  NULL AS WM_YARD_NAME,
  NULL AS WM_LOCATION_ID,
  NULL AS WM_TIME_ZONE_ID,
  NULL AS GENERATE_MOVE_TASK_FLAG,
  NULL AS GENERATE_NEXT_EQUIP_FLAG,
  NULL AS RANGE_TASKS_FLAG,
  NULL AS SEAL_TASK_TRGD_FLAG,
  NULL AS OVERRIDE_SYSTEM_TASKS_FLAG,
  NULL AS TASKING_ALLOWED_FLAG,
  NULL AS LOCK_TRAILER_ON_MOVE_TO_DOOR_FLAG,
  NULL AS YARD_SVG_FILE,
  NULL AS ADDRESS,
  NULL AS CITY,
  NULL AS STATE_PROV,
  NULL AS POSTAL_CD,
  NULL AS COUNTY,
  NULL AS COUNTRY_CD,
  NULL AS MAX_EQUIPMENT_ALLOWED,
  NULL AS UPPER_CHECK_IN_TIME_MINS,
  NULL AS LOWER_CHECK_IN_TIME_MINS,
  NULL AS FIXED_TIME_MINS,
  NULL AS THRESHOLD_PERCENT,
  NULL AS MARK_FOR_DELETION,
  NULL AS WM_CREATED_SOURCE_TYPE,
  NULL AS WM_CREATED_SOURCE,
  NULL AS WM_CREATED_TSTMP,
  NULL AS WM_LAST_UPDATED_SOURCE_TYPE,
  NULL AS WM_LAST_UPDATED_SOURCE,
  NULL AS WM_LAST_UPDATED_TSTMP,
  NULL AS UPDATE_TSTMP,
  DAY_DT AS LOAD_TSTMP
FROM
  FIL_FALSE_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Last_CALENDER_DATE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Last_CALENDER_DATE", mainWorkflowId, parentName)
