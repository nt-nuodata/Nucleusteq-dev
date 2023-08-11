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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_etl_control_FORECAST")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_htl_etl_control_FORECAST", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DAYS_0


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

df_0.createOrReplaceTempView("Shortcut_to_DAYS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DAYS_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_DAYS_0
WHERE
  Shortcut_to_DAYS_0.DAY_DT = CURRENT_DATE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_DAYS_1")

# COMMAND ----------
# DBTITLE 1, EXP_PS2_HTL_RUN_DT_2


query_2 = f"""SELECT
  1 AS PS2_HTL_PROCESS_ID,
  IFF (
    DAY_OF_WK_NBR = 1,
    DAY_DT,
    ADD_TO_DATE(DAY_DT, 'DD', 1 - DAY_OF_WK_NBR)
  ) AS PS2_HTL_RUN_DT,
  now() AS UPDATE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_DAYS_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_PS2_HTL_RUN_DT_2")

# COMMAND ----------
# DBTITLE 1, UPD_FORECAST_CONTROL_3


query_3 = f"""SELECT
  PS2_HTL_PROCESS_ID AS PS2_HTL_PROCESS_ID,
  PS2_HTL_RUN_DT AS PS2_HTL_RUN_DT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_PS2_HTL_RUN_DT_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_FORECAST_CONTROL_3")

# COMMAND ----------
# DBTITLE 1, PS2_HTL_ETL_CONTROL


spark.sql("""MERGE INTO PS2_HTL_ETL_CONTROL AS TARGET
USING
  UPD_FORECAST_CONTROL_3 AS SOURCE ON TARGET.PS2_HTL_PROCESS_ID = SOURCE.PS2_HTL_PROCESS_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.PS2_HTL_PROCESS_ID = SOURCE.PS2_HTL_PROCESS_ID,
  TARGET.PS2_HTL_RUN_DT = SOURCE.PS2_HTL_RUN_DT,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_etl_control_FORECAST")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_htl_etl_control_FORECAST", mainWorkflowId, parentName)
