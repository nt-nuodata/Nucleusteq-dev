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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_forecast_DELETE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_htl_forecast_DELETE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_HTL_ETL_CONTROL_0


query_0 = f"""SELECT
  PS2_HTL_PROCESS_ID AS PS2_HTL_PROCESS_ID,
  PS2_HTL_PROCESS_DESC AS PS2_HTL_PROCESS_DESC,
  PS2_HTL_RUN_DT AS PS2_HTL_RUN_DT,
  UPDATE_TSTMP AS UPDATE_TSTMP
FROM
  PS2_HTL_ETL_CONTROL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PS2_HTL_ETL_CONTROL_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_HTL_FORECAST_1


query_1 = f"""SELECT
  FORECAST_DAY_DT AS FORECAST_DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  WEEK_DT AS WEEK_DT,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_YR AS FISCAL_YR,
  OVERNIGHT_GUEST_CNT AS OVERNIGHT_GUEST_CNT,
  OVERNIGHT_WITH_DDC_CNT AS OVERNIGHT_WITH_DDC_CNT,
  DAY_GUEST_CNT AS DAY_GUEST_CNT,
  DAY_CARE_CNT AS DAY_CARE_CNT,
  DAY_CAMP_CNT AS DAY_CAMP_CNT,
  TOTAL_DDC_GUEST_CNT AS TOTAL_DDC_GUEST_CNT,
  TOTAL_GUEST_CNT AS TOTAL_GUEST_CNT,
  REQUIRED_PLAYROOM_CNT AS REQUIRED_PLAYROOM_CNT,
  FRONT_DESK_HRS AS FRONT_DESK_HRS,
  PLAYROOM_HRS AS PLAYROOM_HRS,
  BACK_OF_HOUSE_HRS AS BACK_OF_HOUSE_HRS,
  OVERNIGHT_HRS AS OVERNIGHT_HRS,
  SUPERVISOR_HRS AS SUPERVISOR_HRS,
  FORECAST_HRS AS FORECAST_HRS,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  PS2_HTL_FORECAST"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PS2_HTL_FORECAST_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PS2_HTL_FORECAST_2


query_2 = f"""SELECT
  Shortcut_to_PS2_HTL_ETL_CONTROL_0.PS2_HTL_RUN_DT AS PROCESS_DT,
  Shortcut_to_PS2_HTL_FORECAST_1.FORECAST_DAY_DT AS FORECAST_DAY_DT,
  Shortcut_to_PS2_HTL_FORECAST_1.LOCATION_ID AS LOCATION_ID,
  Shortcut_to_PS2_HTL_FORECAST_1.WEEK_DT AS WEEK_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PS2_HTL_FORECAST_1,
  Shortcut_to_PS2_HTL_ETL_CONTROL_0
WHERE
  Shortcut_to_PS2_HTL_ETL_CONTROL_0.PS2_HTL_PROCESS_ID = 1
  AND Shortcut_to_PS2_HTL_FORECAST_1.WEEK_DT = Shortcut_to_PS2_HTL_ETL_CONTROL_0.PS2_HTL_RUN_DT + 20"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_PS2_HTL_FORECAST_2")

# COMMAND ----------
# DBTITLE 1, UPD_DeleteOnly_3


query_3 = f"""SELECT
  FORECAST_DAY_DT AS FORECAST_DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PS2_HTL_FORECAST_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_DeleteOnly_3")

# COMMAND ----------
# DBTITLE 1, PS2_HTL_FORECAST


spark.sql("""MERGE INTO PS2_HTL_FORECAST AS TARGET
USING
  UPD_DeleteOnly_3 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  AND TARGET.FORECAST_DAY_DT = SOURCE.FORECAST_DAY_DT
  WHEN MATCHED THEN DELETE""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_forecast_DELETE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_htl_forecast_DELETE", mainWorkflowId, parentName)
