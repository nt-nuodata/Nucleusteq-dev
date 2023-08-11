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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_earned_pre_UPDATE1")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_htl_earned_pre_UPDATE1", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_HTL_EARNED_PRE_0


query_0 = f"""SELECT
  EARN_DAY_DT AS EARN_DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  WEEK_DT AS WEEK_DT,
  OVERNIGHT_KITTY_GUEST_CNT AS OVERNIGHT_KITTY_GUEST_CNT,
  OVERNIGHT_DOG_GUEST_CNT AS OVERNIGHT_DOG_GUEST_CNT,
  OVERNIGHT_TOTAL_GUEST_CNT AS OVERNIGHT_TOTAL_GUEST_CNT,
  DAY_CAMP_PLAYROOM_CNT AS DAY_CAMP_PLAYROOM_CNT,
  DAY_CARE_CNT AS DAY_CARE_CNT,
  OVERNIGHT_WITH_DDC_CNT AS OVERNIGHT_WITH_DDC_CNT,
  TOTAL_DAYCAMP_CNT AS TOTAL_DAYCAMP_CNT,
  TOTAL_GUEST_CNT AS TOTAL_GUEST_CNT,
  NIGHT_PETCARE_SPECIALIST_HRS AS NIGHT_PETCARE_SPECIALIST_HRS,
  FRONT_DESK_HRS AS FRONT_DESK_HRS,
  BACK_OF_HOUSE_HRS AS BACK_OF_HOUSE_HRS,
  SUPERVISOR_HRS AS SUPERVISOR_HRS,
  PLAYROOM_HRS AS PLAYROOM_HRS
FROM
  PS2_HTL_EARNED_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PS2_HTL_EARNED_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_TP_SERVICE_SCHEDULE_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  TP_INVOICE_NBR AS TP_INVOICE_NBR,
  UPC_ID AS UPC_ID,
  SERVICE_ITEM_ID AS SERVICE_ITEM_ID,
  FOLIO_STATUS_FLAG AS FOLIO_STATUS_FLAG,
  PRODUCT_ID AS PRODUCT_ID,
  ROOM_NUMBER AS ROOM_NUMBER,
  ROOM_TYPE_ID AS ROOM_TYPE_ID,
  SERVICE_SCHEDULE_QTY AS SERVICE_SCHEDULE_QTY,
  SERVICE_FREQ_ID AS SERVICE_FREQ_ID,
  LOAD_DT AS LOAD_DT
FROM
  TP_SERVICE_SCHEDULE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_TP_SERVICE_SCHEDULE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_DAYS_2


query_2 = f"""SELECT
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

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_To_DAYS_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TP_SERVICE_SCHEDULE_3


query_3 = f"""SELECT
  Shortcut_to_TP_SERVICE_SCHEDULE_1.DAY_DT AS DAY_DT,
  Shortcut_to_TP_SERVICE_SCHEDULE_1.LOCATION_ID AS LOCATION_ID,
  Shortcut_to_TP_SERVICE_SCHEDULE_1.TP_INVOICE_NBR AS TP_INVOICE_NBR,
  Shortcut_To_DAYS_2.DAY_DT AS M_DAY_DT,
  Shortcut_to_PS2_HTL_EARNED_PRE_0.EARN_DAY_DT AS EARN_DAY_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PS2_HTL_EARNED_PRE_0,
  Shortcut_to_TP_SERVICE_SCHEDULE_1,
  Shortcut_To_DAYS_2
WHERE
  Shortcut_To_DAYS_2.DAY_DT = Shortcut_to_TP_SERVICE_SCHEDULE_1.DAY_DT
  AND Shortcut_to_PS2_HTL_EARNED_PRE_0.EARN_DAY_DT = Shortcut_to_TP_SERVICE_SCHEDULE_1.DAY_DT
  AND Shortcut_to_PS2_HTL_EARNED_PRE_0.LOCATION_ID = Shortcut_to_TP_SERVICE_SCHEDULE_1.LOCATION_ID
  AND Shortcut_to_TP_SERVICE_SCHEDULE_1.FOLIO_STATUS_FLAG IN ('A', 'I', 'O')
  AND Shortcut_to_TP_SERVICE_SCHEDULE_1.ROOM_TYPE_ID IN (2, 3, 4, 5, 8, 9, 10, 11, 12, 13, 14, 15, 16)
  AND Shortcut_To_DAYS_2.FISCAL_YR >= 2012
  AND Shortcut_to_TP_SERVICE_SCHEDULE_1.PRODUCT_ID IN (
    131886,
    226352,
    226353,
    227536,
    278067,
    278066,
    278064,
    278065
  )
  AND Shortcut_to_TP_SERVICE_SCHEDULE_1.DAY_DT < current_date"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_TP_SERVICE_SCHEDULE_3")

# COMMAND ----------
# DBTITLE 1, AGG_CountGuests_4


query_4 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  Count(TP_INVOICE_NBR) AS Overnight_with_DDC,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_TP_SERVICE_SCHEDULE_3
GROUP BY
  DAY_DT,
  LOCATION_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("AGG_CountGuests_4")

# COMMAND ----------
# DBTITLE 1, UPD_OnlyUpdate_5


query_5 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  Overnight_with_DDC AS Overnight_with_DDC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  AGG_CountGuests_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("UPD_OnlyUpdate_5")

# COMMAND ----------
# DBTITLE 1, PS2_HTL_EARNED_PRE


spark.sql("""MERGE INTO PS2_HTL_EARNED_PRE AS TARGET
USING
  UPD_OnlyUpdate_5 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  AND TARGET.EARN_DAY_DT = SOURCE.DAY_DT
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.EARN_DAY_DT = SOURCE.DAY_DT,
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID,
  TARGET.OVERNIGHT_WITH_DDC_CNT = SOURCE.Overnight_with_DDC""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_earned_pre_UPDATE1")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_htl_earned_pre_UPDATE1", mainWorkflowId, parentName)
