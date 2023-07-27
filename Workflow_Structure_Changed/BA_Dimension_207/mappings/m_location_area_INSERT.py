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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_location_area_INSERT")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_location_area_INSERT", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_STORE_AREA_PRE_0


query_0 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  LOC_AREA_EFF_DT AS LOC_AREA_EFF_DT,
  AREA_1_SQ_FT_AMT AS AREA_1_SQ_FT_AMT,
  AREA_2_SQ_FT_AMT AS AREA_2_SQ_FT_AMT,
  AREA_3_SQ_FT_AMT AS AREA_3_SQ_FT_AMT,
  AREA_4_SQ_FT_AMT AS AREA_4_SQ_FT_AMT,
  AREA_5_SQ_FT_AMT AS AREA_5_SQ_FT_AMT,
  AREA_6_SQ_FT_AMT AS AREA_6_SQ_FT_AMT,
  AREA_7_SQ_FT_AMT AS AREA_7_SQ_FT_AMT,
  AREA_8_SQ_FT_AMT AS AREA_8_SQ_FT_AMT,
  AREA_9_SQ_FT_AMT AS AREA_9_SQ_FT_AMT,
  AREA_10_SQ_FT_AMT AS AREA_10_SQ_FT_AMT,
  AREA_11_SQ_FT_AMT AS AREA_11_SQ_FT_AMT,
  AREA_12_SQ_FT_AMT AS AREA_12_SQ_FT_AMT,
  AREA_13_SQ_FT_AMT AS AREA_13_SQ_FT_AMT,
  AREA_14_SQ_FT_AMT AS AREA_14_SQ_FT_AMT,
  AREA_15_SQ_FT_AMT AS AREA_15_SQ_FT_AMT,
  AREA_16_SQ_FT_AMT AS AREA_16_SQ_FT_AMT,
  AREA_17_SQ_FT_AMT AS AREA_17_SQ_FT_AMT,
  AREA_18_SQ_FT_AMT AS AREA_18_SQ_FT_AMT,
  AREA_19_SQ_FT_AMT AS AREA_19_SQ_FT_AMT,
  AREA_20_SQ_FT_AMT AS AREA_20_SQ_FT_AMT,
  AREA_21_SQ_FT_AMT AS AREA_21_SQ_FT_AMT,
  AREA_22_SQ_FT_AMT AS AREA_22_SQ_FT_AMT,
  AREA_23_SQ_FT_AMT AS AREA_23_SQ_FT_AMT,
  AREA_24_SQ_FT_AMT AS AREA_24_SQ_FT_AMT,
  AREA_25_SQ_FT_AMT AS AREA_25_SQ_FT_AMT,
  AREA_26_SQ_FT_AMT AS AREA_26_SQ_FT_AMT,
  AREA_27_SQ_FT_AMT AS AREA_27_SQ_FT_AMT,
  AREA_28_SQ_FT_AMT AS AREA_28_SQ_FT_AMT,
  AREA_29_SQ_FT_AMT AS AREA_29_SQ_FT_AMT,
  AREA_30_SQ_FT_AMT AS AREA_30_SQ_FT_AMT,
  AREA_31_SQ_FT_AMT AS AREA_31_SQ_FT_AMT,
  AREA_32_SQ_FT_AMT AS AREA_32_SQ_FT_AMT,
  AREA_33_SQ_FT_AMT AS AREA_33_SQ_FT_AMT,
  AREA_34_SQ_FT_AMT AS AREA_34_SQ_FT_AMT,
  AREA_35_SQ_FT_AMT AS AREA_35_SQ_FT_AMT
FROM
  STORE_AREA_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_STORE_AREA_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_LOCATION_1


query_1 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  COMPANY_DESC AS COMPANY_DESC,
  COMPANY_ID AS COMPANY_ID,
  DATE_CLOSED AS DATE_CLOSED,
  DATE_OPEN AS DATE_OPEN,
  DATE_LOC_ADDED AS DATE_LOC_ADDED,
  DATE_LOC_DELETED AS DATE_LOC_DELETED,
  DATE_LOC_REFRESHED AS DATE_LOC_REFRESHED,
  DISTRICT_DESC AS DISTRICT_DESC,
  DISTRICT_ID AS DISTRICT_ID,
  PRICE_AD_ZONE_DESC AS PRICE_AD_ZONE_DESC,
  PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
  PRICE_ZONE_DESC AS PRICE_ZONE_DESC,
  PRICE_ZONE_ID AS PRICE_ZONE_ID,
  REGION_DESC AS REGION_DESC,
  REGION_ID AS REGION_ID,
  REPL_DC_NBR AS REPL_DC_NBR,
  REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
  REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
  STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
  STORE_CTRY AS STORE_CTRY,
  STORE_NAME AS STORE_NAME,
  STORE_NBR AS STORE_NBR,
  STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  STORE_STATE_ABBR AS STORE_STATE_ABBR,
  STORE_TYPE_DESC AS STORE_TYPE_DESC,
  STORE_TYPE_ID AS STORE_TYPE_ID,
  EQUINE_MERCH AS EQUINE_MERCH,
  DATE_GR_OPEN AS DATE_GR_OPEN,
  SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  BP_COMPANY_NBR AS BP_COMPANY_NBR,
  BP_GL_ACCT AS BP_GL_ACCT,
  TP_LOC_FLAG AS TP_LOC_FLAG,
  TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
  TP_START_DT AS TP_START_DT,
  SITE_ADDRESS AS SITE_ADDRESS,
  SITE_CITY AS SITE_CITY,
  SITE_POSTAL_CD AS SITE_POSTAL_CD,
  SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
  SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO
FROM
  LOCATION"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_LOCATION_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_STORE_AREA_PRE_2


query_2 = f"""SELECT
  Shortcut_to_STORE_AREA_PRE_0.LOC_AREA_EFF_DT AS LOC_AREA_EFF_DT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_1_SQ_FT_AMT AS AREA_1_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_2_SQ_FT_AMT AS AREA_2_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_3_SQ_FT_AMT AS AREA_3_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_4_SQ_FT_AMT AS AREA_4_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_5_SQ_FT_AMT AS AREA_5_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_6_SQ_FT_AMT AS AREA_6_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_7_SQ_FT_AMT AS AREA_7_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_8_SQ_FT_AMT AS AREA_8_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_9_SQ_FT_AMT AS AREA_9_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_10_SQ_FT_AMT AS AREA_10_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_11_SQ_FT_AMT AS AREA_11_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_12_SQ_FT_AMT AS AREA_12_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_13_SQ_FT_AMT AS AREA_13_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_14_SQ_FT_AMT AS AREA_14_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_15_SQ_FT_AMT AS AREA_15_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_16_SQ_FT_AMT AS AREA_16_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_17_SQ_FT_AMT AS AREA_17_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_18_SQ_FT_AMT AS AREA_18_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_19_SQ_FT_AMT AS AREA_19_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_20_SQ_FT_AMT AS AREA_20_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_21_SQ_FT_AMT AS AREA_21_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_22_SQ_FT_AMT AS AREA_22_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_23_SQ_FT_AMT AS AREA_23_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_24_SQ_FT_AMT AS AREA_24_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_25_SQ_FT_AMT AS AREA_25_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_26_SQ_FT_AMT AS AREA_26_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_27_SQ_FT_AMT AS AREA_27_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_28_SQ_FT_AMT AS AREA_28_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_29_SQ_FT_AMT AS AREA_29_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_30_SQ_FT_AMT AS AREA_30_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_31_SQ_FT_AMT AS AREA_31_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_32_SQ_FT_AMT AS AREA_32_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_33_SQ_FT_AMT AS AREA_33_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_34_SQ_FT_AMT AS AREA_34_SQ_FT_AMT,
  Shortcut_to_STORE_AREA_PRE_0.AREA_35_SQ_FT_AMT AS AREA_35_SQ_FT_AMT,
  Shortcut_to_LOCATION_1.LOCATION_ID AS LOCATION_ID,
  Shortcut_to_LOCATION_1.DATE_CLOSED AS DATE_CLOSED,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_STORE_AREA_PRE_0,
  Shortcut_to_LOCATION_1
WHERE
  Shortcut_to_LOCATION_1.STORE_NBR = Shortcut_to_STORE_AREA_PRE_0.STORE_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_Shortcut_to_STORE_AREA_PRE_2")

# COMMAND ----------
# DBTITLE 1, NRM_LocationArea


Normalizer Transformation not supported

# COMMAND ----------
# DBTITLE 1, EXP_LocationArea_load_XDate_3


query_3 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  GCID_SQ_FT_AMT AS AREA_ID,
  TRUNC(
    TO_DATE(LOCATION_AREA_EFF_DT, 'MM/DD/YYYY HH24:MI:SS')
  ) AS LOCATION_AREA_EFF_DT_OUT,
  LOCATION_AREA_END_DT AS LOCATION_AREA_END_DT_IN,
  TRUNC(
    TO_DATE(LOCATION_AREA_END_DT, 'MM/DD/YYYY HH24:MI:SS')
  ) AS LOCATION_AREA_END_DT_OUT,
  SQ_FT_AMT AS AREA_SQ_FT_AMT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  null"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_LocationArea_load_XDate_3")

# COMMAND ----------
# DBTITLE 1, LOCATION_AREA


spark.sql("""INSERT INTO
  LOCATION_AREA
SELECT
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  AREA_ID AS AREA_ID,
  AREA_ID AS AREA_ID,
  LOC_AREA_EFF_DT AS LOC_AREA_EFF_DT,
  LOCATION_AREA_EFF_DT_OUT AS LOC_AREA_EFF_DT,
  LOC_AREA_END_DT AS LOC_AREA_END_DT,
  LOCATION_AREA_END_DT_OUT AS LOC_AREA_END_DT,
  AREA_SQ_FT_AMT AS SQ_FT_AMT
FROM
  EXP_LocationArea_load_XDate_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_location_area_INSERT")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_location_area_INSERT", mainWorkflowId, parentName)
