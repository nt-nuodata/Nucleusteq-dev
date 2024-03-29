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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_location_old")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_location_old", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_LOCATION_0


query_0 = f"""SELECT
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

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_LOCATION_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_LOCATION_1


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
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_LOCATION_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_LOCATION_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOCATION_OLD_2


query_2 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
  STORE_TYPE_ID AS STORE_TYPE_ID,
  DATE_OPEN AS DATE_OPEN,
  DATE_CLOSED AS DATE_CLOSED,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_To_LOCATION_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOCATION_OLD_2")

# COMMAND ----------
# DBTITLE 1, LOCATION_OLD


spark.sql("""INSERT INTO
  LOCATION_OLD
SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  STORE_CTRY_ABBR AS STORE_CTRY_ABBR,
  STORE_TYPE_ID AS STORE_TYPE_ID,
  DATE_OPEN AS DATE_OPEN,
  DATE_CLOSED AS DATE_CLOSED
FROM
  EXP_LOCATION_OLD_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_location_old")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_location_old", mainWorkflowId, parentName)
