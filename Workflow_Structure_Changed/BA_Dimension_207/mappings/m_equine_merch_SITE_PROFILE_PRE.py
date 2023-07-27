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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_equine_merch_SITE_PROFILE_PRE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_equine_merch_SITE_PROFILE_PRE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SITE_PROFILE_PRE_0


query_0 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  COMPANY_DESC AS COMPANY_DESC,
  COMPANY_ID AS COMPANY_ID,
  ADD_DT AS ADD_DT,
  CLOSE_DT AS CLOSE_DT,
  DELETE_DT AS DELETE_DT,
  OPEN_DT AS OPEN_DT,
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
  COUNTRY_CD AS COUNTRY_CD,
  COUNTRY_NAME AS COUNTRY_NAME,
  STORE_NAME AS STORE_NAME,
  STORE_NBR AS STORE_NBR,
  STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  STATE_CD AS STATE_CD,
  STORE_TYPE_DESC AS STORE_TYPE_DESC,
  STORE_TYPE_ID AS STORE_TYPE_ID,
  EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC,
  GR_OPEN_DT AS GR_OPEN_DT,
  SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  BP_COMPANY_NBR AS BP_COMPANY_NBR,
  BP_GL_ACCT AS BP_GL_ACCT,
  SITE_ADDRESS AS SITE_ADDRESS,
  POSTAL_CD AS POSTAL_CD,
  SITE_CITY AS SITE_CITY,
  SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
  SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
  LOAD_DT AS LOAD_DT,
  PROMO_LABEL_CD AS PROMO_LABEL_CD,
  SFT_OPEN_DT AS SFT_OPEN_DT,
  BANFIELD_FLAG AS BANFIELD_FLAG,
  SALES_AREA_FLR_SPC AS SALES_AREA_FLR_SPC,
  SITE_CATEGORY AS SITE_CATEGORY
FROM
  SITE_PROFILE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SITE_PROFILE_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_SHORTCUT_TO_SITE_PROFILE_PRE_1


query_1 = f"""SELECT
  DISTINCT (
    CASE
      WHEN RTRIM(EQUINE_MERCH_DESC) = 'Basic Full Equine Dept' THEN 1
      WHEN RTRIM(EQUINE_MERCH_DESC) = 'Extended Full Equine Dept' THEN 2
      WHEN RTRIM(EQUINE_MERCH_DESC) = 'Basic Limited Equine Dept' THEN 3
      WHEN RTRIM(EQUINE_MERCH_DESC) = 'Extended Limited Equine Dept' THEN 4
      WHEN RTRIM(EQUINE_MERCH_DESC) = 'Stand Alone Equine Store' THEN 5
      WHEN RTRIM(EQUINE_MERCH_DESC) = 'Non Equine Store' THEN 99
      ELSE 99
    END
  ) EQUINE_MERCH_ID,
  NVL(RTRIM(equine_merch_desc), 'Non Equine Store') EQUINE_MERCH_DESC
FROM
  Shortcut_To_SITE_PROFILE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_SHORTCUT_TO_SITE_PROFILE_PRE_1")

# COMMAND ----------
# DBTITLE 1, EQUINE_MERCH


spark.sql("""INSERT INTO
  EQUINE_MERCH
SELECT
  STORE_TYPE_ID_DUMMY AS EQUINE_MERCH_ID,
  EQUINE_MERCH_DESC AS EQUINE_MERCH_DESC
FROM
  ASQ_SHORTCUT_TO_SITE_PROFILE_PRE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_equine_merch_SITE_PROFILE_PRE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_equine_merch_SITE_PROFILE_PRE", mainWorkflowId, parentName)
