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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile_SQ_FOOTAGE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_site_profile_SQ_FOOTAGE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_LOCATION_AREA_0


query_0 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  AREA_ID AS AREA_ID,
  LOC_AREA_EFF_DT AS LOC_AREA_EFF_DT,
  LOC_AREA_END_DT AS LOC_AREA_END_DT,
  SQ_FT_AMT AS SQ_FT_AMT
FROM
  LOCATION_AREA"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_LOCATION_AREA_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_LOCATION_AREA_1


query_1 = f"""SELECT
  DISTINCT a.location_id,
  a.sq_ft_amt AS sq_feet_total,
  b.sq_ft_amt AS sq_feet_retail
FROM
  (
    SELECT
      la.location_id,
      la.area_id,
      la.sq_ft_amt
    FROM
      Shortcut_To_LOCATION_AREA_0 la,
      site_profile sp
    WHERE
      la.location_id = sp.location_id
      AND la.area_id = 1
      AND la.loc_area_end_dt >= CURRENT_DATE
  ) a,
  (
    SELECT
      la.location_id,
      la.area_id,
      la.sq_ft_amt
    FROM
      Shortcut_To_LOCATION_AREA_0 la,
      site_profile sp
    WHERE
      la.location_id = sp.location_id
      AND la.area_id = 2
      AND la.loc_area_end_dt >= CURRENT_DATE
  ) b
WHERE
  a.location_id = b.location_id"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_LOCATION_AREA_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SITE_PROFILE_2


query_2 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  STORE_NBR AS STORE_NBR,
  STORE_NAME AS STORE_NAME,
  STORE_TYPE_ID AS STORE_TYPE_ID,
  STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  COMPANY_ID AS COMPANY_ID,
  REGION_ID AS REGION_ID,
  DISTRICT_ID AS DISTRICT_ID,
  PRICE_ZONE_ID AS PRICE_ZONE_ID,
  PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
  REPL_DC_NBR AS REPL_DC_NBR,
  REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
  REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
  SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  SITE_ADDRESS AS SITE_ADDRESS,
  SITE_CITY AS SITE_CITY,
  STATE_CD AS STATE_CD,
  COUNTRY_CD AS COUNTRY_CD,
  POSTAL_CD AS POSTAL_CD,
  SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
  SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
  SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
  SITE_SALES_FLAG AS SITE_SALES_FLAG,
  EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
  EQUINE_SITE_ID AS EQUINE_SITE_ID,
  EQUINE_SITE_OPEN_DT AS EQUINE_SITE_OPEN_DT,
  GEO_LATITUDE_NBR AS GEO_LATITUDE_NBR,
  GEO_LONGITUDE_NBR AS GEO_LONGITUDE_NBR,
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
  LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
  BP_COMPANY_NBR AS BP_COMPANY_NBR,
  BP_GL_ACCT AS BP_GL_ACCT,
  TP_LOC_FLAG AS TP_LOC_FLAG,
  TP_ACTIVE_CNT AS TP_ACTIVE_CNT,
  PROMO_LABEL_CD AS PROMO_LABEL_CD,
  PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
  LOCATION_NBR AS LOCATION_NBR,
  TIME_ZONE_ID AS TIME_ZONE_ID,
  DELV_SERVICE_CLASS_ID AS DELV_SERVICE_CLASS_ID,
  PICK_SERVICE_CLASS_ID AS PICK_SERVICE_CLASS_ID,
  SITE_LOGIN_ID AS SITE_LOGIN_ID,
  SITE_MANAGER_ID AS SITE_MANAGER_ID,
  SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
  HOTEL_FLAG AS HOTEL_FLAG,
  DAYCAMP_FLAG AS DAYCAMP_FLAG,
  VET_FLAG AS VET_FLAG,
  DIST_MGR_NAME AS DIST_MGR_NAME,
  DIST_SVC_MGR_NAME AS DIST_SVC_MGR_NAME,
  REGION_VP_NAME AS REGION_VP_NAME,
  REGION_TRAINER_NAME AS REGION_TRAINER_NAME,
  ASSET_PROTECT_NAME AS ASSET_PROTECT_NAME,
  SITE_COUNTY AS SITE_COUNTY,
  SITE_FAX_NO AS SITE_FAX_NO,
  SFT_OPEN_DT AS SFT_OPEN_DT,
  DM_EMAIL_ADDRESS AS DM_EMAIL_ADDRESS,
  DSM_EMAIL_ADDRESS AS DSM_EMAIL_ADDRESS,
  RVP_EMAIL_ADDRESS AS RVP_EMAIL_ADDRESS,
  TRADE_AREA AS TRADE_AREA,
  FDLPS_NAME AS FDLPS_NAME,
  FDLPS_EMAIL AS FDLPS_EMAIL,
  OVERSITE_MGR_NAME AS OVERSITE_MGR_NAME,
  OVERSITE_MGR_EMAIL AS OVERSITE_MGR_EMAIL,
  SAFETY_DIRECTOR_NAME AS SAFETY_DIRECTOR_NAME,
  SAFETY_DIRECTOR_EMAIL AS SAFETY_DIRECTOR_EMAIL,
  RETAIL_MANAGER_SAFETY_NAME AS RETAIL_MANAGER_SAFETY_NAME,
  RETAIL_MANAGER_SAFETY_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
  AREA_DIRECTOR_NAME AS AREA_DIRECTOR_NAME,
  AREA_DIRECTOR_EMAIL AS AREA_DIRECTOR_EMAIL,
  DC_GENERAL_MANAGER_NAME AS DC_GENERAL_MANAGER_NAME,
  DC_GENERAL_MANAGER_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
  ASST_DC_GENERAL_MANAGER_NAME1 AS ASST_DC_GENERAL_MANAGER_NAME1,
  ASST_DC_GENERAL_MANAGER_EMAIL1 AS ASST_DC_GENERAL_MANAGER_EMAIL1,
  ASST_DC_GENERAL_MANAGER_NAME2 AS ASST_DC_GENERAL_MANAGER_NAME2,
  ASST_DC_GENERAL_MANAGER_EMAIL2 AS ASST_DC_GENERAL_MANAGER_EMAIL2,
  REGIONAL_DC_SAFETY_MGR_NAME AS REGIONAL_DC_SAFETY_MGR_NAME,
  REGIONAL_DC_SAFETY_MGR_EMAIL AS REGIONAL_DC_SAFETY_MGR_EMAIL,
  DC_PEOPLE_SUPERVISOR_NAME AS DC_PEOPLE_SUPERVISOR_NAME,
  DC_PEOPLE_SUPERVISOR_EMAIL AS DC_PEOPLE_SUPERVISOR_EMAIL,
  PEOPLE_MANAGER_NAME AS PEOPLE_MANAGER_NAME,
  PEOPLE_MANAGER_EMAIL AS PEOPLE_MANAGER_EMAIL,
  ASSET_PROT_DIR_NAME AS ASSET_PROT_DIR_NAME,
  ASSET_PROT_DIR_EMAIL AS ASSET_PROT_DIR_EMAIL,
  SR_REG_ASSET_PROT_MGR_NAME AS SR_REG_ASSET_PROT_MGR_NAME,
  SR_REG_ASSET_PROT_MGR_EMAIL AS SR_REG_ASSET_PROT_MGR_EMAIL,
  REG_ASSET_PROT_MGR_NAME AS REG_ASSET_PROT_MGR_NAME,
  REG_ASSET_PROT_MGR_EMAIL AS REG_ASSET_PROT_MGR_EMAIL,
  ASSET_PROTECT_EMAIL AS ASSET_PROTECT_EMAIL,
  TP_START_DT AS TP_START_DT,
  OPEN_DT AS OPEN_DT,
  GR_OPEN_DT AS GR_OPEN_DT,
  CLOSE_DT AS CLOSE_DT,
  HOTEL_OPEN_DT AS HOTEL_OPEN_DT,
  ADD_DT AS ADD_DT,
  DELETE_DT AS DELETE_DT,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  SITE_PROFILE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_To_SITE_PROFILE_2")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE


spark.sql("""INSERT INTO
  SITE_PROFILE
SELECT
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID21 AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  LocationID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_TYPE_ID1 AS LOCATION_TYPE_ID,
  STORE_NBR1 AS STORE_NBR,
  VENDOR_NAME AS STORE_NAME,
  VENDOR_NAME AS STORE_NAME,
  STORE_TYPE_ID1 AS STORE_TYPE_ID,
  STORE_OPEN_CLOSE_FLAG1 AS STORE_OPEN_CLOSE_FLAG,
  COMPANY_ID1 AS COMPANY_ID,
  ZERO_DEFAULT1 AS REGION_ID,
  ZERO_DEFAULT1 AS DISTRICT_ID,
  ZERO_DEFAULT1 AS PRICE_ZONE_ID,
  ZERO_DEFAULT1 AS PRICE_AD_ZONE_ID,
  ZERO_DEFAULT1 AS REPL_DC_NBR,
  ZERO_DEFAULT1 AS REPL_FISH_DC_NBR,
  ZERO_DEFAULT1 AS REPL_FWD_DC_NBR,
  ZERO_DEFAULT1 AS SQ_FEET_RETAIL,
  SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  ZERO_DEFAULT1 AS SQ_FEET_TOTAL,
  SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  ADDRESS AS SITE_ADDRESS,
  ADDRESS AS SITE_ADDRESS,
  CITY AS SITE_CITY,
  CITY AS SITE_CITY,
  STATE AS STATE_CD,
  STATE AS STATE_CD,
  COUNTRY_CD AS COUNTRY_CD,
  COUNTRY_CD AS COUNTRY_CD,
  ZIP AS POSTAL_CD,
  ZIP AS POSTAL_CD,
  PHONE AS SITE_MAIN_TELE_NO,
  PHONE AS SITE_MAIN_TELE_NO,
  ZERO_DEFAULT1 AS SITE_GROOM_TELE_NO,
  DEFAULT_NOT_DEFINED1 AS SITE_EMAIL_ADDRESS,
  ZERO_DEFAULT1 AS SITE_SALES_FLAG,
  EQUINE_MERCH_ID1 AS EQUINE_MERCH_ID,
  ZERO_DEFAULT1 AS EQUINE_SITE_ID,
  DEFAULT_DT1 AS EQUINE_SITE_OPEN_DT,
  NULL AS GEO_LATITUDE_NBR,
  NULL AS GEO_LONGITUDE_NBR,
  ZERO_DEFAULT1 AS PETSMART_DMA_CD,
  ZERO_DEFAULT1 AS LOYALTY_PGM_TYPE_ID,
  ZERO_DEFAULT1 AS LOYALTY_PGM_STATUS_ID,
  DEFAULT_DT1 AS LOYALTY_PGM_START_DT,
  DEFAULT_DT1 AS LOYALTY_PGM_CHANGE_DT,
  ZERO_DEFAULT1 AS BP_COMPANY_NBR,
  ZERO_DEFAULT1 AS BP_GL_ACCT,
  TP_LOC_FLAG1 AS TP_LOC_FLAG,
  ZERO_DEFAULT1 AS TP_ACTIVE_CNT,
  ZERO_DEFAULT1 AS PROMO_LABEL_CD,
  PARENT_VENDOR_ID AS PARENT_LOCATION_ID,
  PARENT_VENDOR_ID AS PARENT_LOCATION_ID,
  VENDOR_NBR AS LOCATION_NBR,
  VENDOR_NBR AS LOCATION_NBR,
  ZERO_DEFAULT1 AS TIME_ZONE_ID,
  TimeZoneDesc AS TIME_ZONE_ID,
  ZERO_DEFAULT1 AS DELV_SERVICE_CLASS_ID,
  ZERO_DEFAULT1 AS PICK_SERVICE_CLASS_ID,
  ZERO_DEFAULT1 AS SITE_LOGIN_ID,
  ZERO_DEFAULT1 AS SITE_MANAGER_ID,
  ZERO_DEFAULT1 AS SITE_OPEN_YRS_AMT,
  ZERO_DEFAULT1 AS HOTEL_FLAG,
  ZERO_DEFAULT1 AS DAYCAMP_FLAG,
  ZERO_DEFAULT1 AS VET_FLAG,
  DEFAULT_NOT_DEFINED1 AS DIST_MGR_NAME,
  Output_DM_NAME AS DIST_MGR_NAME,
  DEFAULT_NOT_DEFINED1 AS DIST_SVC_MGR_NAME,
  DEFAULT_NOT_DEFINED1 AS REGION_VP_NAME,
  DEFAULT_NOT_DEFINED1 AS REGION_TRAINER_NAME,
  DEFAULT_NOT_DEFINED1 AS ASSET_PROTECT_NAME,
  NULL AS SITE_COUNTY,
  NULL AS SITE_FAX_NO,
  NULL AS SFT_OPEN_DT,
  Output_DM_EMAIL AS DM_EMAIL_ADDRESS,
  NULL AS DSM_EMAIL_ADDRESS,
  NULL AS RVP_EMAIL_ADDRESS,
  NULL AS TRADE_AREA,
  NULL AS FDLPS_NAME,
  NULL AS FDLPS_EMAIL,
  NULL AS OVERSITE_MGR_NAME,
  NULL AS OVERSITE_MGR_EMAIL,
  NULL AS SAFETY_DIRECTOR_NAME,
  NULL AS SAFETY_DIRECTOR_EMAIL,
  NULL AS RETAIL_MANAGER_SAFETY_NAME,
  NULL AS RETAIL_MANAGER_SAFETY_EMAIL,
  NULL AS AREA_DIRECTOR_NAME,
  NULL AS AREA_DIRECTOR_EMAIL,
  NULL AS DC_GENERAL_MANAGER_NAME,
  NULL AS DC_GENERAL_MANAGER_EMAIL,
  NULL AS ASST_DC_GENERAL_MANAGER_NAME1,
  NULL AS ASST_DC_GENERAL_MANAGER_EMAIL1,
  NULL AS ASST_DC_GENERAL_MANAGER_NAME2,
  NULL AS ASST_DC_GENERAL_MANAGER_EMAIL2,
  NULL AS REGIONAL_DC_SAFETY_MGR_NAME,
  NULL AS REGIONAL_DC_SAFETY_MGR_EMAIL,
  NULL AS DC_PEOPLE_SUPERVISOR_NAME,
  NULL AS DC_PEOPLE_SUPERVISOR_EMAIL,
  NULL AS PEOPLE_MANAGER_NAME,
  NULL AS PEOPLE_MANAGER_EMAIL,
  NULL AS ASSET_PROT_DIR_NAME,
  NULL AS ASSET_PROT_DIR_EMAIL,
  NULL AS SR_REG_ASSET_PROT_MGR_NAME,
  NULL AS SR_REG_ASSET_PROT_MGR_EMAIL,
  NULL AS REG_ASSET_PROT_MGR_NAME,
  NULL AS REG_ASSET_PROT_MGR_EMAIL,
  NULL AS ASSET_PROTECT_EMAIL,
  DEFAULT_DT1 AS TP_START_DT,
  DEFAULT_DT1 AS OPEN_DT,
  DEFAULT_DT1 AS GR_OPEN_DT,
  DEFAULT_DT1 AS CLOSE_DT,
  DEFAULT_DT1 AS HOTEL_OPEN_DT,
  ADD_DT AS ADD_DT,
  DEFAULT_DT1 AS DELETE_DT,
  UPDATE_DT AS UPDATE_DT,
  UPDATE_DT AS UPDATE_DT,
  Update_Dt AS UPDATE_DT,
  Update_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  ASQ_Shortcut_To_LOCATION_AREA_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile_SQ_FOOTAGE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_site_profile_SQ_FOOTAGE", mainWorkflowId, parentName)
