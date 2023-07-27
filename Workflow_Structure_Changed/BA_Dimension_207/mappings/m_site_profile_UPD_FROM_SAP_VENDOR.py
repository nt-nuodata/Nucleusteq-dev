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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile_UPD_FROM_SAP_VENDOR")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_site_profile_UPD_FROM_SAP_VENDOR", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_PROFILE_0


query_0 = f"""SELECT
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

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SITE_PROFILE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_PROFILE_1


query_1 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  VENDOR_NAME AS VENDOR_NAME,
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  VENDOR_NBR AS VENDOR_NBR,
  LOCATION_ID AS LOCATION_ID,
  SUPERIOR_VENDOR_ID AS SUPERIOR_VENDOR_ID,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  PARENT_VENDOR_NAME AS PARENT_VENDOR_NAME,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  EDI_ELIG_FLAG AS EDI_ELIG_FLAG,
  PURCHASE_BLOCK AS PURCHASE_BLOCK,
  POSTING_BLOCK AS POSTING_BLOCK,
  DELETION_FLAG AS DELETION_FLAG,
  VIP_CD AS VIP_CD,
  INACTIVE_FLAG AS INACTIVE_FLAG,
  PAYMENT_TERM_CD AS PAYMENT_TERM_CD,
  INCO_TERM_CD AS INCO_TERM_CD,
  ADDRESS AS ADDRESS,
  CITY AS CITY,
  STATE AS STATE,
  COUNTRY_CD AS COUNTRY_CD,
  ZIP AS ZIP,
  CONTACT AS CONTACT,
  CONTACT_PHONE AS CONTACT_PHONE,
  PHONE AS PHONE,
  PHONE_EXT AS PHONE_EXT,
  FAX AS FAX,
  RTV_ELIG_FLAG AS RTV_ELIG_FLAG,
  RTV_TYPE_CD AS RTV_TYPE_CD,
  RTV_FREIGHT_TYPE_CD AS RTV_FREIGHT_TYPE_CD,
  INDUSTRY_CD AS INDUSTRY_CD,
  LATITUDE AS LATITUDE,
  LONGITUDE AS LONGITUDE,
  TIME_ZONE_ID AS TIME_ZONE_ID,
  ADD_DT AS ADD_DT,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  VENDOR_PROFILE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_VENDOR_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_VENDOR_PROFILE1_2


query_2 = f"""SELECT
  S.LOCATION_ID AS LOCATION_ID,
  NVL(TRIM(V.VENDOR_NAME), 'Not Defined') AS VENDOR_NAME,
  NVL(TRIM(V.VENDOR_NBR), '0') AS VENDOR_NBR,
  NVL(V.PARENT_VENDOR_ID, 0) AS PARENT_VENDOR_ID,
  SUBSTR(NVL(TRIM(V.ADDRESS), 'Not Defined'), 1, 30) AS ADDRESS,
  SUBSTR(NVL(TRIM(V.CITY), 'Not Defined'), 1, 25) AS CITY,
  SUBSTR(NVL(TRIM(V.STATE), 'NA'), 1, 3) AS STATE,
  NVL(TRIM(V.COUNTRY_CD), 'NA') AS COUNTRY_CD,
  NVL(TRIM(V.ZIP), '00000') AS ZIP,
  SUBSTR(NVL(TRIM(V.PHONE), '0'), 1, 10) AS PHONE,
  NVL(V.LATITUDE, 0) AS LATITUDE,
  NVL(V.LONGITUDE, 0) AS LONGITUDE,
  CURRENT_DATE AS UPDATE_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_VENDOR_PROFILE_1 V,
  Shortcut_to_SITE_PROFILE_0 S
WHERE
  TRIM(V.VENDOR_NBR) = TRIM(S.LOCATION_NBR)
  AND S.LOCATION_TYPE_ID = 19
  AND V.VENDOR_TYPE_ID IN (1, 2)
  AND (
    NVL(TRIM(V.VENDOR_NAME), 'Not Defined') <> NVL(TRIM(S.STORE_NAME), ' ')
    OR NVL(TRIM(V.VENDOR_NBR), '0') <> NVL(TRIM(S.LOCATION_NBR), ' ')
    OR NVL(V.PARENT_VENDOR_ID, 0) <> NVL(S.PARENT_LOCATION_ID, 999999999)
    OR SUBSTR(NVL(TRIM(V.ADDRESS), 'Not Defined'), 1, 30) <> NVL(TRIM(S.SITE_ADDRESS), ' ')
    OR SUBSTR(NVL(TRIM(V.CITY), 'Not Defined'), 1, 25) <> NVL(TRIM(S.SITE_CITY), ' ')
    OR SUBSTR(NVL(TRIM(V.STATE), 'NA'), 1, 3) <> NVL(TRIM(S.STATE_CD), ' ')
    OR NVL(TRIM(V.COUNTRY_CD), 'NA') <> NVL(TRIM(S.COUNTRY_CD), ' ')
    OR NVL(TRIM(V.ZIP), '00000') <> NVL(TRIM(S.POSTAL_CD), ' ')
    OR SUBSTR(NVL(TRIM(V.PHONE), '0'), 1, 10) <> NVL(TRIM(S.SITE_MAIN_TELE_NO), ' ')
    OR NVL(V.LATITUDE, 0) <> NVL(S.GEO_LATITUDE_NBR, 999999999)
    OR NVL(V.LONGITUDE, 0) <> NVL(S.GEO_LONGITUDE_NBR, 999999999)
  )"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_VENDOR_PROFILE1_2")

# COMMAND ----------
# DBTITLE 1, UPD_update_3


query_3 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  VENDOR_NAME AS VENDOR_NAME,
  ADDRESS AS ADDRESS,
  CITY AS CITY,
  STATE AS STATE,
  COUNTRY_CD AS COUNTRY_CD,
  ZIP AS ZIP,
  PHONE AS PHONE,
  LATITUDE AS LATITUDE,
  LONGITUDE AS LONGITUDE,
  UPDATE_DT AS UPDATE_DT,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  VENDOR_NBR AS VENDOR_NBR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_VENDOR_PROFILE1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_update_3")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE


spark.sql("""MERGE INTO SITE_PROFILE AS TARGET
USING
  UPD_update_3 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID,
  TARGET.STORE_NAME = SOURCE.VENDOR_NAME,
  TARGET.SITE_ADDRESS = SOURCE.ADDRESS,
  TARGET.SITE_CITY = SOURCE.CITY,
  TARGET.STATE_CD = SOURCE.STATE,
  TARGET.COUNTRY_CD = SOURCE.COUNTRY_CD,
  TARGET.POSTAL_CD = SOURCE.ZIP,
  TARGET.SITE_MAIN_TELE_NO = SOURCE.PHONE,
  TARGET.PARENT_LOCATION_ID = SOURCE.PARENT_VENDOR_ID,
  TARGET.LOCATION_NBR = SOURCE.VENDOR_NBR,
  TARGET.UPDATE_DT = SOURCE.UPDATE_DT""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile_UPD_FROM_SAP_VENDOR")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_site_profile_UPD_FROM_SAP_VENDOR", mainWorkflowId, parentName)
