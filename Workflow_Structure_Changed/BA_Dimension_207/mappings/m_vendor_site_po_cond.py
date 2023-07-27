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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_site_po_cond")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_site_po_cond", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_SITE_PO_COND_PRE_0


query_0 = f"""SELECT
  PO_COND_CD AS PO_COND_CD,
  PURCH_ORG_CD AS PURCH_ORG_CD,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  PO_COND_END_DT AS PO_COND_END_DT,
  DELETE_IND AS DELETE_IND,
  PO_COND_EFF_DT AS PO_COND_EFF_DT,
  PO_COND_REC_NBR AS PO_COND_REC_NBR,
  PO_COND_RATE_AMT AS PO_COND_RATE_AMT
FROM
  VENDOR_SITE_PO_COND_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_VENDOR_SITE_PO_COND_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_SITE_PO_COND1_1


query_1 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  LOCATION_ID AS LOCATION_ID,
  PO_COND_CD AS PO_COND_CD,
  PO_COND_EFF_DT AS PO_COND_EFF_DT,
  PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
  PO_COND_END_DT AS PO_COND_END_DT,
  DELETE_IND AS DELETE_IND,
  LOAD_DT AS LOAD_DT
FROM
  VENDOR_SITE_PO_COND"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_VENDOR_SITE_PO_COND1_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_PROFILE_2


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

df_2.createOrReplaceTempView("Shortcut_to_SITE_PROFILE_2")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_VENDORSITE_PO_COND_PRE_3


query_3 = f"""SELECT
  VSPCP.VENDOR_ID AS VENDOR_ID,
  VSPCP.LOCATION_ID AS LOCATION_ID,
  VSPCP.PO_COND_CD AS PO_COND_CD,
  VSPCP.PO_COND_EFF_DT AS PO_COND_EFF_DT,
  VSPCP.PO_COND_END_DT AS PO_COND_END_DT,
  VSPCP.DELETE_IND AS DELETE_IND,
  VSPCP.PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
  VSPC.VENDOR_ID AS VENDOR_ID_OLD,
  CURRENT_DATE AS LOAD_DT,
  VSPCP.VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      COND.VENDOR_ID,
      SITE.LOCATION_ID,
      COND.PO_COND_CD,
      COND.PO_COND_EFF_DT,
      COND.PO_COND_RATE_AMT,
      COND.PO_COND_END_DT,
      COND.DELETE_IND,
      COND.VENDOR_SUBRANGE_CD
    FROM
      Shortcut_to_VENDOR_SITE_PO_COND_PRE_0 COND,
      Shortcut_to_SITE_PROFILE_2 SITE
    WHERE
      COND.STORE_NBR = SITE.STORE_NBR
  ) VSPCP
  LEFT OUTER JOIN Shortcut_to_VENDOR_SITE_PO_COND1_1 VSPC ON VSPCP.LOCATION_ID = VSPC.LOCATION_ID
  AND VSPCP.VENDOR_ID = VSPC.VENDOR_ID
  AND VSPCP.PO_COND_CD = VSPC.PO_COND_CD
  AND VSPCP.VENDOR_SUBRANGE_CD = VSPC.VENDOR_SUBRANGE_CD"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("ASQ_Shortcut_To_VENDORSITE_PO_COND_PRE_3")

# COMMAND ----------
# DBTITLE 1, UPD_VENDOR_SITE_PO_COND_4


query_4 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  LOCATION_ID AS LOCATION_ID,
  PO_COND_CD AS PO_COND_CD,
  PO_COND_EFF_DT AS PO_COND_EFF_DT,
  PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
  PO_COND_END_DT AS PO_COND_END_DT,
  DELETE_IND AS DELETE_IND,
  LOAD_DT AS LOAD_DT,
  VENDOR_ID_OLD AS VENDOR_ID_OLD,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(ISNULL(VENDOR_ID_OLD), 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  ASQ_Shortcut_To_VENDORSITE_PO_COND_PRE_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("UPD_VENDOR_SITE_PO_COND_4")

# COMMAND ----------
# DBTITLE 1, VENDOR_SITE_PO_COND


spark.sql("""MERGE INTO VENDOR_SITE_PO_COND AS TARGET
USING
  UPD_VENDOR_SITE_PO_COND_4 AS SOURCE ON TARGET.PO_COND_CD = SOURCE.PO_COND_CD
  AND TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  AND TARGET.VENDOR_SUBRANGE_CD = SOURCE.VENDOR_SUBRANGE_CD
  AND TARGET.VENDOR_ID = SOURCE.VENDOR_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.VENDOR_ID = SOURCE.VENDOR_ID,
  TARGET.VENDOR_SUBRANGE_CD = SOURCE.VENDOR_SUBRANGE_CD,
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID,
  TARGET.PO_COND_CD = SOURCE.PO_COND_CD,
  TARGET.PO_COND_EFF_DT = SOURCE.PO_COND_EFF_DT,
  TARGET.PO_COND_RATE_AMT = SOURCE.PO_COND_RATE_AMT,
  TARGET.PO_COND_END_DT = SOURCE.PO_COND_END_DT,
  TARGET.DELETE_IND = SOURCE.DELETE_IND,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PO_COND_EFF_DT = SOURCE.PO_COND_EFF_DT
  AND TARGET.PO_COND_RATE_AMT = SOURCE.PO_COND_RATE_AMT
  AND TARGET.PO_COND_END_DT = SOURCE.PO_COND_END_DT
  AND TARGET.DELETE_IND = SOURCE.DELETE_IND
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.VENDOR_ID,
    TARGET.VENDOR_SUBRANGE_CD,
    TARGET.LOCATION_ID,
    TARGET.PO_COND_CD,
    TARGET.PO_COND_EFF_DT,
    TARGET.PO_COND_RATE_AMT,
    TARGET.PO_COND_END_DT,
    TARGET.DELETE_IND,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.VENDOR_ID,
    SOURCE.VENDOR_SUBRANGE_CD,
    SOURCE.LOCATION_ID,
    SOURCE.PO_COND_CD,
    SOURCE.PO_COND_EFF_DT,
    SOURCE.PO_COND_RATE_AMT,
    SOURCE.PO_COND_END_DT,
    SOURCE.DELETE_IND,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_site_po_cond")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_site_po_cond", mainWorkflowId, parentName)
