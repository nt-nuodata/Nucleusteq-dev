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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile_VET_FLAG")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_site_profile_VET_FLAG", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SALES_DAY_SKU_STORE_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  SALES_CUST_CAPTURE_CD AS SALES_CUST_CAPTURE_CD,
  DATE_LOADED AS DATE_LOADED,
  VENDOR_ID AS VENDOR_ID,
  PROMO_FLAG AS PROMO_FLAG,
  WEEK_DT AS WEEK_DT,
  FISCAL_YR AS FISCAL_YR,
  SKU_VEND_TXN_CNT AS SKU_VEND_TXN_CNT,
  SALES_AMT AS SALES_AMT,
  SALES_COST AS SALES_COST,
  SALES_QTY AS SALES_QTY,
  RETURN_AMT AS RETURN_AMT,
  RETURN_COST AS RETURN_COST,
  RETURN_QTY AS RETURN_QTY,
  DISCOUNT_AMT AS DISCOUNT_AMT,
  DISCOUNT_QTY AS DISCOUNT_QTY,
  DISCOUNT_RETURN_AMT AS DISCOUNT_RETURN_AMT,
  DISCOUNT_RETURN_QTY AS DISCOUNT_RETURN_QTY,
  POS_COUPON_AMT AS POS_COUPON_AMT,
  POS_COUPON_QTY AS POS_COUPON_QTY,
  SPECIAL_SALES_AMT AS SPECIAL_SALES_AMT,
  SPECIAL_SALES_QTY AS SPECIAL_SALES_QTY,
  SPECIAL_RETURN_AMT AS SPECIAL_RETURN_AMT,
  SPECIAL_RETURN_QTY AS SPECIAL_RETURN_QTY,
  SPECIAL_SRVC_AMT AS SPECIAL_SRVC_AMT,
  SPECIAL_SRVC_QTY AS SPECIAL_SRVC_QTY,
  EXCH_RATE_PCT AS EXCH_RATE_PCT
FROM
  SALES_DAY_SKU_STORE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SALES_DAY_SKU_STORE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_SALES_WEEK_CAT_STORE_1


query_1 = f"""SELECT
  sdss.location_id AS LOCATION_ID,
  CASE
    WHEN (
      (
        (
          (
            (
              (SUM(sdss.sales_amt) - SUM(sdss.return_amt)) - SUM(sdss.discount_amt)
            ) + SUM(sdss.discount_return_amt)
          ) - SUM(sdss.special_sales_amt)
        ) + SUM(sdss.special_return_amt)
      ) - SUM(sdss.pos_coupon_amt)
    ) > 50 THEN 1
    ELSE 0
  END AS VET_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SALES_DAY_SKU_STORE_0 sdss
  JOIN sku_profile sp ON sdss.product_id = sp.product_id
WHERE
  sdss.week_dt = (CURRENT_DATE - (date_part('DOW', CURRENT_DATE) - 1))
  AND {p_vet}
GROUP BY
  sdss.location_id
UNION ALL
SELECT
  location_id AS LOCATION_ID,
  0 AS VET_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  site_profile
WHERE
  location_id NOT IN (
    SELECT
      DISTINCT sdss.location_id
    FROM
      Shortcut_to_SALES_DAY_SKU_STORE_0 sdss
      JOIN sku_profile sp ON sdss.product_id = sp.product_id
    WHERE
      sdss.week_dt = (CURRENT_DATE - (date_part('DOW', CURRENT_DATE) - 1))
      AND {p_vet}
  )"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SALES_WEEK_CAT_STORE_1")

# COMMAND ----------
# DBTITLE 1, UPD_SITE_PROFILE_2


query_2 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  VET_FLAG AS VET_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_to_SALES_WEEK_CAT_STORE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("UPD_SITE_PROFILE_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_PROFILE_3


query_3 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_NBR AS SKU_NBR,
  SKU_TYPE AS SKU_TYPE,
  PRIMARY_UPC_ID AS PRIMARY_UPC_ID,
  STATUS_ID AS STATUS_ID,
  SUBS_HIST_FLAG AS SUBS_HIST_FLAG,
  SUBS_CURR_FLAG AS SUBS_CURR_FLAG,
  SKU_DESC AS SKU_DESC,
  ALT_DESC AS ALT_DESC,
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CLASS_ID AS SAP_CLASS_ID,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PARENT_VENDOR_ID AS PARENT_VENDOR_ID,
  COUNTRY_CD AS COUNTRY_CD,
  IMPORT_FLAG AS IMPORT_FLAG,
  HTS_CODE_ID AS HTS_CODE_ID,
  CONTENTS AS CONTENTS,
  CONTENTS_UNITS AS CONTENTS_UNITS,
  WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
  SIZE_DESC AS SIZE_DESC,
  BUM_QTY AS BUM_QTY,
  UOM_CD AS UOM_CD,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  BUYER_ID AS BUYER_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  PURCH_COST_AMT AS PURCH_COST_AMT,
  NAT_PRICE_US_AMT AS NAT_PRICE_US_AMT,
  TAX_CLASS_ID AS TAX_CLASS_ID,
  VALUATION_CLASS_CD AS VALUATION_CLASS_CD,
  BRAND_CD AS BRAND_CD,
  BRAND_CLASSIFICATION_ID AS BRAND_CLASSIFICATION_ID,
  OWNBRAND_FLAG AS OWNBRAND_FLAG,
  STATELINE_FLAG AS STATELINE_FLAG,
  SIGN_TYPE_CD AS SIGN_TYPE_CD,
  OLD_ARTICLE_NBR AS OLD_ARTICLE_NBR,
  VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
  INIT_MKDN_DT AS INIT_MKDN_DT,
  DISC_START_DT AS DISC_START_DT,
  ADD_DT AS ADD_DT,
  DELETE_DT AS DELETE_DT,
  UPDATE_DT AS UPDATE_DT,
  FIRST_SALE_DT AS FIRST_SALE_DT,
  LAST_SALE_DT AS LAST_SALE_DT,
  FIRST_INV_DT AS FIRST_INV_DT,
  LAST_INV_DT AS LAST_INV_DT,
  LOAD_DT AS LOAD_DT,
  BASE_NBR AS BASE_NBR,
  BP_COLOR_ID AS BP_COLOR_ID,
  BP_SIZE_ID AS BP_SIZE_ID,
  BP_BREED_ID AS BP_BREED_ID,
  BP_ITEM_CONCATENATED AS BP_ITEM_CONCATENATED,
  BP_AEROSOL_FLAG AS BP_AEROSOL_FLAG,
  BP_HAZMAT_FLAG AS BP_HAZMAT_FLAG,
  CANADIAN_HTS_CD AS CANADIAN_HTS_CD,
  NAT_PRICE_CA_AMT AS NAT_PRICE_CA_AMT,
  NAT_PRICE_PR_AMT AS NAT_PRICE_PR_AMT,
  RTV_DEPT_CD AS RTV_DEPT_CD,
  GL_ACCT_NBR AS GL_ACCT_NBR,
  ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
  COMPONENT_FLAG AS COMPONENT_FLAG,
  ZDISCO_SCHED_TYPE_ID AS ZDISCO_SCHED_TYPE_ID,
  ZDISCO_MKDN_SCHED_ID AS ZDISCO_MKDN_SCHED_ID,
  ZDISCO_PID_DT AS ZDISCO_PID_DT,
  ZDISCO_START_DT AS ZDISCO_START_DT,
  ZDISCO_INIT_MKDN_DT AS ZDISCO_INIT_MKDN_DT,
  ZDISCO_DC_DT AS ZDISCO_DC_DT,
  ZDISCO_STR_DT AS ZDISCO_STR_DT,
  ZDISCO_STR_OWNRSHP_DT AS ZDISCO_STR_OWNRSHP_DT,
  ZDISCO_STR_WRT_OFF_DT AS ZDISCO_STR_WRT_OFF_DT
FROM
  SKU_PROFILE"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_To_SKU_PROFILE_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_PROFILE_4


query_4 = f"""SELECT
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

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_SITE_PROFILE_4")

# COMMAND ----------
# DBTITLE 1, SITE_PROFILE


spark.sql("""MERGE INTO SITE_PROFILE AS TARGET
USING
  UPD_SITE_PROFILE_2 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID,
  TARGET.VET_FLAG = SOURCE.VET_FLAG""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile_VET_FLAG")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_site_profile_VET_FLAG", mainWorkflowId, parentName)
