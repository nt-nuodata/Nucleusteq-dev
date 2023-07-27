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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_site_profile", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_PROFILE_PRE_0


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

df_0.createOrReplaceTempView("Shortcut_to_SITE_PROFILE_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_PRE_1


query_1 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  COMPANY_ID AS COMPANY_ID,
  ADD_DT AS ADD_DT,
  CLOSE_DT AS CLOSE_DT,
  DELETE_DT AS DELETE_DT,
  OPEN_DT AS OPEN_DT,
  DISTRICT_ID AS DISTRICT_ID,
  PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
  PRICE_ZONE_ID AS PRICE_ZONE_ID,
  REGION_ID AS REGION_ID,
  REPL_DC_NBR AS REPL_DC_NBR,
  REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
  REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
  COUNTRY_CD AS COUNTRY_CD,
  STORE_NAME AS STORE_NAME,
  STORE_NBR AS STORE_NBR,
  STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  STATE_CD AS STATE_CD,
  STORE_TYPE_ID AS STORE_TYPE_ID,
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
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_PROFILE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_PRE_1")

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
# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_3


query_3 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  SITE_SALES_FLAG AS SITE_SALES_FLAG,
  PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
  LOCATION_NBR AS LOCATION_NBR,
  SITE_COUNTY AS SITE_COUNTY,
  SITE_MANAGER_ID AS SITE_MANAGER_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_PROFILE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_3")

# COMMAND ----------
# DBTITLE 1, JNR_Detail_Outer_Join_4


query_4 = f"""SELECT
  MASTER.LOCATION_ID AS LOCATION_ID1,
  MASTER.STORE_NBR AS STORE_NBR,
  MASTER.STORE_NAME AS STORE_NAME,
  MASTER.STORE_TYPE_ID AS STORE_TYPE_ID,
  MASTER.STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  MASTER.COMPANY_ID AS COMPANY_ID,
  MASTER.REGION_ID AS REGION_ID,
  MASTER.DISTRICT_ID AS DISTRICT_ID,
  MASTER.PRICE_ZONE_ID AS PRICE_ZONE_ID,
  MASTER.PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
  MASTER.REPL_DC_NBR AS REPL_DC_NBR,
  MASTER.REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
  MASTER.REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
  MASTER.SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  MASTER.SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  MASTER.SITE_ADDRESS AS SITE_ADDRESS,
  MASTER.SITE_CITY AS SITE_CITY,
  MASTER.STATE_CD AS STATE_CD,
  MASTER.COUNTRY_CD AS COUNTRY_CD,
  MASTER.POSTAL_CD AS POSTAL_CD,
  MASTER.SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
  MASTER.SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
  DETAIL.SITE_SALES_FLAG AS SITE_SALES_FLAG,
  DETAIL.SITE_MANAGER_ID AS SITE_MANAGER_ID,
  MASTER.BP_COMPANY_NBR AS BP_COMPANY_NBR,
  MASTER.BP_GL_ACCT AS BP_GL_ACCT,
  MASTER.PROMO_LABEL_CD AS PROMO_LABEL_CD,
  DETAIL.PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
  DETAIL.LOCATION_NBR AS LOCATION_NBR,
  MASTER.SFT_OPEN_DT AS SFT_OPEN_DT,
  DETAIL.SITE_COUNTY AS SITE_COUNTY,
  MASTER.OPEN_DT AS OPEN_DT,
  MASTER.GR_OPEN_DT AS GR_OPEN_DT,
  MASTER.CLOSE_DT AS CLOSE_DT,
  MASTER.ADD_DT AS ADD_DT,
  MASTER.DELETE_DT AS DELETE_DT,
  MASTER.LOAD_DT AS LOAD_DT,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SITE_PROFILE_PRE_1 MASTER
  LEFT JOIN SQ_Shortcut_to_SITE_PROFILE_3 DETAIL ON MASTER.LOCATION_ID = DETAIL.LOCATION_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_Detail_Outer_Join_4")

# COMMAND ----------
# DBTITLE 1, LKP_USR_DC_NAMES_5


query_5 = f"""SELECT
  UDN.DC_RLPM_NAME AS DC_RLPM_NAME,
  UDN.DC_RLPM_EMAIL AS DC_RLPM_EMAIL,
  UDN.DC_ASST_GM1_NAME AS DC_ASST_GM1_NAME,
  UDN.DC_ASST_GM1_EMAIL AS DC_ASST_GM1_EMAIL,
  UDN.DC_ASST_GM2_NAME AS DC_ASST_GM2_NAME,
  UDN.DC_ASST_GM2_EMAIL AS DC_ASST_GM2_EMAIL,
  UDN.DC_GM_NAME AS DC_GM_NAME,
  UDN.DC_GM_EMAIL AS DC_GM_EMAIL,
  UDN.DC_HR_SUPER1_NAME AS DC_HR_SUPER1_NAME,
  UDN.DC_HR_SUPER1_EMAIL AS DC_HR_SUPER1_EMAIL,
  UDN.DC_HR_SUPER2_NAME AS DC_HR_SUPER2_NAME,
  UDN.DC_HR_SUPER2_EMAIL AS DC_HR_SUPER2_EMAIL,
  UDN.DC_HR_MGR_NAME AS DC_HR_MGR_NAME,
  UDN.DC_HR_MGR_EMAIL AS DC_HR_MGR_EMAIL,
  JDOJ4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_Detail_Outer_Join_4 JDOJ4
  LEFT JOIN USR_DC_NAMES UDN ON UDN.STORE_NBR = JDOJ4.STORE_NBR"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("LKP_USR_DC_NAMES_5")

# COMMAND ----------
# DBTITLE 1, EXP_Conv_STORE_NBR_6


query_6 = f"""SELECT
  TO_CHAR(STORE_NBR) AS o_STORE_NBR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_Detail_Outer_Join_4"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_Conv_STORE_NBR_6")

# COMMAND ----------
# DBTITLE 1, LKP_SAP_T001W_SITE_PRE_7


query_7 = f"""SELECT
  STSP.BEZEI AS BEZEI,
  ECSN6.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Conv_STORE_NBR_6 ECSN6
  LEFT JOIN SAP_T001W_SITE_PRE STSP ON STSP.WERKS = ECSN6.o_STORE_NBR"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("LKP_SAP_T001W_SITE_PRE_7")

# COMMAND ----------
# DBTITLE 1, EXP_Conv_REGION_8


query_8 = f"""SELECT
  REGION_ID AS REGION_ID,
  TO_INTEGER(REGION_ID) AS o_REGION_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_Detail_Outer_Join_4"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("EXP_Conv_REGION_8")

# COMMAND ----------
# DBTITLE 1, LKP_USR_REGION_NAMES_9


query_9 = f"""SELECT
  URN.RVP_NAME AS RVP_NAME,
  URN.RVP_EMAIL AS RVP_EMAIL,
  URN.RD_NAME AS RD_NAME,
  URN.RD_EMAIL AS RD_EMAIL,
  URN.R_PEOPLE_DIR_NAME AS R_PEOPLE_DIR_NAME,
  URN.R_PEOPLE_DIR_EMAIL AS R_PEOPLE_DIR_EMAIL,
  URN.SR_LP_SAFETY_MGR_NAME AS SR_LP_SAFETY_MGR_NAME,
  URN.SR_LP_SAFETY_MGR_EMAIL AS SR_LP_SAFETY_MGR_EMAIL,
  URN.RETAIL_SAFETY_MGR_NAME AS RETAIL_SAFETY_MGR_NAME,
  URN.RETAIL_SAFETY_MGR_EMAIL AS RETAIL_SAFETY_MGR_EMAIL,
  URN.LP_SAFETY_DIR_NAME AS LP_SAFETY_DIR_NAME,
  URN.LP_SAFETY_DIR_EMAIL AS LP_SAFETY_DIR_EMAIL,
  URN.LEARN_SOLUTION_MGR_NAME AS LEARN_SOLUTION_MGR_NAME,
  URN.LEARN_SOLUTION_MGR_EMAIL AS LEARN_SOLUTION_MGR_EMAIL,
  ECR8.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Conv_REGION_8 ECR8
  LEFT JOIN USR_REGION_NAMES URN ON URN.REGION_ID = ECR8.o_REGION_ID"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("LKP_USR_REGION_NAMES_9")

# COMMAND ----------
# DBTITLE 1, EXP_Conv_DISTRICT_10


query_10 = f"""SELECT
  TO_INTEGER(DISTRICT_ID) AS o_DISTRICT_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_Detail_Outer_Join_4"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("EXP_Conv_DISTRICT_10")

# COMMAND ----------
# DBTITLE 1, LKP_USR_DISTRICT_NAMES_11


query_11 = f"""SELECT
  UDN.DM_NAME AS DM_NAME,
  UDN.DM_NAME_EMAIL AS DM_NAME_EMAIL,
  UDN.LP_SAFETY_NAME AS LP_SAFETY_NAME,
  UDN.LP_SAFETY_EMAIL AS LP_SAFETY_EMAIL,
  UDN.HR_SUPER_NAME AS HR_SUPER_NAME,
  UDN.HR_SUPER_EMAIL AS HR_SUPER_EMAIL,
  ECD1.o_DISTRICT_ID AS o_DISTRICT_ID,
  ECD1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Conv_DISTRICT_10 ECD1
  LEFT JOIN USR_DISTRICT_NAMES UDN ON UDN.DISTRICT_ID = ECD1.o_DISTRICT_ID"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("LKP_USR_DISTRICT_NAMES_11")

# COMMAND ----------
# DBTITLE 1, EXP_Default_12


query_12 = f"""SELECT
  JDOJ4.LOCATION_ID1 AS LOCATION_ID1,
  DECODE(
    TRUE,
    STORE_NBR1 <> 2940
    AND STORE_NBR1 <> 2941
    AND STORE_NBR1 >= 2900
    AND STORE_NBR1 <> 2956
    AND STORE_NBR1 <= 2999,
    15,
    STORE_NBR1 >= 3800,
    15,
    IN(
      STORE_NBR1,
      8,
      9,
      10,
      12,
      17,
      24,
      36,
      38,
      39,
      40,
      41,
      42,
      43,
      44,
      32,
      35,
      37,
      45,
      46,
      31,
      22,
      23
    ),
    1,
    STORE_NBR1 = 4,
    2,
    IN(STORE_NBR1, 14, 16, 18, 20, 21),
    3,
    STORE_TYPE_ID = 'MIX'
    OR STORE_TYPE_ID = 'WHL',
    4,
    IN(STORE_NBR1, 2000, 2030, 2050),
    5,
    IN(
      STORE_NBR1,
      2940,
      2941,
      2075,
      2801,
      2859,
      2877,
      2802,
      2956
    ),
    6,
    STORE_NBR1 = 2077,
    7,
    STORE_NBR1 = 2010
    OR STORE_NBR1 = 2012
    OR STORE_NBR1 = 2014
    OR STORE_NBR1 = 2095,
    9,
    STORE_NBR1 = 876,
    16,
    (
      (
        STORE_NBR1 >= 800
        AND STORE_NBR1 <= 899
      )
      AND (
        REGION_ID > 3000
        OR REGION_ID = 0
      )
    )
    OR STORE_NBR1 = 1000,
    15,
    STORE_TYPE_ID = '120',
    8,
    STORE_NBR1 = 2071,
    17,
    STORE_NBR1 = 2073,
    18,
    99
  ) AS LOCATION_TYPE_ID,
  DECODE(
    TRUE,
    JDOJ4.STORE_NBR <> 2940
    AND JDOJ4.STORE_NBR <> 2941
    AND JDOJ4.STORE_NBR >= 2900
    AND JDOJ4.STORE_NBR <> 2956
    AND JDOJ4.STORE_NBR <= 2999,
    15,
    JDOJ4.STORE_NBR >= 3800,
    15,
    IN(
      JDOJ4.STORE_NBR,
      8,
      9,
      10,
      12,
      17,
      24,
      36,
      38,
      39,
      40,
      41,
      42,
      43,
      44,
      32,
      35,
      37,
      45,
      46,
      31,
      22,
      23
    ),
    1,
    JDOJ4.STORE_NBR = 4,
    2,
    IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
    3,
    JDOJ4.STORE_TYPE_ID = 'MIX'
    OR JDOJ4.STORE_TYPE_ID = 'WHL',
    4,
    IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
    5,
    IN(
      JDOJ4.STORE_NBR,
      2940,
      2941,
      2075,
      2801,
      2859,
      2877,
      2802,
      2956
    ),
    6,
    JDOJ4.STORE_NBR = 2077,
    7,
    JDOJ4.STORE_NBR = 2010
    OR JDOJ4.STORE_NBR = 2012
    OR JDOJ4.STORE_NBR = 2014
    OR JDOJ4.STORE_NBR = 2095,
    9,
    JDOJ4.STORE_NBR = 876,
    16,
    (
      (
        JDOJ4.STORE_NBR >= 800
        AND JDOJ4.STORE_NBR <= 899
      )
      AND (
        JDOJ4.REGION_ID > 3000
        OR JDOJ4.REGION_ID = 0
      )
    )
    OR JDOJ4.STORE_NBR = 1000,
    15,
    JDOJ4.STORE_TYPE_ID = '120',
    8,
    JDOJ4.STORE_NBR = 2071,
    17,
    JDOJ4.STORE_NBR = 2073,
    18,
    99
  ) AS o_LOCATION_TYPE_ID,
  JDOJ4.STORE_NBR AS STORE_NBR1,
  JDOJ4.STORE_NAME AS STORE_NAME,
  JDOJ4.STORE_TYPE_ID AS STORE_TYPE_ID,
  JDOJ4.STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  JDOJ4.COMPANY_ID AS COMPANY_ID,
  JDOJ4.REGION_ID AS REGION_ID,
  JDOJ4.DISTRICT_ID AS DISTRICT_ID,
  JDOJ4.PRICE_ZONE_ID AS PRICE_ZONE_ID,
  JDOJ4.PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
  JDOJ4.REPL_DC_NBR AS REPL_DC_NBR,
  JDOJ4.REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
  JDOJ4.REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
  JDOJ4.SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  JDOJ4.SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  JDOJ4.SITE_ADDRESS AS SITE_ADDRESS,
  JDOJ4.SITE_CITY AS SITE_CITY,
  JDOJ4.STATE_CD AS STATE_CD,
  JDOJ4.COUNTRY_CD AS COUNTRY_CD,
  JDOJ4.POSTAL_CD AS POSTAL_CD,
  JDOJ4.SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
  JDOJ4.SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
  IFF(
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 8,
    DECODE(
      TRUE,
      DECODE(
        TRUE,
        JDOJ4.STORE_NBR <> 2940
        AND JDOJ4.STORE_NBR <> 2941
        AND JDOJ4.STORE_NBR >= 2900
        AND JDOJ4.STORE_NBR <> 2956
        AND JDOJ4.STORE_NBR <= 2999,
        15,
        JDOJ4.STORE_NBR >= 3800,
        15,
        IN(
          JDOJ4.STORE_NBR,
          8,
          9,
          10,
          12,
          17,
          24,
          36,
          38,
          39,
          40,
          41,
          42,
          43,
          44,
          32,
          35,
          37,
          45,
          46,
          31,
          22,
          23
        ),
        1,
        JDOJ4.STORE_NBR = 4,
        2,
        IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
        3,
        JDOJ4.STORE_TYPE_ID = 'MIX'
        OR JDOJ4.STORE_TYPE_ID = 'WHL',
        4,
        IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
        5,
        IN(
          JDOJ4.STORE_NBR,
          2940,
          2941,
          2075,
          2801,
          2859,
          2877,
          2802,
          2956
        ),
        6,
        JDOJ4.STORE_NBR = 2077,
        7,
        JDOJ4.STORE_NBR = 2010
        OR JDOJ4.STORE_NBR = 2012
        OR JDOJ4.STORE_NBR = 2014
        OR JDOJ4.STORE_NBR = 2095,
        9,
        JDOJ4.STORE_NBR = 876,
        16,
        (
          (
            JDOJ4.STORE_NBR >= 800
            AND JDOJ4.STORE_NBR <= 899
          )
          AND (
            JDOJ4.REGION_ID > 3000
            OR JDOJ4.REGION_ID = 0
          )
        )
        OR JDOJ4.STORE_NBR = 1000,
        15,
        JDOJ4.STORE_TYPE_ID = '120',
        8,
        JDOJ4.STORE_NBR = 2071,
        17,
        JDOJ4.STORE_NBR = 2073,
        18,
        99
      ) = 8
      AND JDOJ4.STORE_NBR >= 1
      AND JDOJ4.STORE_NBR <= 9,
      'M000' || JDOJ4.STORE_NBR,
      DECODE(
        TRUE,
        JDOJ4.STORE_NBR <> 2940
        AND JDOJ4.STORE_NBR <> 2941
        AND JDOJ4.STORE_NBR >= 2900
        AND JDOJ4.STORE_NBR <> 2956
        AND JDOJ4.STORE_NBR <= 2999,
        15,
        JDOJ4.STORE_NBR >= 3800,
        15,
        IN(
          JDOJ4.STORE_NBR,
          8,
          9,
          10,
          12,
          17,
          24,
          36,
          38,
          39,
          40,
          41,
          42,
          43,
          44,
          32,
          35,
          37,
          45,
          46,
          31,
          22,
          23
        ),
        1,
        JDOJ4.STORE_NBR = 4,
        2,
        IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
        3,
        JDOJ4.STORE_TYPE_ID = 'MIX'
        OR JDOJ4.STORE_TYPE_ID = 'WHL',
        4,
        IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
        5,
        IN(
          JDOJ4.STORE_NBR,
          2940,
          2941,
          2075,
          2801,
          2859,
          2877,
          2802,
          2956
        ),
        6,
        JDOJ4.STORE_NBR = 2077,
        7,
        JDOJ4.STORE_NBR = 2010
        OR JDOJ4.STORE_NBR = 2012
        OR JDOJ4.STORE_NBR = 2014
        OR JDOJ4.STORE_NBR = 2095,
        9,
        JDOJ4.STORE_NBR = 876,
        16,
        (
          (
            JDOJ4.STORE_NBR >= 800
            AND JDOJ4.STORE_NBR <= 899
          )
          AND (
            JDOJ4.REGION_ID > 3000
            OR JDOJ4.REGION_ID = 0
          )
        )
        OR JDOJ4.STORE_NBR = 1000,
        15,
        JDOJ4.STORE_TYPE_ID = '120',
        8,
        JDOJ4.STORE_NBR = 2071,
        17,
        JDOJ4.STORE_NBR = 2073,
        18,
        99
      ) = 8
      AND JDOJ4.STORE_NBR >= 10
      AND JDOJ4.STORE_NBR <= 99,
      'M00' || JDOJ4.STORE_NBR,
      DECODE(
        TRUE,
        JDOJ4.STORE_NBR <> 2940
        AND JDOJ4.STORE_NBR <> 2941
        AND JDOJ4.STORE_NBR >= 2900
        AND JDOJ4.STORE_NBR <> 2956
        AND JDOJ4.STORE_NBR <= 2999,
        15,
        JDOJ4.STORE_NBR >= 3800,
        15,
        IN(
          JDOJ4.STORE_NBR,
          8,
          9,
          10,
          12,
          17,
          24,
          36,
          38,
          39,
          40,
          41,
          42,
          43,
          44,
          32,
          35,
          37,
          45,
          46,
          31,
          22,
          23
        ),
        1,
        JDOJ4.STORE_NBR = 4,
        2,
        IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
        3,
        JDOJ4.STORE_TYPE_ID = 'MIX'
        OR JDOJ4.STORE_TYPE_ID = 'WHL',
        4,
        IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
        5,
        IN(
          JDOJ4.STORE_NBR,
          2940,
          2941,
          2075,
          2801,
          2859,
          2877,
          2802,
          2956
        ),
        6,
        JDOJ4.STORE_NBR = 2077,
        7,
        JDOJ4.STORE_NBR = 2010
        OR JDOJ4.STORE_NBR = 2012
        OR JDOJ4.STORE_NBR = 2014
        OR JDOJ4.STORE_NBR = 2095,
        9,
        JDOJ4.STORE_NBR = 876,
        16,
        (
          (
            JDOJ4.STORE_NBR >= 800
            AND JDOJ4.STORE_NBR <= 899
          )
          AND (
            JDOJ4.REGION_ID > 3000
            OR JDOJ4.REGION_ID = 0
          )
        )
        OR JDOJ4.STORE_NBR = 1000,
        15,
        JDOJ4.STORE_TYPE_ID = '120',
        8,
        JDOJ4.STORE_NBR = 2071,
        17,
        JDOJ4.STORE_NBR = 2073,
        18,
        99
      ) = 8
      AND JDOJ4.STORE_NBR >= 100
      AND JDOJ4.STORE_NBR <= 999,
      'M0' || JDOJ4.STORE_NBR,
      DECODE(
        TRUE,
        JDOJ4.STORE_NBR <> 2940
        AND JDOJ4.STORE_NBR <> 2941
        AND JDOJ4.STORE_NBR >= 2900
        AND JDOJ4.STORE_NBR <> 2956
        AND JDOJ4.STORE_NBR <= 2999,
        15,
        JDOJ4.STORE_NBR >= 3800,
        15,
        IN(
          JDOJ4.STORE_NBR,
          8,
          9,
          10,
          12,
          17,
          24,
          36,
          38,
          39,
          40,
          41,
          42,
          43,
          44,
          32,
          35,
          37,
          45,
          46,
          31,
          22,
          23
        ),
        1,
        JDOJ4.STORE_NBR = 4,
        2,
        IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
        3,
        JDOJ4.STORE_TYPE_ID = 'MIX'
        OR JDOJ4.STORE_TYPE_ID = 'WHL',
        4,
        IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
        5,
        IN(
          JDOJ4.STORE_NBR,
          2940,
          2941,
          2075,
          2801,
          2859,
          2877,
          2802,
          2956
        ),
        6,
        JDOJ4.STORE_NBR = 2077,
        7,
        JDOJ4.STORE_NBR = 2010
        OR JDOJ4.STORE_NBR = 2012
        OR JDOJ4.STORE_NBR = 2014
        OR JDOJ4.STORE_NBR = 2095,
        9,
        JDOJ4.STORE_NBR = 876,
        16,
        (
          (
            JDOJ4.STORE_NBR >= 800
            AND JDOJ4.STORE_NBR <= 899
          )
          AND (
            JDOJ4.REGION_ID > 3000
            OR JDOJ4.REGION_ID = 0
          )
        )
        OR JDOJ4.STORE_NBR = 1000,
        15,
        JDOJ4.STORE_TYPE_ID = '120',
        8,
        JDOJ4.STORE_NBR = 2071,
        17,
        JDOJ4.STORE_NBR = 2073,
        18,
        99
      ) = 8
      AND JDOJ4.STORE_NBR >= 1000
      AND JDOJ4.STORE_NBR <= 9999,
      'M' || JDOJ4.STORE_NBR,
      NULL
    ) || '@PETsMART.com',
    'Not Defined'
  ) AS o_SITE_EMAIL_ADDRESS,
  IFF(
    ISNULL(JDOJ4.SITE_SALES_FLAG),
    '0',
    JDOJ4.SITE_SALES_FLAG
  ) AS o_SITE_SALES_FLAG,
  0 AS EQUINE_SITE_ID,
  JDOJ4.BP_COMPANY_NBR AS BP_COMPANY_NBR,
  JDOJ4.BP_GL_ACCT AS BP_GL_ACCT,
  JDOJ4.PROMO_LABEL_CD AS PROMO_LABEL_CD,
  IFF(
    ISNULL(JDOJ4.PARENT_LOCATION_ID),
    90000,
    JDOJ4.PARENT_LOCATION_ID
  ) AS o_PARENT_LOCATION_ID,
  TO_CHAR(STORE_NBR1) AS v_STORE_NBR,
  IFF(
    ISNULL(JDOJ4.LOCATION_NBR),
    (LPAD(TO_CHAR(JDOJ4.STORE_NBR), 4, '0')),
    JDOJ4.LOCATION_NBR
  ) AS o_LOCATION_NBR,
  DECODE(
    TRUE,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 8
    AND JDOJ4.STORE_NBR >= 1
    AND JDOJ4.STORE_NBR <= 9,
    'M000' || JDOJ4.STORE_NBR,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 8
    AND JDOJ4.STORE_NBR >= 10
    AND JDOJ4.STORE_NBR <= 99,
    'M00' || JDOJ4.STORE_NBR,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 8
    AND JDOJ4.STORE_NBR >= 100
    AND JDOJ4.STORE_NBR <= 999,
    'M0' || JDOJ4.STORE_NBR,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 8
    AND JDOJ4.STORE_NBR >= 1000
    AND JDOJ4.STORE_NBR <= 9999,
    'M' || JDOJ4.STORE_NBR,
    NULL
  ) AS o_SITE_LOGIN_ID,
  DECODE(
    ISNULL(JDOJ4.SITE_MANAGER_ID),
    0,
    JDOJ4.SITE_MANAGER_ID
  ) AS o_SITE_MANAGER_ID,
  DECODE(
    TRUE,
    LOCATION_TYPE_ID = 8
    AND STORE_NBR1 >= 1
    AND STORE_NBR1 <= 9,
    'M000' || STORE_NBR1,
    LOCATION_TYPE_ID = 8
    AND STORE_NBR1 >= 10
    AND STORE_NBR1 <= 99,
    'M00' || STORE_NBR1,
    LOCATION_TYPE_ID = 8
    AND STORE_NBR1 >= 100
    AND STORE_NBR1 <= 999,
    'M0' || STORE_NBR1,
    LOCATION_TYPE_ID = 8
    AND STORE_NBR1 >= 1000
    AND STORE_NBR1 <= 9999,
    'M' || STORE_NBR1,
    NULL
  ) AS v_SITE_LOGIN_ID,
  DECODE(
    TRUE,
    JDOJ4.OPEN_DT = TO_DATE('99991231', 'YYYYMMDD'),
    0,
    ROUND(DATE_DIFF(now(), JDOJ4.OPEN_DT, 'MM') / 12, 1)
  ) AS o_SITE_OPEN_YRS_AMT,
  LUDN1.DM_NAME AS DM_NAME,
  LURN9.RVP_NAME AS RVP_NAME,
  JDOJ4.SFT_OPEN_DT AS SFT_OPEN_DT,
  LUDN1.LP_SAFETY_NAME AS LP_SAFETY_NAME,
  DECODE(
    TRUE,
    NOT ISNULL(LSTSP7.BEZEI),
    LSTSP7.BEZEI,
    ISNULL (LSTSP7.BEZEI)
    AND JDOJ4.SITE_COUNTY <> 'Not Defined',
    JDOJ4.SITE_COUNTY,
    'Not Defined'
  ) AS o_SITE_COUNTY,
  '0' AS SITE_FAX_NO,
  LUDN1.DM_NAME_EMAIL AS DM_NAME_EMAIL,
  LURN9.RVP_EMAIL AS RVP_EMAIL,
  LURN9.RETAIL_SAFETY_MGR_NAME AS RETAIL_SAFETY_MGR_NAME,
  LURN9.RETAIL_SAFETY_MGR_EMAIL AS RETAIL_SAFETY_MGR_EMAIL,
  DECODE(
    TRUE,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 1,
    LUDN1.DM_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 2,
    LUDN1.DM_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 3,
    LUDN1.DM_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 4,
    LUDN1.DM_NAME,
    ''
  ) AS AREA_DIRECTOR_NAME,
  DECODE(
    TRUE,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 1,
    LUDN1.DM_NAME_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 2,
    LUDN1.DM_NAME_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 3,
    LUDN1.DM_NAME_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 4,
    LUDN1.DM_NAME_EMAIL,
    ''
  ) AS AREA_DIRECTOR_EMAIL,
  LUDN5.DC_GM_NAME AS DC_GM_NAME,
  LUDN5.DC_GM_EMAIL AS DC_GM_EMAIL,
  LUDN5.DC_ASST_GM1_NAME AS DC_ASST_GM1_NAME,
  LUDN5.DC_ASST_GM1_EMAIL AS DC_ASST_GM1_EMAIL,
  LUDN5.DC_ASST_GM2_NAME AS DC_ASST_GM2_NAME,
  LUDN5.DC_ASST_GM2_EMAIL AS DC_ASST_GM2_EMAIL,
  LUDN5.DC_RLPM_NAME AS DC_RLPM_NAME,
  LUDN5.DC_RLPM_EMAIL AS DC_RLPM_EMAIL,
  LUDN5.DC_HR_SUPER1_NAME AS DC_HR_SUPER1_NAME,
  LUDN5.DC_HR_SUPER1_EMAIL AS DC_HR_SUPER1_EMAIL,
  LUDN5.DC_HR_SUPER2_NAME AS DC_HR_SUPER2_NAME,
  LUDN5.DC_HR_SUPER2_EMAIL AS DC_HR_SUPER2_EMAIL,
  LUDN5.DC_HR_MGR_NAME AS DC_HR_MANAGER_NAME,
  LUDN5.DC_HR_MGR_EMAIL AS DC_HR_MANAGER_EMAIL,
  DECODE(
    TRUE,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 1,
    LUDN5.DC_HR_MGR_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 2,
    LUDN5.DC_HR_MGR_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 3,
    LUDN5.DC_HR_MGR_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 4,
    LUDN5.DC_HR_MGR_NAME,
    LURN9.R_PEOPLE_DIR_NAME
  ) AS o_PEOPLE_MANAGER_NAME,
  DECODE(
    TRUE,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 1,
    LUDN5.DC_HR_MGR_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 2,
    LUDN5.DC_HR_MGR_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 3,
    LUDN5.DC_HR_MGR_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 4,
    LUDN5.DC_HR_MGR_EMAIL,
    LURN9.R_PEOPLE_DIR_EMAIL
  ) AS o_PEOPLE_MANAGER_EMAIL,
  LURN9.RD_NAME AS RD_NAME,
  LURN9.RD_EMAIL AS RD_EMAIL,
  LURN9.R_PEOPLE_DIR_NAME AS R_PEOPLE_DIR_NAME,
  LURN9.R_PEOPLE_DIR_EMAIL AS R_PEOPLE_DIR_EMAIL,
  LURN9.LP_SAFETY_DIR_NAME AS LP_SAFETY_DIR_NAME,
  LURN9.LP_SAFETY_DIR_EMAIL AS LP_SAFETY_DIR_EMAIL,
  LURN9.SR_LP_SAFETY_MGR_NAME AS SR_LP_SAFETY_MGR_NAME,
  LURN9.SR_LP_SAFETY_MGR_EMAIL AS SR_LP_SAFETY_MGR_EMAIL,
  DECODE(
    TRUE,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 1,
    LUDN5.DC_RLPM_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 2,
    LUDN5.DC_RLPM_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 3,
    LUDN5.DC_RLPM_NAME,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 4,
    LUDN5.DC_RLPM_NAME,
    LURN9.LP_SAFETY_DIR_NAME
  ) AS o_REG_ASSET_PROT_MGR_NAME,
  DECODE(
    TRUE,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 1,
    LUDN5.DC_RLPM_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 2,
    LUDN5.DC_RLPM_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 1,
    LUDN5.DC_RLPM_EMAIL,
    DECODE(
      TRUE,
      JDOJ4.STORE_NBR <> 2940
      AND JDOJ4.STORE_NBR <> 2941
      AND JDOJ4.STORE_NBR >= 2900
      AND JDOJ4.STORE_NBR <> 2956
      AND JDOJ4.STORE_NBR <= 2999,
      15,
      JDOJ4.STORE_NBR >= 3800,
      15,
      IN(
        JDOJ4.STORE_NBR,
        8,
        9,
        10,
        12,
        17,
        24,
        36,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        32,
        35,
        37,
        45,
        46,
        31,
        22,
        23
      ),
      1,
      JDOJ4.STORE_NBR = 4,
      2,
      IN(JDOJ4.STORE_NBR, 14, 16, 18, 20, 21),
      3,
      JDOJ4.STORE_TYPE_ID = 'MIX'
      OR JDOJ4.STORE_TYPE_ID = 'WHL',
      4,
      IN(JDOJ4.STORE_NBR, 2000, 2030, 2050),
      5,
      IN(
        JDOJ4.STORE_NBR,
        2940,
        2941,
        2075,
        2801,
        2859,
        2877,
        2802,
        2956
      ),
      6,
      JDOJ4.STORE_NBR = 2077,
      7,
      JDOJ4.STORE_NBR = 2010
      OR JDOJ4.STORE_NBR = 2012
      OR JDOJ4.STORE_NBR = 2014
      OR JDOJ4.STORE_NBR = 2095,
      9,
      JDOJ4.STORE_NBR = 876,
      16,
      (
        (
          JDOJ4.STORE_NBR >= 800
          AND JDOJ4.STORE_NBR <= 899
        )
        AND (
          JDOJ4.REGION_ID > 3000
          OR JDOJ4.REGION_ID = 0
        )
      )
      OR JDOJ4.STORE_NBR = 1000,
      15,
      JDOJ4.STORE_TYPE_ID = '120',
      8,
      JDOJ4.STORE_NBR = 2071,
      17,
      JDOJ4.STORE_NBR = 2073,
      18,
      99
    ) = 1,
    LUDN5.DC_RLPM_EMAIL,
    LURN9.LP_SAFETY_DIR_EMAIL
  ) AS o_REG_ASSET_PROT_MGR_EMAIL,
  LUDN1.LP_SAFETY_EMAIL AS LP_SAFETY_EMAIL,
  LUDN1.HR_SUPER_NAME AS HR_SUPER_NAME,
  LUDN1.HR_SUPER_EMAIL AS HR_SUPER_EMAIL,
  LURN9.LEARN_SOLUTION_MGR_NAME AS LEARN_SOLUTION_MGR_NAME,
  LURN9.LEARN_SOLUTION_MGR_EMAIL AS LEARN_SOLUTION_MGR_EMAIL,
  JDOJ4.OPEN_DT AS OPEN_DT,
  JDOJ4.GR_OPEN_DT AS GR_OPEN_DT,
  JDOJ4.CLOSE_DT AS CLOSE_DT,
  JDOJ4.ADD_DT AS ADD_DT,
  JDOJ4.DELETE_DT AS DELETE_DT,
  JDOJ4.LOAD_DT AS LOAD_DT,
  LUDN1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_USR_DISTRICT_NAMES_11 LUDN1
  INNER JOIN LKP_USR_REGION_NAMES_9 LURN9 ON LUDN1.Monotonically_Increasing_Id = LURN9.Monotonically_Increasing_Id
  INNER JOIN JNR_Detail_Outer_Join_4 JDOJ4 ON LURN9.Monotonically_Increasing_Id = JDOJ4.Monotonically_Increasing_Id
  INNER JOIN LKP_SAP_T001W_SITE_PRE_7 LSTSP7 ON JDOJ4.Monotonically_Increasing_Id = LSTSP7.Monotonically_Increasing_Id
  INNER JOIN LKP_USR_DC_NAMES_5 LUDN5 ON LSTSP7.Monotonically_Increasing_Id = LUDN5.Monotonically_Increasing_Id"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("EXP_Default_12")

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
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID1 AS LOCATION_ID,
  LOCATION_TYPE_ID1 AS LOCATION_TYPE_ID,
  o_LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  STORE_NBR1 AS STORE_NBR,
  STORE_NBR1 AS STORE_NBR,
  VENDOR_NAME AS STORE_NAME,
  VENDOR_NAME AS STORE_NAME,
  STORE_NAME AS STORE_NAME,
  STORE_TYPE_ID1 AS STORE_TYPE_ID,
  STORE_TYPE_ID AS STORE_TYPE_ID,
  STORE_OPEN_CLOSE_FLAG1 AS STORE_OPEN_CLOSE_FLAG,
  STORE_OPEN_CLOSE_FLAG AS STORE_OPEN_CLOSE_FLAG,
  COMPANY_ID1 AS COMPANY_ID,
  COMPANY_ID AS COMPANY_ID,
  ZERO_DEFAULT1 AS REGION_ID,
  REGION_ID AS REGION_ID,
  ZERO_DEFAULT1 AS DISTRICT_ID,
  DISTRICT_ID AS DISTRICT_ID,
  ZERO_DEFAULT1 AS PRICE_ZONE_ID,
  PRICE_ZONE_ID AS PRICE_ZONE_ID,
  ZERO_DEFAULT1 AS PRICE_AD_ZONE_ID,
  PRICE_AD_ZONE_ID AS PRICE_AD_ZONE_ID,
  ZERO_DEFAULT1 AS REPL_DC_NBR,
  REPL_DC_NBR AS REPL_DC_NBR,
  ZERO_DEFAULT1 AS REPL_FISH_DC_NBR,
  REPL_FISH_DC_NBR AS REPL_FISH_DC_NBR,
  ZERO_DEFAULT1 AS REPL_FWD_DC_NBR,
  REPL_FWD_DC_NBR AS REPL_FWD_DC_NBR,
  ZERO_DEFAULT1 AS SQ_FEET_RETAIL,
  SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  SQ_FEET_RETAIL AS SQ_FEET_RETAIL,
  ZERO_DEFAULT1 AS SQ_FEET_TOTAL,
  SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  SQ_FEET_TOTAL AS SQ_FEET_TOTAL,
  ADDRESS AS SITE_ADDRESS,
  ADDRESS AS SITE_ADDRESS,
  SITE_ADDRESS AS SITE_ADDRESS,
  CITY AS SITE_CITY,
  CITY AS SITE_CITY,
  SITE_CITY AS SITE_CITY,
  STATE AS STATE_CD,
  STATE AS STATE_CD,
  STATE_CD AS STATE_CD,
  COUNTRY_CD AS COUNTRY_CD,
  COUNTRY_CD AS COUNTRY_CD,
  COUNTRY_CD AS COUNTRY_CD,
  ZIP AS POSTAL_CD,
  ZIP AS POSTAL_CD,
  POSTAL_CD AS POSTAL_CD,
  PHONE AS SITE_MAIN_TELE_NO,
  PHONE AS SITE_MAIN_TELE_NO,
  SITE_MAIN_TELE_NO AS SITE_MAIN_TELE_NO,
  ZERO_DEFAULT1 AS SITE_GROOM_TELE_NO,
  SITE_GROOM_TELE_NO AS SITE_GROOM_TELE_NO,
  DEFAULT_NOT_DEFINED1 AS SITE_EMAIL_ADDRESS,
  DC_GM_EMAIL AS SITE_EMAIL_ADDRESS,
  o_SITE_EMAIL_ADDRESS AS SITE_EMAIL_ADDRESS,
  ZERO_DEFAULT1 AS SITE_SALES_FLAG,
  o_SITE_SALES_FLAG AS SITE_SALES_FLAG,
  EQUINE_MERCH_ID1 AS EQUINE_MERCH_ID,
  EQUINE_MERCH_ID AS EQUINE_MERCH_ID,
  ZERO_DEFAULT1 AS EQUINE_SITE_ID,
  EQUINE_SITE_ID_SQL AS EQUINE_SITE_ID,
  EQUINE_SITE_ID AS EQUINE_SITE_ID,
  DEFAULT_DT1 AS EQUINE_SITE_OPEN_DT,
  NULL AS GEO_LATITUDE_NBR,
  NULL AS GEO_LONGITUDE_NBR,
  ZERO_DEFAULT1 AS PETSMART_DMA_CD,
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  ZERO_DEFAULT1 AS LOYALTY_PGM_TYPE_ID,
  IN_LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  ZERO_DEFAULT1 AS LOYALTY_PGM_STATUS_ID,
  IN_LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  DEFAULT_DT1 AS LOYALTY_PGM_START_DT,
  IN_LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
  DEFAULT_DT1 AS LOYALTY_PGM_CHANGE_DT,
  IN_LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
  ZERO_DEFAULT1 AS BP_COMPANY_NBR,
  BP_COMPANY_NBR AS BP_COMPANY_NBR,
  ZERO_DEFAULT1 AS BP_GL_ACCT,
  BP_GL_ACCT AS BP_GL_ACCT,
  TP_LOC_FLAG1 AS TP_LOC_FLAG,
  ZERO_DEFAULT1 AS TP_ACTIVE_CNT,
  ZERO_DEFAULT1 AS PROMO_LABEL_CD,
  PROMO_LABEL_CD AS PROMO_LABEL_CD,
  PARENT_VENDOR_ID AS PARENT_LOCATION_ID,
  PARENT_VENDOR_ID AS PARENT_LOCATION_ID,
  o_PARENT_LOCATION_ID AS PARENT_LOCATION_ID,
  VENDOR_NBR AS LOCATION_NBR,
  VENDOR_NBR AS LOCATION_NBR,
  o_LOCATION_NBR AS LOCATION_NBR,
  ZERO_DEFAULT1 AS TIME_ZONE_ID,
  TimeZoneDesc AS TIME_ZONE_ID,
  ZERO_DEFAULT1 AS DELV_SERVICE_CLASS_ID,
  ZERO_DEFAULT1 AS PICK_SERVICE_CLASS_ID,
  ZERO_DEFAULT1 AS SITE_LOGIN_ID,
  o_SITE_LOGIN_ID AS SITE_LOGIN_ID,
  ZERO_DEFAULT1 AS SITE_MANAGER_ID,
  EMPLOYEE_ID AS SITE_MANAGER_ID,
  EMPLOYEE_ID AS SITE_MANAGER_ID,
  o_SITE_MANAGER_ID AS SITE_MANAGER_ID,
  ZERO_DEFAULT1 AS SITE_OPEN_YRS_AMT,
  o_SITE_OPEN_YRS_AMT AS SITE_OPEN_YRS_AMT,
  ZERO_DEFAULT1 AS HOTEL_FLAG,
  HOTEL_FLAG AS HOTEL_FLAG,
  ZERO_DEFAULT1 AS DAYCAMP_FLAG,
  DAYCAMP_FLAG AS DAYCAMP_FLAG,
  ZERO_DEFAULT1 AS VET_FLAG,
  DEFAULT_NOT_DEFINED1 AS DIST_MGR_NAME,
  Output_DM_NAME AS DIST_MGR_NAME,
  DM_NAME AS DIST_MGR_NAME,
  DEFAULT_NOT_DEFINED1 AS DIST_SVC_MGR_NAME,
  DEFAULT_NOT_DEFINED1 AS REGION_VP_NAME,
  RVP_NAME AS REGION_VP_NAME,
  DEFAULT_NOT_DEFINED1 AS REGION_TRAINER_NAME,
  DEFAULT_NOT_DEFINED1 AS ASSET_PROTECT_NAME,
  LP_SAFETY_NAME AS ASSET_PROTECT_NAME,
  o_SITE_COUNTY AS SITE_COUNTY,
  SITE_FAX_NO AS SITE_FAX_NO,
  SFT_OPEN_DT AS SFT_OPEN_DT,
  Output_DM_EMAIL AS DM_EMAIL_ADDRESS,
  DM_NAME_EMAIL AS DM_EMAIL_ADDRESS,
  NULL AS DSM_EMAIL_ADDRESS,
  RVP_EMAIL AS RVP_EMAIL_ADDRESS,
  NULL AS TRADE_AREA,
  NULL AS FDLPS_NAME,
  NULL AS FDLPS_EMAIL,
  NULL AS OVERSITE_MGR_NAME,
  NULL AS OVERSITE_MGR_EMAIL,
  LP_SAFETY_DIR_NAME AS SAFETY_DIRECTOR_NAME,
  LP_SAFETY_DIR_EMAIL AS SAFETY_DIRECTOR_EMAIL,
  RETAIL_SAFETY_MGR_NAME AS RETAIL_MANAGER_SAFETY_NAME,
  RETAIL_SAFETY_MGR_EMAIL AS RETAIL_MANAGER_SAFETY_EMAIL,
  AREA_DIRECTOR_NAME AS AREA_DIRECTOR_NAME,
  AREA_DIRECTOR_EMAIL AS AREA_DIRECTOR_EMAIL,
  DC_GM_NAME AS DC_GENERAL_MANAGER_NAME,
  DC_GM_EMAIL AS DC_GENERAL_MANAGER_EMAIL,
  DC_ASST_GM1_NAME AS ASST_DC_GENERAL_MANAGER_NAME1,
  DC_ASST_GM1_EMAIL AS ASST_DC_GENERAL_MANAGER_EMAIL1,
  DC_ASST_GM2_NAME AS ASST_DC_GENERAL_MANAGER_NAME2,
  DC_ASST_GM2_EMAIL AS ASST_DC_GENERAL_MANAGER_EMAIL2,
  DC_RLPM_NAME AS REGIONAL_DC_SAFETY_MGR_NAME,
  DC_RLPM_EMAIL AS REGIONAL_DC_SAFETY_MGR_EMAIL,
  DC_HR_SUPER1_NAME AS DC_PEOPLE_SUPERVISOR_NAME,
  DC_HR_SUPER1_EMAIL AS DC_PEOPLE_SUPERVISOR_EMAIL,
  o_PEOPLE_MANAGER_NAME AS PEOPLE_MANAGER_NAME,
  o_PEOPLE_MANAGER_EMAIL AS PEOPLE_MANAGER_EMAIL,
  LP_SAFETY_DIR_NAME AS ASSET_PROT_DIR_NAME,
  LP_SAFETY_DIR_EMAIL AS ASSET_PROT_DIR_EMAIL,
  SR_LP_SAFETY_MGR_NAME AS SR_REG_ASSET_PROT_MGR_NAME,
  SR_LP_SAFETY_MGR_EMAIL AS SR_REG_ASSET_PROT_MGR_EMAIL,
  o_REG_ASSET_PROT_MGR_NAME AS REG_ASSET_PROT_MGR_NAME,
  o_REG_ASSET_PROT_MGR_EMAIL AS REG_ASSET_PROT_MGR_EMAIL,
  LP_SAFETY_EMAIL AS ASSET_PROTECT_EMAIL,
  DEFAULT_DT1 AS TP_START_DT,
  DEFAULT_DT1 AS OPEN_DT,
  OPEN_DT AS OPEN_DT,
  DEFAULT_DT1 AS GR_OPEN_DT,
  GR_OPEN_DT AS GR_OPEN_DT,
  DEFAULT_DT1 AS CLOSE_DT,
  CLOSE_DT AS CLOSE_DT,
  DEFAULT_DT1 AS HOTEL_OPEN_DT,
  ADD_DT AS ADD_DT,
  ADD_DT AS ADD_DT,
  DEFAULT_DT1 AS DELETE_DT,
  DELETE_DT AS DELETE_DT,
  UPDATE_DT AS UPDATE_DT,
  UPDATE_DT AS UPDATE_DT,
  Update_Dt AS UPDATE_DT,
  Update_DT AS UPDATE_DT,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  LOAD_DT AS LOAD_DT
FROM
  EXP_Default_12""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_profile")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_site_profile", mainWorkflowId, parentName)
