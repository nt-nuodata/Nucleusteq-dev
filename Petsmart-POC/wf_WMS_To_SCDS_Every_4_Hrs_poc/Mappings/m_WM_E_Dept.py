# Databricks notebook source
# %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------

# DBTITLE 1, Shortcut_to_WM_E_DEPT_PRE_0

df_0 = spark.sql("""SELECT
  DC_NBR AS DC_NBR,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  WM_E_DEPT_PRE""")

df_0.createOrReplaceTempView("Shortcut_to_WM_E_DEPT_PRE_0")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_WM_E_DEPT_PRE_1

df_1 = spark.sql("""SELECT
  DC_NBR AS DC_NBR,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WM_E_DEPT_PRE_0""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_WM_E_DEPT_PRE_1")

# COMMAND ----------

# DBTITLE 1, EXP_INT_CONV_2

df_2 = spark.sql("""SELECT
  TO_INTEGER(DC_NBR) AS DC_NBR,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_WM_E_DEPT_PRE_1""")

df_2.createOrReplaceTempView("EXP_INT_CONV_2")

# COMMAND ----------

# DBTITLE 1, Shortcut_to_WM_E_DEPT1_3

df_3 = spark.sql("""SELECT
  LOCATION_ID AS LOCATION_ID,
  WM_DEPT_ID AS WM_DEPT_ID,
  WM_WHSE AS WM_WHSE,
  WM_DEPT_CD AS WM_DEPT_CD,
  WM_DEPT_DESC AS WM_DEPT_DESC,
  PERF_GOAL AS PERF_GOAL,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  WM_USER_ID AS WM_USER_ID,
  WM_VERSION_ID AS WM_VERSION_ID,
  WM_CREATED_TSTMP AS WM_CREATED_TSTMP,
  WM_LAST_UPDATED_TSTMP AS WM_LAST_UPDATED_TSTMP,
  WM_CREATE_TSTMP AS WM_CREATE_TSTMP,
  WM_MOD_TSTMP AS WM_MOD_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  WM_E_DEPT""")

df_3.createOrReplaceTempView("Shortcut_to_WM_E_DEPT1_3")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_WM_E_DEPT_4

df_4 = spark.sql("""SELECT
  LOCATION_ID AS LOCATION_ID,
  WM_DEPT_ID AS WM_DEPT_ID,
  WM_DEPT_CD AS WM_DEPT_CD,
  WM_DEPT_DESC AS WM_DEPT_DESC,
  WM_WHSE AS WM_WHSE,
  PERF_GOAL AS PERF_GOAL,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  WM_USER_ID AS WM_USER_ID,
  WM_VERSION_ID AS WM_VERSION_ID,
  WM_CREATED_TSTMP AS WM_CREATED_TSTMP,
  WM_LAST_UPDATED_TSTMP AS WM_LAST_UPDATED_TSTMP,
  WM_CREATE_TSTMP AS WM_CREATE_TSTMP,
  WM_MOD_TSTMP AS WM_MOD_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WM_E_DEPT1_3
WHERE
  WM_DEPT_ID IN (
    SELECT
      DEPT_ID
    FROM
      WM_E_DEPT_PRE
  )""")

df_4.createOrReplaceTempView("SQ_Shortcut_to_WM_E_DEPT_4")

# COMMAND ----------

# DBTITLE 1, Shortcut_to_SITE_PROFILE_5

df_5 = spark.sql("""SELECT
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
  SITE_PROFILE""")

df_5.createOrReplaceTempView("Shortcut_to_SITE_PROFILE_5")

# COMMAND ----------

# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_6

df_6 = spark.sql("""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_PROFILE_5""")

df_6.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_6")

# COMMAND ----------

# DBTITLE 1, JNR_SITE_PROFILE_7

df_7 = spark.sql("""SELECT
  DETAIL.DC_NBR AS DC_NBR,
  DETAIL.DEPT_ID AS DEPT_ID,
  DETAIL.DEPT_CODE AS DEPT_CODE,
  DETAIL.DESCRIPTION AS DESCRIPTION,
  DETAIL.CREATE_DATE_TIME AS CREATE_DATE_TIME,
  DETAIL.MOD_DATE_TIME AS MOD_DATE_TIME,
  DETAIL.USER_ID AS USER_ID,
  DETAIL.WHSE AS WHSE,
  DETAIL.MISC_TXT_1 AS MISC_TXT_1,
  DETAIL.MISC_TXT_2 AS MISC_TXT_2,
  DETAIL.MISC_NUM_1 AS MISC_NUM_1,
  DETAIL.MISC_NUM_2 AS MISC_NUM_2,
  DETAIL.PERF_GOAL AS PERF_GOAL,
  DETAIL.VERSION_ID AS VERSION_ID,
  DETAIL.CREATED_DTTM AS CREATED_DTTM,
  DETAIL.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  MASTER.LOCATION_ID AS LOCATION_ID,
  MASTER.STORE_NBR AS STORE_NBR,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SITE_PROFILE_6 MASTER
  INNER JOIN EXP_INT_CONV_2 DETAIL ON MASTER.STORE_NBR = DETAIL.DC_NBR""")

df_7.createOrReplaceTempView("JNR_SITE_PROFILE_7")

# COMMAND ----------

df_7.display()
df_7.count()

# COMMAND ----------

# DBTITLE 1, JNR_WM_E_DEPT_8

df_8 = spark.sql("""SELECT
  DETAIL.LOCATION_ID AS LOCATION_ID,
  DETAIL.DEPT_ID AS DEPT_ID,
  DETAIL.DEPT_CODE AS DEPT_CODE,
  DETAIL.DESCRIPTION AS DESCRIPTION,
  DETAIL.CREATE_DATE_TIME AS CREATE_DATE_TIME,
  DETAIL.MOD_DATE_TIME AS MOD_DATE_TIME,
  DETAIL.USER_ID AS USER_ID,
  DETAIL.WHSE AS WHSE,
  DETAIL.MISC_TXT_1 AS MISC_TXT_1,
  DETAIL.MISC_TXT_2 AS MISC_TXT_2,
  DETAIL.MISC_NUM_1 AS MISC_NUM_1,
  DETAIL.MISC_NUM_2 AS MISC_NUM_2,
  DETAIL.PERF_GOAL AS PERF_GOAL,
  DETAIL.VERSION_ID AS VERSION_ID,
  DETAIL.CREATED_DTTM AS CREATED_DTTM,
  DETAIL.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  MASTER.LOCATION_ID AS in_LOCATION_ID,
  MASTER.WM_DEPT_ID AS in_WM_DEPT_ID,
  MASTER.LOAD_TSTMP AS in_LOAD_TSTMP,
  MASTER.WM_CREATE_TSTMP AS in_WM_CREATE_TSTMP,
  MASTER.WM_MOD_TSTMP AS in_WM_MOD_TSTMP,
  MASTER.WM_CREATED_TSTMP AS in_WM_CREATED_TSTMP,
  MASTER.WM_LAST_UPDATED_TSTMP AS in_WM_LAST_UPDATED_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_WM_E_DEPT_4 MASTER
  RIGHT JOIN JNR_SITE_PROFILE_7 DETAIL ON MASTER.LOCATION_ID = DETAIL.LOCATION_ID
  AND MASTER.WM_DEPT_ID = DETAIL.DEPT_ID""")

df_8.createOrReplaceTempView("JNR_WM_E_DEPT_8")

# COMMAND ----------

df_8.display()
df_8.count()

# COMMAND ----------

# DBTITLE 1, FIL_NO_CHANGE_REC_9

df_9 = spark.sql("""SELECT
  LOCATION_ID AS LOCATION_ID,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  in_WM_DEPT_ID AS in_WM_DEPT_ID,
  in_LOAD_TSTMP AS in_LOAD_TSTMP,
  in_WM_CREATE_TSTMP AS in_WM_CREATE_TSTMP,
  in_WM_MOD_TSTMP AS in_WM_MOD_TSTMP,
  in_WM_CREATED_TSTMP AS in_WM_CREATED_TSTMP,
  in_WM_LAST_UPDATED_TSTMP AS in_WM_LAST_UPDATED_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_WM_E_DEPT_8
WHERE
  ISNULL(in_WM_DEPT_ID)
  OR (
    NOT ISNULL(in_WM_DEPT_ID)
    AND (
      IFF (
        ISNULL(CREATE_DATE_TIME),
        TO_DATE('01/01/1900', 'MM/dd/yyyy'),
        CREATE_DATE_TIME
      ) != IFF (
        ISNULL(in_WM_CREATE_TSTMP),
        TO_DATE('01/01/1900', 'MM/dd/yyyy'),
        in_WM_CREATE_TSTMP
      )
      OR IFF (
        ISNULL(MOD_DATE_TIME),
        TO_DATE('01/01/1900', 'MM/dd/yyyy'),
        MOD_DATE_TIME
      ) != IFF (
        ISNULL(in_WM_MOD_TSTMP),
        TO_DATE('01/01/1900', 'MM/dd/yyyy'),
        in_WM_MOD_TSTMP
      )
      OR IFF (
        ISNULL(CREATED_DTTM),
        TO_DATE('01/01/1900', 'MM/dd/yyyy'),
        CREATED_DTTM
      ) != IFF (
        ISNULL(in_WM_CREATED_TSTMP),
        TO_DATE('01/01/1900', 'MM/dd/yyyy'),
        in_WM_CREATED_TSTMP
      )
      OR IFF (
        ISNULL(LAST_UPDATED_DTTM),
        TO_DATE('01/01/1900', 'MM/dd/yyyy'),
        LAST_UPDATED_DTTM
      ) != IFF (
        ISNULL(in_WM_LAST_UPDATED_TSTMP),
        TO_DATE('01/01/1900', 'MM/dd/yyyy'),
        in_WM_LAST_UPDATED_TSTMP
      )
    )
  )""")

df_9.createOrReplaceTempView("FIL_NO_CHANGE_REC_9")

# COMMAND ----------

df_9.display()
df_9.count()

# COMMAND ----------

# DBTITLE 1, EXP_EVAL_VALUES_10

df_10 = spark.sql("""SELECT
  LOCATION_ID AS LOCATION_ID,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  IFF(ISNULL(in_LOAD_TSTMP), now(), in_LOAD_TSTMP) AS LOAD_TSTMP,
  now() AS UPDATE_TSTMP,
  in_WM_DEPT_ID AS in_WM_DEPT_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_NO_CHANGE_REC_9""")

df_10.createOrReplaceTempView("EXP_EVAL_VALUES_10")

# COMMAND ----------

# DBTITLE 1, UPD_VALIDATE_11

df_11 = spark.sql("""SELECT
  LOCATION_ID AS LOCATION_ID,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  in_WM_DEPT_ID AS in_WM_DEPT_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(ISNULL(in_WM_DEPT_ID), 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_EVAL_VALUES_10""")

df_11.createOrReplaceTempView("UPD_VALIDATE_11")

# COMMAND ----------

df_11.display()

# COMMAND ----------

# DBTITLE 1, WM_E_DEPT

spark.sql("""MERGE INTO WM_E_DEPT AS TARGET
USING
  UPD_VALIDATE_11 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  AND TARGET.WM_DEPT_ID = SOURCE.DEPT_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID,
  TARGET.WM_DEPT_ID = SOURCE.DEPT_ID,
  TARGET.WM_WHSE = SOURCE.WHSE,
  TARGET.WM_DEPT_CD = SOURCE.DEPT_CODE,
  TARGET.WM_DEPT_DESC = SOURCE.DESCRIPTION,
  TARGET.PERF_GOAL = SOURCE.PERF_GOAL,
  TARGET.MISC_TXT_1 = SOURCE.MISC_TXT_1,
  TARGET.MISC_TXT_2 = SOURCE.MISC_TXT_2,
  TARGET.MISC_NUM_1 = SOURCE.MISC_NUM_1,
  TARGET.MISC_NUM_2 = SOURCE.MISC_NUM_2,
  TARGET.WM_USER_ID = SOURCE.USER_ID,
  TARGET.WM_VERSION_ID = SOURCE.VERSION_ID,
  TARGET.WM_CREATED_TSTMP = SOURCE.CREATED_DTTM,
  TARGET.WM_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM,
  TARGET.WM_CREATE_TSTMP = SOURCE.CREATE_DATE_TIME,
  TARGET.WM_MOD_TSTMP = SOURCE.MOD_DATE_TIME,
  TARGET.UPDATE_TSTMP = SOURCE.LOAD_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.UPDATE_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.WM_WHSE = SOURCE.WHSE
  AND TARGET.WM_DEPT_CD = SOURCE.DEPT_CODE
  AND TARGET.WM_DEPT_DESC = SOURCE.DESCRIPTION
  AND TARGET.PERF_GOAL = SOURCE.PERF_GOAL
  AND TARGET.MISC_TXT_1 = SOURCE.MISC_TXT_1
  AND TARGET.MISC_TXT_2 = SOURCE.MISC_TXT_2
  AND TARGET.MISC_NUM_1 = SOURCE.MISC_NUM_1
  AND TARGET.MISC_NUM_2 = SOURCE.MISC_NUM_2
  AND TARGET.WM_USER_ID = SOURCE.USER_ID
  AND TARGET.WM_VERSION_ID = SOURCE.VERSION_ID
  AND TARGET.WM_CREATED_TSTMP = SOURCE.CREATED_DTTM
  AND TARGET.WM_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM
  AND TARGET.WM_CREATE_TSTMP = SOURCE.CREATE_DATE_TIME
  AND TARGET.WM_MOD_TSTMP = SOURCE.MOD_DATE_TIME
  AND TARGET.UPDATE_TSTMP = SOURCE.LOAD_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.UPDATE_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.LOCATION_ID,
    TARGET.WM_DEPT_ID,
    TARGET.WM_WHSE,
    TARGET.WM_DEPT_CD,
    TARGET.WM_DEPT_DESC,
    TARGET.PERF_GOAL,
    TARGET.MISC_TXT_1,
    TARGET.MISC_TXT_2,
    TARGET.MISC_NUM_1,
    TARGET.MISC_NUM_2,
    TARGET.WM_USER_ID,
    TARGET.WM_VERSION_ID,
    TARGET.WM_CREATED_TSTMP,
    TARGET.WM_LAST_UPDATED_TSTMP,
    TARGET.WM_CREATE_TSTMP,
    TARGET.WM_MOD_TSTMP,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.LOCATION_ID,
    SOURCE.DEPT_ID,
    SOURCE.WHSE,
    SOURCE.DEPT_CODE,
    SOURCE.DESCRIPTION,
    SOURCE.PERF_GOAL,
    SOURCE.MISC_TXT_1,
    SOURCE.MISC_TXT_2,
    SOURCE.MISC_NUM_1,
    SOURCE.MISC_NUM_2,
    SOURCE.USER_ID,
    SOURCE.VERSION_ID,
    SOURCE.CREATED_DTTM,
    SOURCE.LAST_UPDATED_DTTM,
    SOURCE.CREATE_DATE_TIME,
    SOURCE.MOD_DATE_TIME,
    SOURCE.LOAD_TSTMP,
    SOURCE.UPDATE_TSTMP
  )""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM WM_E_DEPT;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM WM_E_DEPT;