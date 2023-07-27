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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_Hours_Day_Pre_Store")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Site_Hours_Day_Pre_Store", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, SQ_Shortcut_to_SITE_PROFILE_1


query_1 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_PROFILE_0
WHERE
  Shortcut_to_SITE_PROFILE_0.STORE_NBR = 133"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SITE_PROFILE_1")

# COMMAND ----------
# DBTITLE 1, Exp_Client1_2


query_2 = f"""SELECT
  {mPar_ClientId} AS client_id,
  {mPar_ClientSecret} AS client_secret,
  '1' AS field,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SITE_PROFILE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Exp_Client1_2")

# COMMAND ----------
# DBTITLE 1, HTTP_StoreHours_3


query_3 = f"""SELECT
  field AS null,
  client_id AS null,
  client_secret AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_Client1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("HTTP_StoreHours_3")

# COMMAND ----------
# DBTITLE 1, StoreHoursXML_NewAPIResponse_4


query_4 = f"""SELECT
  HTTPOUT AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  HTTP_StoreHours_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("StoreHoursXML_NewAPIResponse_4")

# COMMAND ----------
# DBTITLE 1, Exp_StoreHours_5


query_5 = f"""SELECT
  n3_StoreNumber0 AS n3_StoreNumber0,
  d2p1_ForDate AS d2p1_ForDate,
  d2p1_OpenTime AS d2p1_OpenTime,
  d2p1_CloseTime AS d2p1_CloseTime,
  d2p1_IsClosed AS d2p1_IsClosed,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  StoreHoursXML_NewAPIResponse_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Exp_StoreHours_5")

# COMMAND ----------
# DBTITLE 1, Exp_ServicesHours_6


query_6 = f"""SELECT
  n3_StoreNumber1 AS n3_StoreNumber1,
  n3_Name2 AS n3_Name2,
  n3_ForDate AS n3_ForDate,
  n3_OpenTime AS n3_OpenTime,
  n3_CloseTime AS n3_CloseTime,
  n3_IsClosed AS n3_IsClosed,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  StoreHoursXML_NewAPIResponse_4"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Exp_ServicesHours_6")

# COMMAND ----------
# DBTITLE 1, Unn_Store_ServiceHours_7


query_7 = f"""SELECT
  n3_StoreNumber1 AS n3_StoreNumber0,
  n3_ForDate AS d2p1_ForDate,
  n3_Name2 AS name,
  n3_CloseTime AS d2p1_CloseTime,
  n3_OpenTime AS d2p1_OpenTime,
  n3_IsClosed AS d2p1_IsClosed,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_ServicesHours_6
UNION ALL
SELECT
  n3_StoreNumber0 AS n3_StoreNumber0,
  d2p1_ForDate AS d2p1_ForDate,
  null AS name,
  d2p1_CloseTime AS d2p1_CloseTime,
  d2p1_OpenTime AS d2p1_OpenTime,
  d2p1_IsClosed AS d2p1_IsClosed,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_StoreHours_5"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Unn_Store_ServiceHours_7")

# COMMAND ----------
# DBTITLE 1, Fil_StoreHours_8


query_8 = f"""SELECT
  n3_StoreNumber0 AS n3_StoreNumber0,
  d2p1_ForDate AS d2p1_ForDate,
  name AS name,
  d2p1_CloseTime AS d2p1_CloseTime,
  d2p1_OpenTime AS d2p1_OpenTime,
  d2p1_IsClosed AS d2p1_IsClosed,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Unn_Store_ServiceHours_7
WHERE
  NOT ISNULL(n3_StoreNumber0)
  AND NOT ISNULL(d2p1_ForDate)"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("Fil_StoreHours_8")

# COMMAND ----------
# DBTITLE 1, Exp_Site_Hours_Day_Pre_9


query_9 = f"""SELECT
  TO_DATE(SUBSTR(d2p1_ForDate, 0, 10), 'YYYY-MM-DD') AS v_Day_Dt,
  TO_DATE(SUBSTR(d2p1_ForDate, 0, 10), 'YYYY-MM-DD') AS DAY_DT,
  LPAD(n3_StoreNumber0, 4, '0') AS LOCATION_NBR,
  8 AS LOCATION_TYPE_ID,
  IFF(ISNULL(name), 'Store', name) AS BUSINESS_AREA,
  IFF(UPPER(d2p1_IsClosed) = 'FALSE', 0, 1) AS v_Is_Closed,
  IFF(UPPER(d2p1_IsClosed) = 'FALSE', 0, 1) AS IS_CLOSED,
  TO_INTEGER(
    REG_EXTRACT(
      d2p1_OpenTime,
      '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
      2,
      0
    )
  ) AS v_OpenHours,
  TO_INTEGER(
    REG_EXTRACT(
      d2p1_OpenTime,
      '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
      4,
      0
    )
  ) AS v_OpenMinutes,
  IFF(
    v_Is_Closed = 0,
    IFF(
      ISNULL(v_OpenMinutes),
      SET_DATE_PART(v_Day_Dt, 'HH', v_OpenHours),
      SET_DATE_PART(
        SET_DATE_PART(v_Day_Dt, 'HH', v_OpenHours),
        'MI',
        v_OpenMinutes
      )
    ),
    v_Day_Dt
  ) AS v_OpenTstmp,
  IFF(
    IFF(UPPER(d2p1_IsClosed) = 'FALSE', 0, 1) = 0,
    IFF(
      ISNULL(
        TO_INTEGER(
          REG_EXTRACT(
            d2p1_OpenTime,
            '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
            4,
            0
          )
        )
      ),
      SET_DATE_PART(
        TO_DATE(SUBSTR(d2p1_ForDate, 0, 10), 'YYYY-MM-DD'),
        'HH',
        TO_INTEGER(
          REG_EXTRACT(
            d2p1_OpenTime,
            '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
            2,
            0
          )
        )
      ),
      SET_DATE_PART(
        SET_DATE_PART(
          TO_DATE(SUBSTR(d2p1_ForDate, 0, 10), 'YYYY-MM-DD'),
          'HH',
          TO_INTEGER(
            REG_EXTRACT(
              d2p1_OpenTime,
              '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
              2,
              0
            )
          )
        ),
        'MI',
        TO_INTEGER(
          REG_EXTRACT(
            d2p1_OpenTime,
            '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
            4,
            0
          )
        )
      )
    ),
    TO_DATE(SUBSTR(d2p1_ForDate, 0, 10), 'YYYY-MM-DD')
  ) AS OPEN_TSTMP,
  TO_INTEGER(
    REG_EXTRACT(
      d2p1_CloseTime,
      '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
      2,
      0
    )
  ) AS v_CloseHours,
  TO_INTEGER(
    REG_EXTRACT(
      d2p1_CloseTime,
      '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
      4,
      0
    )
  ) AS v_CloseMinutes,
  IFF(
    v_Is_Closed = 0,
    IFF(
      ISNULL(v_CloseMinutes),
      SET_DATE_PART(v_Day_Dt, 'HH', v_CloseHours),
      SET_DATE_PART(
        SET_DATE_PART(v_Day_Dt, 'HH', v_CloseHours),
        'MI',
        v_CloseMinutes
      )
    ),
    v_Day_Dt
  ) AS v_CloseTstmp,
  IFF(
    IFF(UPPER(d2p1_IsClosed) = 'FALSE', 0, 1) = 0,
    IFF(
      ISNULL(
        TO_INTEGER(
          REG_EXTRACT(
            d2p1_CloseTime,
            '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
            4,
            0
          )
        )
      ),
      SET_DATE_PART(
        TO_DATE(SUBSTR(d2p1_ForDate, 0, 10), 'YYYY-MM-DD'),
        'HH',
        TO_INTEGER(
          REG_EXTRACT(
            d2p1_CloseTime,
            '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
            2,
            0
          )
        )
      ),
      SET_DATE_PART(
        SET_DATE_PART(
          TO_DATE(SUBSTR(d2p1_ForDate, 0, 10), 'YYYY-MM-DD'),
          'HH',
          TO_INTEGER(
            REG_EXTRACT(
              d2p1_CloseTime,
              '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
              2,
              0
            )
          )
        ),
        'MI',
        TO_INTEGER(
          REG_EXTRACT(
            d2p1_CloseTime,
            '([A-Z]{2})(\d+)([A-Z]{1})(\d+)?([A-Z]{1})?',
            4,
            0
          )
        )
      )
    ),
    TO_DATE(SUBSTR(d2p1_ForDate, 0, 10), 'YYYY-MM-DD')
  ) AS CLOSE_TSTMP,
  sysdate AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_StoreHours_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("Exp_Site_Hours_Day_Pre_9")

# COMMAND ----------
# DBTITLE 1, SITE_HOURS_DAY_PRE


spark.sql("""INSERT INTO
  SITE_HOURS_DAY_PRE
SELECT
  DAY_DT AS DAY_DT,
  DAY_DT AS DAY_DT,
  DAY_DT AS DAY_DT,
  LOCATION_NBR AS LOCATION_NBR,
  LOCATION_NBR AS LOCATION_NBR,
  LOCATION_NBR AS LOCATION_NBR,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  BUSINESS_AREA AS BUSINESS_AREA,
  BUSINESS_AREA AS BUSINESS_AREA,
  OPEN_TSTMP AS OPEN_TSTMP,
  OPEN_TSTMP AS OPEN_TSTMP,
  OPEN_TSTMP AS OPEN_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  IS_CLOSE_FLAG AS IS_CLOSED,
  IS_CLOSE_FLAG AS IS_CLOSED,
  IS_CLOSED AS IS_CLOSED,
  LOAD_TSTMP AS LOAD_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  Exp_Site_Hours_Day_Pre_9""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_Hours_Day_Pre_Store")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Site_Hours_Day_Pre_Store", mainWorkflowId, parentName)
