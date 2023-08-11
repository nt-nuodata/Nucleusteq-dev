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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_earn_wage_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_htl_earn_wage_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_ACCRUED_LABOR_WK_0


query_0 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  STORE_DEPT_NBR AS STORE_DEPT_NBR,
  EARN_ID AS EARN_ID,
  JOB_CODE AS JOB_CODE,
  FULLPT_FLAG AS FULLPT_FLAG,
  PAY_FREQ_CD AS PAY_FREQ_CD,
  HOURS_WORKED AS HOURS_WORKED,
  EARNINGS_AMT AS EARNINGS_AMT,
  EARNINGS_LOC_AMT AS EARNINGS_LOC_AMT,
  CURRENCY_NBR AS CURRENCY_NBR
FROM
  PS2_ACCRUED_LABOR_WK"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PS2_ACCRUED_LABOR_WK_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_ADJUSTED_LABOR_WK_1


query_1 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  EARN_ID AS EARN_ID,
  STORE_DEPT_NBR AS STORE_DEPT_NBR,
  JOB_CODE AS JOB_CODE,
  ACT_HOURS_WORKED AS ACT_HOURS_WORKED,
  ACT_EARNINGS_LOC_AMT AS ACT_EARNINGS_LOC_AMT,
  EARNED_HOURS AS EARNED_HOURS,
  EARNED_LOC_AMT AS EARNED_LOC_AMT,
  FORECAST_HRS AS FORECAST_HRS,
  FORECAST_LOC_AMT AS FORECAST_LOC_AMT,
  EXCHANGE_RATE AS EXCHANGE_RATE,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  PS2_ADJUSTED_LABOR_WK"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PS2_ADJUSTED_LABOR_WK_1")

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
# DBTITLE 1, SQ_Shortcut_to_PS2_ACCRUED_LABOR_WK_3


query_3 = f"""SELECT
  NVL(accrl.week_dt_custcol, adj.week_dt_custcol) AS WEEK_DT,
  NVL(accrl.location_id, adj.location_id) AS LOCATION_ID,
  NVL(accrl.store_nbr, adj.store_nbr) AS STORE_NBR,
  'HOTEL' AS BUSINESS_AREA,
  SUM(adj.adjusted_amt) AS ADJUSTED_HRS,
  SUM(adj.adjusted_hrs) AS ADJUSTED_AMT,
  SUM(accrl.actual_amt) AS ACTUAL_HRS,
  SUM(accrl.actual_hrs) AS ACTUAL_AMT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      a11.week_dt AS WEEK_DT_CUSTCOL,
      a12.location_id,
      a12.store_nbr,
      a11.store_dept_nbr,
      SUM(
        CASE
          WHEN SUBSTR(a11.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '2015',
            '3535',
            '6050'
          ) THEN a11.hours_worked
          ELSE 0
        END
      ) AS actual_hrs,
      SUM(
        CASE
          WHEN SUBSTR(a11.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '2011',
            '2015',
            '2025',
            '2030',
            '2031',
            '2032',
            '2033',
            '3031',
            '3039',
            '3525',
            '3531',
            '3535',
            '3539',
            '6050'
          ) THEN a11.earnings_loc_amt
          ELSE 0
        END
      ) AS actual_amt
    FROM
      Shortcut_to_PS2_ACCRUED_LABOR_WK_0 a11
      JOIN Shortcut_to_SITE_PROFILE_2 a12 ON (a11.location_id = a12.location_id)
    WHERE
      a11.store_dept_nbr IN ('12', '13')
    GROUP BY
      a11.week_dt,
      a12.location_id,
      a12.store_nbr,
      a11.store_dept_nbr
  ) ACCRl
  FULL OUTER JOIN (
    SELECT
      a11.week_dt week_dt_custcol,
      a12.location_id,
      a12.store_nbr store_nbr,
      a11.store_dept_nbr,
      SUM(a11.act_earnings_loc_amt) adjusted_amt,
      SUM(a11.act_hours_worked) adjusted_hrs
    FROM
      (
        SELECT
          *
        FROM
          Shortcut_to_PS2_ADJUSTED_LABOR_WK_1
        WHERE
          week_dt >= '02-JAN-2011'
      ) A11
      JOIN Shortcut_to_SITE_PROFILE_2 a12 ON (a11.location_id = a12.location_id)
    WHERE
      (
        a11.store_dept_nbr IN (12)
        AND SUBSTR(a11.earn_id, 4, 4) = '1300'
        AND a11.job_code = 0
      )
    GROUP BY
      a11.week_dt,
      a12.location_id,
      a12.store_nbr,
      a11.store_dept_nbr
  ) ADJ ON accrl.week_dt_custcol = adj.week_dt_custcol
  AND accrl.store_nbr = adj.store_nbr
  AND accrl.store_dept_nbr = adj.store_dept_nbr
WHERE
  NVL(adj.adjusted_amt, 0) <> 0
  OR NVL(accrl.actual_hrs, 0) <> 0
  OR NVL(accrl.actual_amt, 0) <> 0
  OR NVL(adj.adjusted_hrs, 0) <> 0
GROUP BY
  NVL(accrl.week_dt_custcol, adj.week_dt_custcol),
  NVL(accrl.location_id, adj.location_id),
  NVL(accrl.store_nbr, adj.store_nbr)
ORDER BY
  WEEK_DT,
  LOCATION_ID"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PS2_ACCRUED_LABOR_WK_3")

# COMMAND ----------
# DBTITLE 1, PS2_HTL_EARN_WAGE_PRE


spark.sql("""INSERT INTO
  PS2_HTL_EARN_WAGE_PRE
SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BUSINESS_AREA AS BUSINESS_TYPE,
  ADJUSTED_HRS AS ADJUSTED_AMT,
  ADJUSTED_AMT AS ADJUSTED_HRS,
  ACTUAL_HRS AS ACTUAL_AMT,
  ACTUAL_AMT AS ACTUAL_HRS
FROM
  SQ_Shortcut_to_PS2_ACCRUED_LABOR_WK_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_earn_wage_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_htl_earn_wage_pre", mainWorkflowId, parentName)
