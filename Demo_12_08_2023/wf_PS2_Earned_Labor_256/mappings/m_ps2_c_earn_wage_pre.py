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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_c_earn_wage_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_c_earn_wage_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_EARNED_HRS_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  WFA_BUSN_AREA_ID AS WFA_BUSN_AREA_ID,
  WFA_DEPT_ID AS WFA_DEPT_ID,
  WFA_TASK_ID AS WFA_TASK_ID,
  STORE_NBR AS STORE_NBR,
  WFA_BUSN_AREA_DESC AS WFA_BUSN_AREA_DESC,
  WFA_DEPT_DESC AS WFA_DEPT_DESC,
  WFA_TASK_DESC AS WFA_TASK_DESC,
  EARNED_HRS AS EARNED_HRS,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  PS2_EARNED_HRS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PS2_EARNED_HRS_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_EMPLOYEE_PROFILE_WK1_1


query_1 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  EMPLOYEE_ID AS EMPLOYEE_ID,
  EMPL_FIRST_NAME AS EMPL_FIRST_NAME,
  EMPL_MIDDLE_NAME AS EMPL_MIDDLE_NAME,
  EMPL_LAST_NAME AS EMPL_LAST_NAME,
  EMPL_BIRTH_DT AS EMPL_BIRTH_DT,
  GENDER_CD AS GENDER_CD,
  PS_MARITAL_STATUS_CD AS PS_MARITAL_STATUS_CD,
  ETHNIC_GROUP_ID AS ETHNIC_GROUP_ID,
  EMPL_ADDR_1 AS EMPL_ADDR_1,
  EMPL_ADDR_2 AS EMPL_ADDR_2,
  EMPL_CITY AS EMPL_CITY,
  EMPL_STATE AS EMPL_STATE,
  EMPL_PROVINCE AS EMPL_PROVINCE,
  EMPL_ZIPCODE AS EMPL_ZIPCODE,
  COUNTRY_CD AS COUNTRY_CD,
  EMPL_HOME_PHONE AS EMPL_HOME_PHONE,
  EMPL_EMAIL_ADDR AS EMPL_EMAIL_ADDR,
  EMPL_LOGIN_ID AS EMPL_LOGIN_ID,
  BADGE_NBR AS BADGE_NBR,
  EMPL_STATUS_CD AS EMPL_STATUS_CD,
  STATUS_CHG_DT AS STATUS_CHG_DT,
  FULLPT_FLAG AS FULLPT_FLAG,
  FULLPT_CHG_DT AS FULLPT_CHG_DT,
  EMPL_TYPE_CD AS EMPL_TYPE_CD,
  PS_REG_TEMP_CD AS PS_REG_TEMP_CD,
  EMPL_CATEGORY_CD AS EMPL_CATEGORY_CD,
  EMPL_GROUP_CD AS EMPL_GROUP_CD,
  EMPL_SUBGROUP_CD AS EMPL_SUBGROUP_CD,
  EMPL_HIRE_DT AS EMPL_HIRE_DT,
  EMPL_REHIRE_DT AS EMPL_REHIRE_DT,
  EMPL_TERM_DT AS EMPL_TERM_DT,
  TERM_REASON_CD AS TERM_REASON_CD,
  EMPL_SENORITY_DT AS EMPL_SENORITY_DT,
  PS_ACTION_DT AS PS_ACTION_DT,
  PS_ACTION_CD AS PS_ACTION_CD,
  PS_ACTION_REASON_CD AS PS_ACTION_REASON_CD,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_CHG_DT AS LOCATION_CHG_DT,
  STORE_NBR AS STORE_NBR,
  STORE_DEPT_NBR AS STORE_DEPT_NBR,
  COMPANY_ID AS COMPANY_ID,
  PS_PERSONNEL_AREA_ID AS PS_PERSONNEL_AREA_ID,
  PS_PERSONNEL_SUBAREA_ID AS PS_PERSONNEL_SUBAREA_ID,
  PS_DEPT_CD AS PS_DEPT_CD,
  PS_DEPT_CHG_DT AS PS_DEPT_CHG_DT,
  PS_POSITION_ID AS PS_POSITION_ID,
  POSITION_CHG_DT AS POSITION_CHG_DT,
  PS_SUPERVISOR_ID AS PS_SUPERVISOR_ID,
  JOB_CODE AS JOB_CODE,
  JOB_CODE_CHG_DT AS JOB_CODE_CHG_DT,
  EMPL_JOB_ENTRY_DT AS EMPL_JOB_ENTRY_DT,
  PS_GRADE_ID AS PS_GRADE_ID,
  EMPL_STD_BONUS_PCT AS EMPL_STD_BONUS_PCT,
  EMPL_OVR_BONUS_PCT AS EMPL_OVR_BONUS_PCT,
  EMPL_RATING AS EMPL_RATING,
  PAY_RATE_CHG_DT AS PAY_RATE_CHG_DT,
  PS_PAYROLL_AREA_CD AS PS_PAYROLL_AREA_CD,
  PS_TAX_COMPANY_CD AS PS_TAX_COMPANY_CD,
  PS_COMP_FREQ_CD AS PS_COMP_FREQ_CD,
  COMP_RATE_AMT AS COMP_RATE_AMT,
  ANNUAL_RATE_LOC_AMT AS ANNUAL_RATE_LOC_AMT,
  HOURLY_RATE_LOC_AMT AS HOURLY_RATE_LOC_AMT,
  CURRENCY_ID AS CURRENCY_ID,
  EXCH_RATE_PCT AS EXCH_RATE_PCT,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EMPLOYEE_PROFILE_WK"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_EMPLOYEE_PROFILE_WK1_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_ACCRUED_LABOR_WK_2


query_2 = f"""SELECT
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

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_PS2_ACCRUED_LABOR_WK_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_ADJUSTED_LABOR_WK_3


query_3 = f"""SELECT
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

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_PS2_ADJUSTED_LABOR_WK_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DAYS_4


query_4 = f"""SELECT
  DAY_DT AS DAY_DT,
  BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
  HOLIDAY_FLAG AS HOLIDAY_FLAG,
  DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
  DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
  CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
  CAL_WK AS CAL_WK,
  CAL_WK_NBR AS CAL_WK_NBR,
  CAL_MO AS CAL_MO,
  CAL_MO_NBR AS CAL_MO_NBR,
  CAL_MO_NAME AS CAL_MO_NAME,
  CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
  CAL_QTR AS CAL_QTR,
  CAL_QTR_NBR AS CAL_QTR_NBR,
  CAL_HALF AS CAL_HALF,
  CAL_YR AS CAL_YR,
  FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
  FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_WK_NBR AS FISCAL_WK_NBR,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_MO_NBR AS FISCAL_MO_NBR,
  FISCAL_MO_NAME AS FISCAL_MO_NAME,
  FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
  FISCAL_HALF AS FISCAL_HALF,
  FISCAL_YR AS FISCAL_YR,
  LYR_WEEK_DT AS LYR_WEEK_DT,
  LWK_WEEK_DT AS LWK_WEEK_DT,
  WEEK_DT AS WEEK_DT,
  EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
  EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
  ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
  ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
  CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
  CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
  CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
  CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
  MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
  MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
  MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
  MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
  PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
  PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS
FROM
  DAYS"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_DAYS_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_5


query_5 = f"""SELECT
  hrs.week_dt AS WEEK_DT,
  hrs.location_id AS LOCATION_ID,
  hrs.earned_hrs AS TTL_EARNED_HRS,
  hrs.wfa_sal_mgr_hrs AS WFA_SAL_MGR_HRS,
  NVL(p1.sal_mgr_hrs, 0) AS ACT_SAL_MGR_HRS,
  NVL(p1.sal_mgr_amt, 0) AS ACT_SAL_MGR_AMT,
  NVL(p2.count_hrly_mgr, 0) AS CNT_OF_HRLY_MGR,
  NVL(
    ROUND(
      p1.hrly_pay_amt / (
        CASE
          WHEN NVL(p1.hrly_pay_hrs, 0) = 0 THEN 1
          ELSE p1.hrly_pay_hrs
        END
      ),
      4
    ),
    0
  ) AS AVG_ACT_WAGE_RATE,
  0 AS HDAY_OT_HRS,
  NVL(
    ROUND(
      p1.hrly_mgr_pay_amt / (
        CASE
          WHEN NVL(p1.hrly_mgr_pay_hrs, 0) = 0 THEN 1
          ELSE p1.hrly_mgr_pay_hrs
        END
      ),
      4
    ),
    0
  ) AS STR_HRLY_MGR_AVG_WG_RT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      d.week_dt,
      e.location_id,
      ROUND(SUM(e.earned_hrs), 2) earned_hrs,
      ROUND(
        SUM(
          CASE
            WHEN e.wfa_task_desc = 'Salaried Manager' THEN e.earned_hrs
            ELSE 0
          END
        ),
        2
      ) AS wfa_sal_mgr_hrs
    FROM
      Shortcut_to_PS2_EARNED_HRS_0 e,
      Shortcut_to_DAYS_4 d,
      site_profile sp
    WHERE
      e.day_dt = d.day_dt
      AND e.location_id = sp.location_id
      AND e.day_dt > '26-dec-2010'
      AND e.wfa_busn_area_id IN (1, 3)
    GROUP BY
      d.week_dt,
      e.location_id
  ) hrs
  LEFT OUTER JOIN (
    SELECT
      NVL(acr.week_dt, adj.week_dt) week_dt,
      NVL(acr.location_id, adj.location_id) location_id,
      SUM(
        CASE
          WHEN acr.week_dt IS NOT NULL
          AND acr.job_code NOT IN (1604, 1641, 7000, 2610, 3001)
          AND acr.pay_freq_cd <> 'B'
          AND SUBSTR(acr.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
            '2015',
            '3535',
            '6050',
            '2575',
            '2576',
            '1385'
          )
          AND acr.store_dept_nbr IN ('0', '2', '6') THEN DECODE(
            SUBSTR(acr.earn_id, 4, 4),
            '1513',
            (acr.hours_worked * 1.5),
            '1550',
            (acr.hours_worked * 1.5),
            acr.hours_worked
          )
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.week_dt IS NOT NULL
          AND adj.job_code NOT IN (1604, 1641, 7000, 2610, 3001)
          AND SUBSTR(acr.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
            '2015',
            '3535',
            '6050',
            '2575',
            '2576',
            '1385'
          )
          AND adj.store_dept_nbr IN ('0', '2', '6') THEN DECODE(
            SUBSTR(acr.earn_id, 4, 4),
            '1513',
            (adj.act_hours_worked * 1.5),
            '1550',
            (acr.hours_worked * 1.5),
            adj.act_hours_worked
          )
          ELSE 0
        END
      ) AS hrly_pay_hrs,
      SUM(
        CASE
          WHEN acr.week_dt IS NOT NULL
          AND acr.job_code NOT IN (1604, 1641, 7000, 2610, 3001)
          AND acr.pay_freq_cd <> 'B'
          AND SUBSTR(acr.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1550',
            '1551',
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
            '6050',
            '2575',
            '2576',
            '1385'
          )
          AND acr.store_dept_nbr IN ('0', '2', '6') THEN acr.earnings_loc_amt
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.week_dt IS NOT NULL
          AND adj.job_code NOT IN (1604, 1641, 7000, 2610, 3001)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1350',
            '1341',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1550',
            '1551',
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
            '6050',
            '2575',
            '2576',
            '1385'
          )
          AND adj.store_dept_nbr IN ('0', '2', '6') THEN adj.act_earnings_loc_amt
          ELSE 0
        END
      ) AS hrly_pay_amt,
      SUM(
        CASE
          WHEN acr.week_dt IS NOT NULL
          AND acr.job_code IN (7002, 7006, 7008)
          AND acr.pay_freq_cd <> 'B'
          AND SUBSTR(acr.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
            '2015',
            '3535',
            '6050',
            '2575',
            '2576'
          )
          AND acr.store_dept_nbr IN ('0', '2', '6') THEN DECODE(
            SUBSTR(acr.earn_id, 4, 4),
            '1513',
            (acr.hours_worked * 1.5),
            '1550',
            (acr.hours_worked * 1.5),
            acr.hours_worked
          )
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.week_dt IS NOT NULL
          AND adj.job_code IN (7002, 7006, 7008)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
            '2015',
            '3535',
            '6050',
            '2575',
            '2576'
          )
          AND adj.store_dept_nbr IN ('0', '2', '6') THEN DECODE(
            SUBSTR(acr.earn_id, 4, 4),
            '1513',
            (adj.act_hours_worked * 1.5),
            '1550',
            (acr.hours_worked * 1.5),
            adj.act_hours_worked
          )
          ELSE 0
        END
      ) AS hrly_mgr_pay_hrs,
      SUM(
        CASE
          WHEN acr.week_dt IS NOT NULL
          AND acr.job_code IN (7002, 7006, 7008)
          AND acr.pay_freq_cd <> 'B'
          AND SUBSTR(acr.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
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
            '6050',
            '2575',
            '2576'
          )
          AND acr.store_dept_nbr IN ('0', '2', '6') THEN acr.earnings_loc_amt
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.week_dt IS NOT NULL
          AND adj.job_code IN (7002, 7006, 7008)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
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
            '6050',
            '2575',
            '2576'
          )
          AND adj.store_dept_nbr IN ('0', '2', '6') THEN adj.act_earnings_loc_amt
          ELSE 0
        END
      ) AS hrly_mgr_pay_amt,
      SUM(
        CASE
          WHEN acr.job_code IN (1604, 1641, 7000, 2610, 3001)
          AND SUBSTR(acr.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
            '2015',
            '3535',
            '6050',
            '2575',
            '2576'
          ) THEN acr.hours_worked
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.job_code IN (1604, 1641, 7000, 2610, 3001)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
            '2015',
            '3535',
            '6050',
            '2575',
            '2576'
          ) THEN adj.act_hours_worked
          ELSE 0
        END
      ) AS sal_mgr_hrs,
      SUM(
        CASE
          WHEN acr.job_code IN (1604, 1641, 7000, 2610, 3001)
          AND SUBSTR(acr.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
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
            '6050',
            '2575',
            '2576'
          )
          AND acr.pay_freq_cd = 'B' THEN acr.earnings_loc_amt
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.job_code IN (1604, 1641, 7000, 2610, 3001)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
            '1100',
            '1300',
            '1310',
            '1341',
            '1350',
            '1380',
            '1513',
            '1514',
            '1517',
            '1518',
            '1519',
            '1530',
            '1535',
            '1550',
            '1551',
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
          ) THEN adj.act_earnings_loc_amt
          ELSE 0
        END
      ) AS sal_mgr_amt
    FROM
      (
        SELECT
          p.week_dt,
          p.location_id,
          p.store_dept_nbr,
          p.earn_id,
          p.job_code,
          p.pay_freq_cd,
          SUM(p.hours_worked) AS hours_worked,
          SUM(p.earnings_loc_amt) AS earnings_loc_amt
        FROM
          Shortcut_to_PS2_ACCRUED_LABOR_WK_2 p
        GROUP BY
          p.week_dt,
          p.location_id,
          p.store_dept_nbr,
          p.earn_id,
          p.job_code,
          p.pay_freq_cd
      ) acr
      FULL OUTER JOIN (
        SELECT
          *
        FROM
          ps2_adjusted_labor_wk
        WHERE
          week_dt >= '02-JAN-2011'
      ) adj ON acr.week_dt = adj.week_dt
      AND acr.location_id = adj.location_id
      AND acr.store_dept_nbr = adj.store_dept_nbr
      AND acr.earn_id = adj.earn_id
      AND acr.job_code = adj.job_code
    GROUP BY
      NVL(acr.week_dt, adj.week_dt),
      NVL(acr.location_id, adj.location_id)
    HAVING
      (
        hrly_pay_hrs <> 0
        OR hrly_pay_amt <> 0
        OR sal_mgr_hrs <> 0
        OR sal_mgr_amt <> 0
      )
  ) p1 ON hrs.week_dt = p1.week_dt
  AND hrs.location_id = p1.location_id
  LEFT OUTER JOIN (
    SELECT
      week_dt,
      location_id,
      COUNT(
        DISTINCT CASE
          WHEN job_code IN (7002, 7006, 7008)
          AND store_dept_nbr IN ('2', '6') THEN employee_id
          ELSE NULL
        END
      ) AS count_hrly_mgr
    FROM
      Shortcut_to_EMPLOYEE_PROFILE_WK1_1
    WHERE
      empl_status_cd = 'A'
      AND week_dt > '01-jan-2011'
    GROUP BY
      week_dt,
      location_id
  ) p2 ON (
    hrs.week_dt = p2.week_dt
    AND hrs.location_id = p2.location_id
  )
ORDER BY
  hrs.week_dt,
  hrs.location_id"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_5")

# COMMAND ----------
# DBTITLE 1, PS2_C_EARN_WAGE_PRE


spark.sql("""INSERT INTO
  PS2_C_EARN_WAGE_PRE
SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  TTL_EARNED_HRS AS TTL_EARNED_HRS,
  WFA_SAL_MGR_HRS AS WFA_SAL_MGR_HRS,
  ACT_SAL_MGR_HRS AS ACT_SAL_MGR_HRS,
  ACT_SAL_MGR_AMT AS ACT_SAL_MGR_AMT,
  CNT_OF_HRLY_MGR AS CNT_OF_HRLY_MGRS,
  AVG_ACT_WAGE_RATE AS AVG_ACT_WAGE_RATE,
  HDAY_OT_HRS AS HOLIDAY_OT_HRS,
  STR_HRLY_MGR_AVG_WG_RT AS STR_HRLY_MGR_AVG_WG_RT
FROM
  SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_5""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_c_earn_wage_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_c_earn_wage_pre", mainWorkflowId, parentName)
