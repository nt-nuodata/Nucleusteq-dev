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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_ca_c_earn_wage_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_ca_c_earn_wage_pre", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, Shortcut_to_PS_PAYROLL_CALENDAR1_1


query_1 = f"""SELECT
  CHECK_DT AS CHECK_DT,
  PS_TAX_COMPANY_CD AS PS_TAX_COMPANY_CD,
  PAY_PERIOD_PARAMETER AS PAY_PERIOD_PARAMETER,
  PERIOD_START_DT AS PERIOD_START_DT,
  PERIOD_END_DT AS PERIOD_END_DT
FROM
  PS_PAYROLL_CALENDAR"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PS_PAYROLL_CALENDAR1_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_EMPLOYEE_PROFILE_WK1_2


query_2 = f"""SELECT
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

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_EMPLOYEE_PROFILE_WK1_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_ACCRUED_LABOR_WK_3


query_3 = f"""SELECT
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

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_PS2_ACCRUED_LABOR_WK_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_ADJUSTED_LABOR_WK_4


query_4 = f"""SELECT
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

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_PS2_ADJUSTED_LABOR_WK_4")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DAYS_5


query_5 = f"""SELECT
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

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Shortcut_to_DAYS_5")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_6


query_6 = f"""SELECT
  NVL(pc.week_dt, hrs.week_dt + 7) AS WEEK_DT,
  hrs.location_id AS LOCATION_ID,
  SUM(hrs.earned_hrs) AS TTL_EARNED_HRS,
  SUM(hrs.wfa_sal_mgr_hrs) AS WFA_SAL_MGR_HRS,
  SUM(NVL(p1.sal_mgr_hrs, 0)) AS ACT_SAL_MGR_HRS,
  SUM(NVL(p1.sal_mgr_amt, 0)) AS ACT_SAL_MGR_AMT,
  SUM(NVL(p2.count_hrly_mgr, 0)) AS CNT_OF_HRLY_MGR,
  ROUND(
    SUM(NVL(p1.hrly_pay_amt, 0)) / CASE
      WHEN SUM(NVL(p1.hrly_pay_hrs, 0)) = 0 THEN 1
      ELSE SUM(NVL(p1.hrly_pay_hrs, 0))
    END,
    4
  ) AS AVG_ACT_WAGE_RATE,
  NVL(
    ROUND(
      SUM(NVL(p1.hrly_mgr_pay_amt, 0)) / (
        CASE
          WHEN SUM(NVL(p1.hrly_mgr_pay_hrs, 0)) = 0 THEN 1
          ELSE SUM(NVL(p1.hrly_mgr_pay_hrs, 0))
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
      Shortcut_to_DAYS_5 d,
      site_profile sp
    WHERE
      e.day_dt = d.day_dt
      AND e.location_id = sp.location_id
      AND d.week_dt > '01-jan-2011'
      AND d.week_dt < CURRENT_DATE
      AND sp.country_cd = 'CA'
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
          AND acr.job_code NOT IN (1604, 7000)
          AND SUBSTR(acr.earn_id, 4, 4) IN (
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
          )
          AND acr.store_dept_nbr IN ('2', '6') THEN DECODE(
            SUBSTR(acr.earn_id, 4, 4),
            '1513',
            (acr.hours_worked * 1.5),
            '1530',
            (acr.hours_worked * 1.5),
            '1535',
            (acr.hours_worked * 2.0),
            '1350',
            (acr.hours_worked * 1.5),
            acr.hours_worked
          )
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.week_dt IS NOT NULL
          AND adj.job_code NOT IN (1604, 7000)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
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
          )
          AND adj.store_dept_nbr IN ('2', '6') THEN DECODE(
            SUBSTR(acr.earn_id, 4, 4),
            '1513',
            (adj.act_hours_worked * 1.5),
            '1530',
            (adj.act_hours_worked * 1.5),
            '1535',
            (adj.act_hours_worked * 2.0),
            '1350',
            (adj.act_hours_worked * 1.5),
            adj.act_hours_worked
          )
          ELSE 0
        END
      ) AS hrly_pay_hrs,
      SUM(
        CASE
          WHEN acr.week_dt IS NOT NULL
          AND acr.job_code NOT IN (1604, 7000)
          AND SUBSTR(acr.earn_id, 4, 4) IN (
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
          )
          AND acr.store_dept_nbr IN ('2', '6') THEN acr.earnings_loc_amt
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.week_dt IS NOT NULL
          AND adj.job_code NOT IN (1604, 7000)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
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
          )
          AND adj.store_dept_nbr IN ('2', '6') THEN adj.act_earnings_loc_amt
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
          )
          AND acr.store_dept_nbr IN ('2', '6') THEN DECODE(
            SUBSTR(acr.earn_id, 4, 4),
            '1513',
            (acr.hours_worked * 1.5),
            '1530',
            (acr.hours_worked * 1.5),
            '1535',
            (acr.hours_worked * 2.0),
            '1350',
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
          )
          AND adj.store_dept_nbr IN ('2', '6') THEN DECODE(
            SUBSTR(acr.earn_id, 4, 4),
            '1513',
            (adj.act_hours_worked * 1.5),
            '1530',
            (adj.act_hours_worked * 1.5),
            '1535',
            (adj.act_hours_worked * 2.0),
            '1350',
            (adj.act_hours_worked * 1.5),
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
          )
          AND acr.store_dept_nbr IN ('2', '6') THEN acr.earnings_loc_amt
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
          )
          AND adj.store_dept_nbr IN ('2', '6') THEN adj.act_earnings_loc_amt
          ELSE 0
        END
      ) AS hrly_mgr_pay_amt,
      SUM(
        CASE
          WHEN acr.job_code IN (1604, 7000)
          AND SUBSTR(acr.earn_id, 4, 4) IN (
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
          ) THEN acr.hours_worked
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.job_code IN (1604, 7000)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
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
          ) THEN adj.act_hours_worked
          ELSE 0
        END
      ) AS sal_mgr_hrs,
      SUM(
        CASE
          WHEN acr.job_code IN (1604, 7000)
          AND SUBSTR(acr.earn_id, 4, 4) IN (
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
          ) THEN acr.earnings_loc_amt
          ELSE 0
        END
      ) + SUM(
        CASE
          WHEN adj.job_code IN (1604, 7000)
          AND SUBSTR(adj.earn_id, 4, 4) IN (
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
          Shortcut_to_PS2_ACCRUED_LABOR_WK_3 p
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
          WHEN job_code IN (7002, 7006, 7008) THEN employee_id
          ELSE NULL
        END
      ) AS count_hrly_mgr
    FROM
      Shortcut_to_EMPLOYEE_PROFILE_WK1_2
    WHERE
      job_code IN (7002, 7006, 7008)
      AND empl_status_cd = 'A'
    GROUP BY
      week_dt,
      location_id
  ) p2 ON (
    hrs.week_dt = p2.week_dt
    AND hrs.location_id = p2.location_id
  )
  LEFT OUTER JOIN (
    SELECT
      DISTINCT (check_dt - date_part('DOW', check_dt) + 1) AS week_dt
    FROM
      ps_payroll_calendar
    WHERE
      ps_tax_company_cd = 'NCJ'
      AND (check_dt - date_part('DOW', check_dt) + 1) BETWEEN '01-jan-2011' AND CURRENT_DATE
  ) pc ON (hrs.week_dt = pc.week_dt)
GROUP BY
  NVL(pc.week_dt, hrs.week_dt + 7),
  hrs.location_id
ORDER BY
  NVL(pc.week_dt, hrs.week_dt + 7),
  hrs.location_id"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_6")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_PROFILE_7


query_7 = f"""SELECT
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

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Shortcut_to_SITE_PROFILE_7")

# COMMAND ----------
# DBTITLE 1, PS2_CA_C_EARN_WAGE_PRE


spark.sql("""INSERT INTO
  PS2_CA_C_EARN_WAGE_PRE
SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  TTL_EARNED_HRS AS TTL_EARNED_HRS,
  WFA_SAL_MGR_HRS AS WFA_SAL_MGR_HRS,
  ACT_SAL_MGR_HRS AS ACT_SAL_MGR_HRS,
  ACT_SAL_MGR_AMT AS ACT_SAL_MGR_AMT,
  CNT_OF_HRLY_MGR AS CNT_OF_HRLY_MGRS,
  AVG_ACT_WAGE_RATE AS AVG_ACT_WAGE_RATE,
  STR_HRLY_MGR_AVG_WG_RT AS STR_HRLY_MGR_AVG_WG_RT
FROM
  SQ_Shortcut_to_PS2_ADJUSTED_LABOR_WK_6""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_ca_c_earn_wage_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_ca_c_earn_wage_pre", mainWorkflowId, parentName)
