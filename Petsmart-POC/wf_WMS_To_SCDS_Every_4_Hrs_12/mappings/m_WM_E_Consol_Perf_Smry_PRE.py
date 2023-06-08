# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")


# COMMAND ----------
# DBTITLE 1, Shortcut_to_E_CONSOL_PERF_SMRY_0


df_0 = spark.sql("""SELECT
  PERF_SMRY_TRAN_ID AS PERF_SMRY_TRAN_ID,
  WHSE AS WHSE,
  LOGIN_USER_ID AS LOGIN_USER_ID,
  JOB_FUNCTION_NAME AS JOB_FUNCTION_NAME,
  SPVSR_LOGIN_USER_ID AS SPVSR_LOGIN_USER_ID,
  DEPT_CODE AS DEPT_CODE,
  CLOCK_IN_DATE AS CLOCK_IN_DATE,
  CLOCK_IN_STATUS AS CLOCK_IN_STATUS,
  TOTAL_SAM AS TOTAL_SAM,
  TOTAL_PAM AS TOTAL_PAM,
  TOTAL_TIME AS TOTAL_TIME,
  OSDL AS OSDL,
  OSIL AS OSIL,
  NSDL AS NSDL,
  SIL AS SIL,
  UDIL AS UDIL,
  UIL AS UIL,
  ADJ_OSDL AS ADJ_OSDL,
  ADJ_OSIL AS ADJ_OSIL,
  ADJ_UDIL AS ADJ_UDIL,
  ADJ_NSDL AS ADJ_NSDL,
  PAID_BRK AS PAID_BRK,
  UNPAID_BRK AS UNPAID_BRK,
  REF_OSDL AS REF_OSDL,
  REF_OSIL AS REF_OSIL,
  REF_UDIL AS REF_UDIL,
  REF_NSDL AS REF_NSDL,
  REF_ADJ_OSDL AS REF_ADJ_OSDL,
  REF_ADJ_OSIL AS REF_ADJ_OSIL,
  REF_ADJ_UDIL AS REF_ADJ_UDIL,
  REF_ADJ_NSDL AS REF_ADJ_NSDL,
  MISC_NUMBER_1 AS MISC_NUMBER_1,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  MISC_1 AS MISC_1,
  MISC_2 AS MISC_2,
  CLOCK_OUT_DATE AS CLOCK_OUT_DATE,
  SHIFT_CODE AS SHIFT_CODE,
  EVENT_COUNT AS EVENT_COUNT,
  START_DATE_TIME AS START_DATE_TIME,
  END_DATE_TIME AS END_DATE_TIME,
  LEVEL_1 AS LEVEL_1,
  LEVEL_2 AS LEVEL_2,
  LEVEL_3 AS LEVEL_3,
  LEVEL_4 AS LEVEL_4,
  LEVEL_5 AS LEVEL_5,
  WHSE_DATE AS WHSE_DATE,
  OPS_CODE AS OPS_CODE,
  REF_SAM AS REF_SAM,
  REF_PAM AS REF_PAM,
  REPORT_SHIFT AS REPORT_SHIFT,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  EVNT_CTGRY_1 AS EVNT_CTGRY_1,
  EVNT_CTGRY_2 AS EVNT_CTGRY_2,
  EVNT_CTGRY_3 AS EVNT_CTGRY_3,
  EVNT_CTGRY_4 AS EVNT_CTGRY_4,
  EVNT_CTGRY_5 AS EVNT_CTGRY_5,
  LABOR_COST_RATE AS LABOR_COST_RATE,
  PAID_OVERLAP_OSDL AS PAID_OVERLAP_OSDL,
  UNPAID_OVERLAP_OSDL AS UNPAID_OVERLAP_OSDL,
  PAID_OVERLAP_NSDL AS PAID_OVERLAP_NSDL,
  UNPAID_OVERLAP_NSDL AS UNPAID_OVERLAP_NSDL,
  PAID_OVERLAP_OSIL AS PAID_OVERLAP_OSIL,
  UNPAID_OVERLAP_OSIL AS UNPAID_OVERLAP_OSIL,
  PAID_OVERLAP_UDIL AS PAID_OVERLAP_UDIL,
  UNPAID_OVERLAP_UDIL AS UNPAID_OVERLAP_UDIL,
  VERSION_ID AS VERSION_ID,
  TEAM_CODE AS TEAM_CODE,
  DEFAULT_JF_FLAG AS DEFAULT_JF_FLAG,
  EMP_PERF_SMRY_ID AS EMP_PERF_SMRY_ID,
  TOTAL_QTY AS TOTAL_QTY,
  REF_NBR AS REF_NBR,
  TEAM_BEGIN_TIME AS TEAM_BEGIN_TIME,
  THRUPUT_MIN AS THRUPUT_MIN,
  DISPLAY_UOM_QTY AS DISPLAY_UOM_QTY,
  DISPLAY_UOM AS DISPLAY_UOM,
  LOCN_GRP_ATTR AS LOCN_GRP_ATTR,
  RESOURCE_GROUP_ID AS RESOURCE_GROUP_ID,
  COMP_ASSIGNMENT_ID AS COMP_ASSIGNMENT_ID,
  REFLECTIVE_CODE AS REFLECTIVE_CODE
FROM
  E_CONSOL_PERF_SMRY""")

df_0.createOrReplaceTempView("Shortcut_to_E_CONSOL_PERF_SMRY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_E_CONSOL_PERF_SMRY_1


df_1 = spark.sql("""SELECT
  PERF_SMRY_TRAN_ID AS PERF_SMRY_TRAN_ID,
  WHSE AS WHSE,
  LOGIN_USER_ID AS LOGIN_USER_ID,
  JOB_FUNCTION_NAME AS JOB_FUNCTION_NAME,
  SPVSR_LOGIN_USER_ID AS SPVSR_LOGIN_USER_ID,
  DEPT_CODE AS DEPT_CODE,
  CLOCK_IN_DATE AS CLOCK_IN_DATE,
  CLOCK_IN_STATUS AS CLOCK_IN_STATUS,
  TOTAL_SAM AS TOTAL_SAM,
  TOTAL_PAM AS TOTAL_PAM,
  TOTAL_TIME AS TOTAL_TIME,
  OSDL AS OSDL,
  OSIL AS OSIL,
  NSDL AS NSDL,
  SIL AS SIL,
  UDIL AS UDIL,
  UIL AS UIL,
  ADJ_OSDL AS ADJ_OSDL,
  ADJ_OSIL AS ADJ_OSIL,
  ADJ_UDIL AS ADJ_UDIL,
  ADJ_NSDL AS ADJ_NSDL,
  PAID_BRK AS PAID_BRK,
  UNPAID_BRK AS UNPAID_BRK,
  REF_OSDL AS REF_OSDL,
  REF_OSIL AS REF_OSIL,
  REF_UDIL AS REF_UDIL,
  REF_NSDL AS REF_NSDL,
  REF_ADJ_OSDL AS REF_ADJ_OSDL,
  REF_ADJ_OSIL AS REF_ADJ_OSIL,
  REF_ADJ_UDIL AS REF_ADJ_UDIL,
  REF_ADJ_NSDL AS REF_ADJ_NSDL,
  MISC_NUMBER_1 AS MISC_NUMBER_1,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  MISC_1 AS MISC_1,
  MISC_2 AS MISC_2,
  CLOCK_OUT_DATE AS CLOCK_OUT_DATE,
  SHIFT_CODE AS SHIFT_CODE,
  EVENT_COUNT AS EVENT_COUNT,
  START_DATE_TIME AS START_DATE_TIME,
  END_DATE_TIME AS END_DATE_TIME,
  LEVEL_1 AS LEVEL_1,
  LEVEL_2 AS LEVEL_2,
  LEVEL_3 AS LEVEL_3,
  LEVEL_4 AS LEVEL_4,
  LEVEL_5 AS LEVEL_5,
  WHSE_DATE AS WHSE_DATE,
  OPS_CODE AS OPS_CODE,
  REF_SAM AS REF_SAM,
  REF_PAM AS REF_PAM,
  REPORT_SHIFT AS REPORT_SHIFT,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  EVNT_CTGRY_1 AS EVNT_CTGRY_1,
  EVNT_CTGRY_2 AS EVNT_CTGRY_2,
  EVNT_CTGRY_3 AS EVNT_CTGRY_3,
  EVNT_CTGRY_4 AS EVNT_CTGRY_4,
  EVNT_CTGRY_5 AS EVNT_CTGRY_5,
  LABOR_COST_RATE AS LABOR_COST_RATE,
  PAID_OVERLAP_OSDL AS PAID_OVERLAP_OSDL,
  UNPAID_OVERLAP_OSDL AS UNPAID_OVERLAP_OSDL,
  PAID_OVERLAP_NSDL AS PAID_OVERLAP_NSDL,
  UNPAID_OVERLAP_NSDL AS UNPAID_OVERLAP_NSDL,
  PAID_OVERLAP_OSIL AS PAID_OVERLAP_OSIL,
  UNPAID_OVERLAP_OSIL AS UNPAID_OVERLAP_OSIL,
  PAID_OVERLAP_UDIL AS PAID_OVERLAP_UDIL,
  UNPAID_OVERLAP_UDIL AS UNPAID_OVERLAP_UDIL,
  VERSION_ID AS VERSION_ID,
  TEAM_CODE AS TEAM_CODE,
  DEFAULT_JF_FLAG AS DEFAULT_JF_FLAG,
  EMP_PERF_SMRY_ID AS EMP_PERF_SMRY_ID,
  TOTAL_QTY AS TOTAL_QTY,
  REF_NBR AS REF_NBR,
  TEAM_BEGIN_TIME AS TEAM_BEGIN_TIME,
  THRUPUT_MIN AS THRUPUT_MIN,
  DISPLAY_UOM_QTY AS DISPLAY_UOM_QTY,
  DISPLAY_UOM AS DISPLAY_UOM,
  LOCN_GRP_ATTR AS LOCN_GRP_ATTR,
  RESOURCE_GROUP_ID AS RESOURCE_GROUP_ID,
  COMP_ASSIGNMENT_ID AS COMP_ASSIGNMENT_ID,
  REFLECTIVE_CODE AS REFLECTIVE_CODE,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_E_CONSOL_PERF_SMRY_0
WHERE
  $$Initial_Load (
    trunc(
      Shortcut_to_E_CONSOL_PERF_SMRY_0.CREATE_DATE_TIME
    ) >= trunc(to_date('$$Prev_Run_Dt', 'MM/DD/YYYY HH24:MI:SS')) - 1
  )
  OR (
    trunc(Shortcut_to_E_CONSOL_PERF_SMRY_0.MOD_DATE_TIME) >= trunc(to_date('$$Prev_Run_Dt', 'MM/DD/YYYY HH24:MI:SS')) - 1
  )
  AND 1 = 1""")

df_1.createOrReplaceTempView("SQ_Shortcut_to_E_CONSOL_PERF_SMRY_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


df_2 = spark.sql("""SELECT
  $$DC_NBR AS DC_NBR_EXP,
  PERF_SMRY_TRAN_ID AS PERF_SMRY_TRAN_ID,
  WHSE AS WHSE,
  LOGIN_USER_ID AS LOGIN_USER_ID,
  JOB_FUNCTION_NAME AS JOB_FUNCTION_NAME,
  SPVSR_LOGIN_USER_ID AS SPVSR_LOGIN_USER_ID,
  DEPT_CODE AS DEPT_CODE,
  CLOCK_IN_DATE AS CLOCK_IN_DATE,
  CLOCK_IN_STATUS AS CLOCK_IN_STATUS,
  TOTAL_SAM AS TOTAL_SAM,
  TOTAL_PAM AS TOTAL_PAM,
  TOTAL_TIME AS TOTAL_TIME,
  OSDL AS OSDL,
  OSIL AS OSIL,
  NSDL AS NSDL,
  SIL AS SIL,
  UDIL AS UDIL,
  UIL AS UIL,
  ADJ_OSDL AS ADJ_OSDL,
  ADJ_OSIL AS ADJ_OSIL,
  ADJ_UDIL AS ADJ_UDIL,
  ADJ_NSDL AS ADJ_NSDL,
  PAID_BRK AS PAID_BRK,
  UNPAID_BRK AS UNPAID_BRK,
  REF_OSDL AS REF_OSDL,
  REF_OSIL AS REF_OSIL,
  REF_UDIL AS REF_UDIL,
  REF_NSDL AS REF_NSDL,
  REF_ADJ_OSDL AS REF_ADJ_OSDL,
  REF_ADJ_OSIL AS REF_ADJ_OSIL,
  REF_ADJ_UDIL AS REF_ADJ_UDIL,
  REF_ADJ_NSDL AS REF_ADJ_NSDL,
  MISC_NUMBER_1 AS MISC_NUMBER_1,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  MISC_1 AS MISC_1,
  MISC_2 AS MISC_2,
  CLOCK_OUT_DATE AS CLOCK_OUT_DATE,
  SHIFT_CODE AS SHIFT_CODE,
  EVENT_COUNT AS EVENT_COUNT,
  START_DATE_TIME AS START_DATE_TIME,
  END_DATE_TIME AS END_DATE_TIME,
  LEVEL_1 AS LEVEL_1,
  LEVEL_2 AS LEVEL_2,
  LEVEL_3 AS LEVEL_3,
  LEVEL_4 AS LEVEL_4,
  LEVEL_5 AS LEVEL_5,
  WHSE_DATE AS WHSE_DATE,
  OPS_CODE AS OPS_CODE,
  REF_SAM AS REF_SAM,
  REF_PAM AS REF_PAM,
  REPORT_SHIFT AS REPORT_SHIFT,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  EVNT_CTGRY_1 AS EVNT_CTGRY_1,
  EVNT_CTGRY_2 AS EVNT_CTGRY_2,
  EVNT_CTGRY_3 AS EVNT_CTGRY_3,
  EVNT_CTGRY_4 AS EVNT_CTGRY_4,
  EVNT_CTGRY_5 AS EVNT_CTGRY_5,
  LABOR_COST_RATE AS LABOR_COST_RATE,
  PAID_OVERLAP_OSDL AS PAID_OVERLAP_OSDL,
  UNPAID_OVERLAP_OSDL AS UNPAID_OVERLAP_OSDL,
  PAID_OVERLAP_NSDL AS PAID_OVERLAP_NSDL,
  UNPAID_OVERLAP_NSDL AS UNPAID_OVERLAP_NSDL,
  PAID_OVERLAP_OSIL AS PAID_OVERLAP_OSIL,
  UNPAID_OVERLAP_OSIL AS UNPAID_OVERLAP_OSIL,
  PAID_OVERLAP_UDIL AS PAID_OVERLAP_UDIL,
  UNPAID_OVERLAP_UDIL AS UNPAID_OVERLAP_UDIL,
  VERSION_ID AS VERSION_ID,
  TEAM_CODE AS TEAM_CODE,
  DEFAULT_JF_FLAG AS DEFAULT_JF_FLAG,
  EMP_PERF_SMRY_ID AS EMP_PERF_SMRY_ID,
  TOTAL_QTY AS TOTAL_QTY,
  REF_NBR AS REF_NBR,
  TEAM_BEGIN_TIME AS TEAM_BEGIN_TIME,
  THRUPUT_MIN AS THRUPUT_MIN,
  DISPLAY_UOM_QTY AS DISPLAY_UOM_QTY,
  DISPLAY_UOM AS DISPLAY_UOM,
  LOCN_GRP_ATTR AS LOCN_GRP_ATTR,
  RESOURCE_GROUP_ID AS RESOURCE_GROUP_ID,
  COMP_ASSIGNMENT_ID AS COMP_ASSIGNMENT_ID,
  REFLECTIVE_CODE AS REFLECTIVE_CODE,
  SYSTIMESTAMP() AS LOAD_TSTMP_EXP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_E_CONSOL_PERF_SMRY_1""")

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, WM_E_CONSOL_PERF_SMRY_PRE


spark.sql("""INSERT INTO
  WM_E_CONSOL_PERF_SMRY_PRE
SELECT
  DC_NBR_EXP AS DC_NBR,
  PERF_SMRY_TRAN_ID AS PERF_SMRY_TRAN_ID,
  WHSE AS WHSE,
  LOGIN_USER_ID AS LOGIN_USER_ID,
  JOB_FUNCTION_NAME AS JOB_FUNCTION_NAME,
  SPVSR_LOGIN_USER_ID AS SPVSR_LOGIN_USER_ID,
  DEPT_CODE AS DEPT_CODE,
  CLOCK_IN_DATE AS CLOCK_IN_DATE,
  CLOCK_IN_STATUS AS CLOCK_IN_STATUS,
  TOTAL_SAM AS TOTAL_SAM,
  TOTAL_PAM AS TOTAL_PAM,
  TOTAL_TIME AS TOTAL_TIME,
  OSDL AS OSDL,
  OSIL AS OSIL,
  NSDL AS NSDL,
  SIL AS SIL,
  UDIL AS UDIL,
  UIL AS UIL,
  ADJ_OSDL AS ADJ_OSDL,
  ADJ_OSIL AS ADJ_OSIL,
  ADJ_UDIL AS ADJ_UDIL,
  ADJ_NSDL AS ADJ_NSDL,
  PAID_BRK AS PAID_BRK,
  UNPAID_BRK AS UNPAID_BRK,
  REF_OSDL AS REF_OSDL,
  REF_OSIL AS REF_OSIL,
  REF_UDIL AS REF_UDIL,
  REF_NSDL AS REF_NSDL,
  REF_ADJ_OSDL AS REF_ADJ_OSDL,
  REF_ADJ_OSIL AS REF_ADJ_OSIL,
  REF_ADJ_UDIL AS REF_ADJ_UDIL,
  REF_ADJ_NSDL AS REF_ADJ_NSDL,
  MISC_NUMBER_1 AS MISC_NUMBER_1,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  MISC_1 AS MISC_1,
  MISC_2 AS MISC_2,
  CLOCK_OUT_DATE AS CLOCK_OUT_DATE,
  SHIFT_CODE AS SHIFT_CODE,
  EVENT_COUNT AS EVENT_COUNT,
  START_DATE_TIME AS START_DATE_TIME,
  END_DATE_TIME AS END_DATE_TIME,
  LEVEL_1 AS LEVEL_1,
  LEVEL_2 AS LEVEL_2,
  LEVEL_3 AS LEVEL_3,
  LEVEL_4 AS LEVEL_4,
  LEVEL_5 AS LEVEL_5,
  WHSE_DATE AS WHSE_DATE,
  OPS_CODE AS OPS_CODE,
  REF_SAM AS REF_SAM,
  REF_PAM AS REF_PAM,
  REPORT_SHIFT AS REPORT_SHIFT,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  EVNT_CTGRY_1 AS EVNT_CTGRY_1,
  EVNT_CTGRY_2 AS EVNT_CTGRY_2,
  EVNT_CTGRY_3 AS EVNT_CTGRY_3,
  EVNT_CTGRY_4 AS EVNT_CTGRY_4,
  EVNT_CTGRY_5 AS EVNT_CTGRY_5,
  LABOR_COST_RATE AS LABOR_COST_RATE,
  PAID_OVERLAP_OSDL AS PAID_OVERLAP_OSDL,
  UNPAID_OVERLAP_OSDL AS UNPAID_OVERLAP_OSDL,
  PAID_OVERLAP_NSDL AS PAID_OVERLAP_NSDL,
  UNPAID_OVERLAP_NSDL AS UNPAID_OVERLAP_NSDL,
  PAID_OVERLAP_OSIL AS PAID_OVERLAP_OSIL,
  UNPAID_OVERLAP_OSIL AS UNPAID_OVERLAP_OSIL,
  PAID_OVERLAP_UDIL AS PAID_OVERLAP_UDIL,
  UNPAID_OVERLAP_UDIL AS UNPAID_OVERLAP_UDIL,
  VERSION_ID AS VERSION_ID,
  TEAM_CODE AS TEAM_CODE,
  DEFAULT_JF_FLAG AS DEFAULT_JF_FLAG,
  EMP_PERF_SMRY_ID AS EMP_PERF_SMRY_ID,
  TOTAL_QTY AS TOTAL_QTY,
  REF_NBR AS REF_NBR,
  TEAM_BEGIN_TIME AS TEAM_BEGIN_TIME,
  THRUPUT_MIN AS THRUPUT_MIN,
  DISPLAY_UOM_QTY AS DISPLAY_UOM_QTY,
  DISPLAY_UOM AS DISPLAY_UOM,
  LOCN_GRP_ATTR AS LOCN_GRP_ATTR,
  RESOURCE_GROUP_ID AS RESOURCE_GROUP_ID,
  COMP_ASSIGNMENT_ID AS COMP_ASSIGNMENT_ID,
  REFLECTIVE_CODE AS REFLECTIVE_CODE,
  LOAD_TSTMP_EXP AS LOAD_TSTMP
FROM
  EXPTRANS_2""")