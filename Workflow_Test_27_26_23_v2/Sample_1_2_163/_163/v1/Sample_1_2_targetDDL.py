# Databricks notebook source
# COMMAND ----------

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.NUODATA_TEST_DEPARTMENT(DC_NBR INT,
DEPT_ID INT,
DEPT_CODE STRING,
DESCRIPTION STRING,
CREATE_DATE_TIME TIMESTAMP,
MOD_DATE_TIME TIMESTAMP,
USER_ID STRING,
WHSE STRING,
MISC_TXT_1 STRING,
MISC_TXT_2 STRING,
MISC_NUM_1 INT,
MISC_NUM_2 INT,
PERF_GOAL INT,
VERSION_ID INT,
CREATED_DTTM TIMESTAMP,
LAST_UPDATED_DTTM TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.NUODATA_TEST_USER(DC_NBR INT,
UCL_USER_ID INT,
COMPANY_ID INT,
USER_NAME STRING,
USER_PASSWORD STRING,
IS_ACTIVE INT,
CREATED_SOURCE_TYPE_ID INT,
CREATED_SOURCE STRING,
CREATED_DTTM TIMESTAMP,
LAST_UPDATED_SOURCE_TYPE_ID INT,
LAST_UPDATED_SOURCE STRING,
LAST_UPDATED_DTTM TIMESTAMP,
USER_TYPE_ID INT,
LOCALE_ID INT,
LOCATION_ID INT,
USER_FIRST_NAME STRING,
USER_MIDDLE_NAME STRING,
USER_LAST_NAME STRING,
USER_PREFIX STRING,
USER_TITLE STRING,
TELEPHONE_NUMBER STRING,
FAX_NUMBER STRING,
ADDRESS_1 STRING,
ADDRESS_2 STRING,
CITY STRING,
STATE_PROV_CODE STRING,
POSTAL_CODE STRING,
COUNTRY_CODE STRING,
USER_EMAIL_1 STRING,
USER_EMAIL_2 STRING,
COMM_METHOD_ID_DURING_BH_1 INT,
COMM_METHOD_ID_DURING_BH_2 INT,
COMM_METHOD_ID_AFTER_BH_1 INT,
COMM_METHOD_ID_AFTER_BH_2 INT,
COMMON_NAME STRING,
LAST_PASSWORD_CHANGE_DTTM TIMESTAMP,
LOGGED_IN INT,
LAST_LOGIN_DTTM TIMESTAMP,
DEFAULT_BUSINESS_UNIT_ID INT,
DEFAULT_WHSE_REGION_ID INT,
CHANNEL_ID INT,
HIBERNATE_VERSION INT,
NUMBER_OF_INVALID_LOGINS INT,
TAX_ID_NBR STRING,
EMP_START_DATE TIMESTAMP,
BIRTH_DATE TIMESTAMP,
GENDER_ID STRING,
PASSWORD_RESET_DATE_TIME TIMESTAMP,
PASSWORD_TOKEN STRING,
ISPASSWORDMANAGEDINTERNALLY INT,
COPY_FROM_USER STRING,
EXTERNAL_USER_ID STRING,
SECURITY_POLICY_GROUP_ID INT,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.NUODATA_TEST_SUMMARY(DC_NBR INT,
PERF_SMRY_TRAN_ID INT,
WHSE STRING,
LOGIN_USER_ID STRING,
JOB_FUNCTION_NAME STRING,
SPVSR_LOGIN_USER_ID STRING,
DEPT_CODE STRING,
CLOCK_IN_DATE TIMESTAMP,
CLOCK_IN_STATUS INT,
TOTAL_SAM INT,
TOTAL_PAM INT,
TOTAL_TIME INT,
OSDL INT,
OSIL INT,
NSDL INT,
SIL INT,
UDIL INT,
UIL INT,
ADJ_OSDL INT,
ADJ_OSIL INT,
ADJ_UDIL INT,
ADJ_NSDL INT,
PAID_BRK INT,
UNPAID_BRK INT,
REF_OSDL INT,
REF_OSIL INT,
REF_UDIL INT,
REF_NSDL INT,
REF_ADJ_OSDL INT,
REF_ADJ_OSIL INT,
REF_ADJ_UDIL INT,
REF_ADJ_NSDL INT,
MISC_NUMBER_1 INT,
CREATE_DATE_TIME TIMESTAMP,
MOD_DATE_TIME TIMESTAMP,
USER_ID STRING,
MISC_1 STRING,
MISC_2 STRING,
CLOCK_OUT_DATE TIMESTAMP,
SHIFT_CODE STRING,
EVENT_COUNT INT,
START_DATE_TIME TIMESTAMP,
END_DATE_TIME TIMESTAMP,
LEVEL_1 STRING,
LEVEL_2 STRING,
LEVEL_3 STRING,
LEVEL_4 STRING,
LEVEL_5 STRING,
WHSE_DATE TIMESTAMP,
OPS_CODE STRING,
REF_SAM INT,
REF_PAM INT,
REPORT_SHIFT STRING,
MISC_TXT_1 STRING,
MISC_TXT_2 STRING,
MISC_NUM_1 INT,
MISC_NUM_2 INT,
EVNT_CTGRY_1 STRING,
EVNT_CTGRY_2 STRING,
EVNT_CTGRY_3 STRING,
EVNT_CTGRY_4 STRING,
EVNT_CTGRY_5 STRING,
LABOR_COST_RATE INT,
PAID_OVERLAP_OSDL INT,
UNPAID_OVERLAP_OSDL INT,
PAID_OVERLAP_NSDL INT,
UNPAID_OVERLAP_NSDL INT,
PAID_OVERLAP_OSIL INT,
UNPAID_OVERLAP_OSIL INT,
PAID_OVERLAP_UDIL INT,
UNPAID_OVERLAP_UDIL INT,
VERSION_ID INT,
TEAM_CODE STRING,
DEFAULT_JF_FLAG INT,
EMP_PERF_SMRY_ID INT,
TOTAL_QTY INT,
REF_NBR STRING,
TEAM_BEGIN_TIME TIMESTAMP,
THRUPUT_MIN INT,
DISPLAY_UOM_QTY INT,
DISPLAY_UOM STRING,
LOCN_GRP_ATTR STRING,
RESOURCE_GROUP_ID STRING,
COMP_ASSIGNMENT_ID STRING,
REFLECTIVE_CODE STRING,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.NUODATA_SUMMARY(LOCATION_ID INT,
WM_PERF_SMRY_TRAN_ID INT,
WM_WHSE STRING,
WM_LOGIN_USER_ID STRING,
WM_JOB_FUNCTION_NAME STRING,
CLOCK_IN_TSTMP TIMESTAMP,
CLOCK_OUT_TSTMP TIMESTAMP,
CLOCK_IN_STATUS INT,
START_TSTMP TIMESTAMP,
END_TSTMP TIMESTAMP,
WHSE_TSTMP TIMESTAMP,
TEAM_BEGIN_TSTMP TIMESTAMP,
WM_SHIFT_CD STRING,
WM_REPORT_SHIFT_CD STRING,
WM_USER_ID STRING,
WM_SPVSR_LOGIN_USER_ID STRING,
WM_DEPT_CD STRING,
WM_RESOURCE_GROUP_ID STRING,
WM_COMP_ASSIGNMENT_ID STRING,
WM_REFLECTIVE_CD STRING,
WM_EMP_PERF_SMRY_ID INT,
WM_LOCN_GRP_ATTR STRING,
WM_VERSION_ID INT,
WM_OPS_CD STRING,
WM_TEAM_CD STRING,
WM_REF_NBR STRING,
DEFAULT_JF_FLAG INT,
EVENT_CNT INT,
TOTAL_SAM INT,
TOTAL_PAM INT,
TOTAL_TIME INT,
TOTAL_QTY INT,
OSDL INT,
OSIL INT,
NSDL INT,
SIL INT,
UDIL INT,
UIL INT,
ADJ_OSDL INT,
ADJ_OSIL INT,
ADJ_UDIL INT,
ADJ_NSDL INT,
PAID_BRK INT,
UNPAID_BRK INT,
REF_OSDL INT,
REF_OSIL INT,
REF_UDIL INT,
REF_NSDL INT,
REF_ADJ_OSDL INT,
REF_ADJ_OSIL INT,
REF_ADJ_UDIL INT,
REF_ADJ_NSDL INT,
REF_SAM INT,
REF_PAM INT,
LABOR_COST_RATE INT,
THRUPUT_MIN INT,
PAID_OVERLAP_OSDL INT,
UNPAID_OVERLAP_OSDL INT,
PAID_OVERLAP_NSDL INT,
UNPAID_OVERLAP_NSDL INT,
PAID_OVERLAP_OSIL INT,
UNPAID_OVERLAP_OSIL INT,
PAID_OVERLAP_UDIL INT,
UNPAID_OVERLAP_UDIL INT,
DISPLAY_UOM_QTY INT,
DISPLAY_UOM STRING,
MISC_1 STRING,
MISC_2 STRING,
MISC_TXT_1 STRING,
MISC_TXT_2 STRING,
MISC_NBR_1 INT,
MISC_NUM_1 INT,
MISC_NUM_2 INT,
LEVEL_1 STRING,
LEVEL_2 STRING,
LEVEL_3 STRING,
LEVEL_4 STRING,
LEVEL_5 STRING,
WM_EVNT_CTGRY_1 STRING,
WM_EVNT_CTGRY_2 STRING,
WM_EVNT_CTGRY_3 STRING,
WM_EVNT_CTGRY_4 STRING,
WM_EVNT_CTGRY_5 STRING,
WM_CREATE_TSTMP TIMESTAMP,
WM_MOD_TSTMP TIMESTAMP,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.NUODATA_USER(LOCATION_ID INT,
WM_UCL_USER_ID INT,
WM_COMPANY_ID INT,
WM_LOCATION_ID INT,
WM_LOCALE_ID INT,
WM_USER_TYPE_ID INT,
ACTIVE_FLAG INT,
USER_NAME STRING,
TAX_ID_NBR STRING,
COMMON_NAME STRING,
USER_PREFIX STRING,
USER_TITLE STRING,
USER_FIRST_NAME STRING,
USER_MIDDLE_NAME STRING,
USER_LAST_NAME STRING,
BIRTH_DT DATE,
GENDER_ID STRING,
EMPLOYEE_START_DT DATE,
ADDR_1 STRING,
ADDR_2 STRING,
CITY STRING,
STATE_PROV_CD STRING,
POSTAL_CD STRING,
COUNTRY_CD STRING,
USER_EMAIL_1 STRING,
USER_EMAIL_2 STRING,
PHONE_NBR STRING,
FAX_NBR STRING,
WM_EXTERNAL_USER_ID STRING,
COPY_FROM_USER STRING,
WM_SECURITY_POLICY_GROUP_ID INT,
DEFAULT_WM_BUSINESS_UNIT_ID INT,
DEFAULT_WM_WHSE_REGION_ID INT,
WM_CHANNEL_ID INT,
WM_COMM_METHOD_ID_DURING_BH_1 INT,
WM_COMM_METHOD_ID_DURING_BH_2 INT,
WM_COMM_METHOD_ID_AFTER_BH_1 INT,
WM_COMM_METHOD_ID_AFTER_BH_2 INT,
PASSWORD_MANAGED_INTERNALLY_FLAG INT,
LOGGED_IN_FLAG INT,
LAST_LOGIN_TSTMP TIMESTAMP,
NUMBER_OF_INVALID_LOGINS INT,
PASSWORD_RESET_TSTMP TIMESTAMP,
LAST_PASSWORD_CHANGE_TSTMP TIMESTAMP,
WM_HIBERNATE_VERSION INT,
WM_CREATED_SOURCE_TYPE_ID INT,
WM_CREATED_SOURCE STRING,
WM_CREATED_TSTMP TIMESTAMP,
WM_LAST_UPDATED_SOURCE_TYPE_ID INT,
WM_LAST_UPDATED_SOURCE STRING,
WM_LAST_UPDATED_TSTMP TIMESTAMP,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.NUODATA_DEPARTMENT(LOCATION_ID INT,
WM_DEPT_ID INT,
WM_WHSE STRING,
WM_DEPT_CD STRING,
WM_DEPT_DESC STRING,
PERF_GOAL INT,
MISC_TXT_1 STRING,
MISC_TXT_2 STRING,
MISC_NUM_1 INT,
MISC_NUM_2 INT,
WM_USER_ID STRING,
WM_VERSION_ID INT,
WM_CREATED_TSTMP TIMESTAMP,
WM_LAST_UPDATED_TSTMP TIMESTAMP,
WM_CREATE_TSTMP TIMESTAMP,
WM_MOD_TSTMP TIMESTAMP,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;

CREATE TABLE IF NOT EXISTS DELTA_TRAINING.TEST_COURT(LOCATION_ID INT,
WM_YARD_ID INT,
WM_TC_COMPANY_ID INT,
WM_YARD_NAME STRING,
WM_LOCATION_ID INT,
WM_TIME_ZONE_ID INT,
GENERATE_MOVE_TASK_FLAG INT,
GENERATE_NEXT_EQUIP_FLAG INT,
RANGE_TASKS_FLAG INT,
SEAL_TASK_TRGD_FLAG INT,
OVERRIDE_SYSTEM_TASKS_FLAG INT,
TASKING_ALLOWED_FLAG INT,
LOCK_TRAILER_ON_MOVE_TO_DOOR_FLAG INT,
YARD_SVG_FILE STRING,
ADDRESS STRING,
CITY STRING,
STATE_PROV STRING,
POSTAL_CD STRING,
COUNTY STRING,
COUNTRY_CD STRING,
MAX_EQUIPMENT_ALLOWED INT,
UPPER_CHECK_IN_TIME_MINS INT,
LOWER_CHECK_IN_TIME_MINS INT,
FIXED_TIME_MINS INT,
THRESHOLD_PERCENT INT,
MARK_FOR_DELETION INT,
WM_CREATED_SOURCE_TYPE INT,
WM_CREATED_SOURCE STRING,
WM_CREATED_TSTMP TIMESTAMP,
WM_LAST_UPDATED_SOURCE_TYPE INT,
WM_LAST_UPDATED_SOURCE STRING,
WM_LAST_UPDATED_TSTMP TIMESTAMP,
UPDATE_TSTMP TIMESTAMP,
LOAD_TSTMP TIMESTAMP) USING DELTA;