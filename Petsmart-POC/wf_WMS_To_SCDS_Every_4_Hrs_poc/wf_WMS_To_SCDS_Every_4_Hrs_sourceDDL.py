# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS DELTA_TRAINING;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.E_DEPT(DEPT_ID DECIMAL(9,0),
# MAGIC DEPT_CODE STRING,
# MAGIC DESCRIPTION STRING,
# MAGIC CREATE_DATE_TIME TIMESTAMP,
# MAGIC MOD_DATE_TIME DATE,
# MAGIC USER_ID STRING,
# MAGIC WHSE STRING,
# MAGIC MISC_TXT_1 STRING,
# MAGIC MISC_TXT_2 STRING,
# MAGIC MISC_NUM_1 DECIMAL(20,7),
# MAGIC MISC_NUM_2 DECIMAL(20,7),
# MAGIC PERF_GOAL DECIMAL(9,2),
# MAGIC VERSION_ID DECIMAL(6,0),
# MAGIC CREATED_DTTM TIMESTAMP,
# MAGIC LAST_UPDATED_DTTM TIMESTAMP) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.UCL_USER(UCL_USER_ID DECIMAL(18,0),
# MAGIC COMPANY_ID DECIMAL(9,0),
# MAGIC USER_NAME STRING,
# MAGIC USER_PASSWORD STRING,
# MAGIC IS_ACTIVE DECIMAL(4,0),
# MAGIC CREATED_SOURCE_TYPE_ID DECIMAL(4,0),
# MAGIC CREATED_SOURCE STRING,
# MAGIC CREATED_DTTM TIMESTAMP,
# MAGIC LAST_UPDATED_SOURCE_TYPE_ID DECIMAL(4,0),
# MAGIC LAST_UPDATED_SOURCE STRING,
# MAGIC LAST_UPDATED_DTTM TIMESTAMP,
# MAGIC USER_TYPE_ID DECIMAL(4,0),
# MAGIC LOCALE_ID DECIMAL(4,0),
# MAGIC LOCATION_ID DECIMAL(18,0),
# MAGIC USER_FIRST_NAME STRING,
# MAGIC USER_MIDDLE_NAME STRING,
# MAGIC USER_LAST_NAME STRING,
# MAGIC USER_PREFIX STRING,
# MAGIC USER_TITLE STRING,
# MAGIC TELEPHONE_NUMBER STRING,
# MAGIC FAX_NUMBER STRING,
# MAGIC ADDRESS_1 STRING,
# MAGIC ADDRESS_2 STRING,
# MAGIC CITY STRING,
# MAGIC STATE_PROV_CODE STRING,
# MAGIC POSTAL_CODE STRING,
# MAGIC COUNTRY_CODE STRING,
# MAGIC USER_EMAIL_1 STRING,
# MAGIC USER_EMAIL_2 STRING,
# MAGIC COMM_METHOD_ID_DURING_BH_1 DECIMAL(4,0),
# MAGIC COMM_METHOD_ID_DURING_BH_2 DECIMAL(4,0),
# MAGIC COMM_METHOD_ID_AFTER_BH_1 DECIMAL(4,0),
# MAGIC COMM_METHOD_ID_AFTER_BH_2 DECIMAL(4,0),
# MAGIC COMMON_NAME STRING,
# MAGIC LAST_PASSWORD_CHANGE_DTTM DATE,
# MAGIC LOGGED_IN DECIMAL(9,0),
# MAGIC LAST_LOGIN_DTTM DATE,
# MAGIC DEFAULT_BUSINESS_UNIT_ID DECIMAL(9,0),
# MAGIC DEFAULT_WHSE_REGION_ID DECIMAL(9,0),
# MAGIC CHANNEL_ID DECIMAL(18,0),
# MAGIC HIBERNATE_VERSION DECIMAL(10,0),
# MAGIC NUMBER_OF_INVALID_LOGINS DECIMAL(4,0),
# MAGIC TAX_ID_NBR STRING,
# MAGIC EMP_START_DATE DATE,
# MAGIC BIRTH_DATE DATE,
# MAGIC GENDER_ID STRING,
# MAGIC PASSWORD_RESET_DATE_TIME TIMESTAMP,
# MAGIC PASSWORD_TOKEN STRING,
# MAGIC ISPASSWORDMANAGEDINTERNALLY DECIMAL(1,0),
# MAGIC COPY_FROM_USER STRING,
# MAGIC EXTERNAL_USER_ID STRING,
# MAGIC SECURITY_POLICY_GROUP_ID DECIMAL(10,0)) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.E_CONSOL_PERF_SMRY(PERF_SMRY_TRAN_ID DECIMAL(20,0),
# MAGIC WHSE STRING,
# MAGIC LOGIN_USER_ID STRING,
# MAGIC JOB_FUNCTION_NAME STRING,
# MAGIC SPVSR_LOGIN_USER_ID STRING,
# MAGIC DEPT_CODE STRING,
# MAGIC CLOCK_IN_DATE TIMESTAMP,
# MAGIC CLOCK_IN_STATUS DECIMAL(3,0),
# MAGIC TOTAL_SAM DECIMAL(20,7),
# MAGIC TOTAL_PAM DECIMAL(13,5),
# MAGIC TOTAL_TIME DECIMAL(13,5),
# MAGIC OSDL DECIMAL(13,5),
# MAGIC OSIL DECIMAL(13,5),
# MAGIC NSDL DECIMAL(13,5),
# MAGIC SIL DECIMAL(13,5),
# MAGIC UDIL DECIMAL(13,5),
# MAGIC UIL DECIMAL(13,5),
# MAGIC ADJ_OSDL DECIMAL(13,5),
# MAGIC ADJ_OSIL DECIMAL(13,5),
# MAGIC ADJ_UDIL DECIMAL(13,5),
# MAGIC ADJ_NSDL DECIMAL(13,5),
# MAGIC PAID_BRK DECIMAL(13,5),
# MAGIC UNPAID_BRK DECIMAL(13,5),
# MAGIC REF_OSDL DECIMAL(13,5),
# MAGIC REF_OSIL DECIMAL(13,5),
# MAGIC REF_UDIL DECIMAL(13,5),
# MAGIC REF_NSDL DECIMAL(13,5),
# MAGIC REF_ADJ_OSDL DECIMAL(13,5),
# MAGIC REF_ADJ_OSIL DECIMAL(13,5),
# MAGIC REF_ADJ_UDIL DECIMAL(13,5),
# MAGIC REF_ADJ_NSDL DECIMAL(13,5),
# MAGIC MISC_NUMBER_1 DECIMAL(13,5),
# MAGIC CREATE_DATE_TIME DATE,
# MAGIC MOD_DATE_TIME DATE,
# MAGIC USER_ID STRING,
# MAGIC MISC_1 STRING,
# MAGIC MISC_2 STRING,
# MAGIC CLOCK_OUT_DATE TIMESTAMP,
# MAGIC SHIFT_CODE STRING,
# MAGIC EVENT_COUNT DECIMAL(9,0),
# MAGIC START_DATE_TIME TIMESTAMP,
# MAGIC END_DATE_TIME TIMESTAMP,
# MAGIC LEVEL_1 STRING,
# MAGIC LEVEL_2 STRING,
# MAGIC LEVEL_3 STRING,
# MAGIC LEVEL_4 STRING,
# MAGIC LEVEL_5 STRING,
# MAGIC WHSE_DATE DATE,
# MAGIC OPS_CODE STRING,
# MAGIC REF_SAM DECIMAL(13,5),
# MAGIC REF_PAM DECIMAL(13,5),
# MAGIC REPORT_SHIFT STRING,
# MAGIC MISC_TXT_1 STRING,
# MAGIC MISC_TXT_2 STRING,
# MAGIC MISC_NUM_1 DECIMAL(20,7),
# MAGIC MISC_NUM_2 DECIMAL(20,7),
# MAGIC EVNT_CTGRY_1 STRING,
# MAGIC EVNT_CTGRY_2 STRING,
# MAGIC EVNT_CTGRY_3 STRING,
# MAGIC EVNT_CTGRY_4 STRING,
# MAGIC EVNT_CTGRY_5 STRING,
# MAGIC LABOR_COST_RATE DECIMAL(20,7),
# MAGIC PAID_OVERLAP_OSDL DECIMAL(20,7),
# MAGIC UNPAID_OVERLAP_OSDL DECIMAL(20,7),
# MAGIC PAID_OVERLAP_NSDL DECIMAL(20,7),
# MAGIC UNPAID_OVERLAP_NSDL DECIMAL(20,7),
# MAGIC PAID_OVERLAP_OSIL DECIMAL(20,7),
# MAGIC UNPAID_OVERLAP_OSIL DECIMAL(20,7),
# MAGIC PAID_OVERLAP_UDIL DECIMAL(20,7),
# MAGIC UNPAID_OVERLAP_UDIL DECIMAL(20,7),
# MAGIC VERSION_ID DECIMAL(6,0),
# MAGIC TEAM_CODE STRING,
# MAGIC DEFAULT_JF_FLAG DECIMAL(9,0),
# MAGIC EMP_PERF_SMRY_ID DECIMAL(20,0),
# MAGIC TOTAL_QTY DECIMAL(13,5),
# MAGIC REF_NBR STRING,
# MAGIC TEAM_BEGIN_TIME TIMESTAMP,
# MAGIC THRUPUT_MIN DECIMAL(20,7),
# MAGIC DISPLAY_UOM_QTY DECIMAL(20,7),
# MAGIC DISPLAY_UOM STRING,
# MAGIC LOCN_GRP_ATTR STRING,
# MAGIC RESOURCE_GROUP_ID STRING,
# MAGIC COMP_ASSIGNMENT_ID STRING,
# MAGIC REFLECTIVE_CODE STRING) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.SITE_PROFILE(LOCATION_ID INT,
# MAGIC LOCATION_TYPE_ID TINYINT,
# MAGIC STORE_NBR INT,
# MAGIC STORE_NAME STRING,
# MAGIC STORE_TYPE_ID STRING,
# MAGIC STORE_OPEN_CLOSE_FLAG STRING,
# MAGIC COMPANY_ID INT,
# MAGIC REGION_ID LONG,
# MAGIC DISTRICT_ID LONG,
# MAGIC PRICE_ZONE_ID STRING,
# MAGIC PRICE_AD_ZONE_ID STRING,
# MAGIC REPL_DC_NBR INT,
# MAGIC REPL_FISH_DC_NBR INT,
# MAGIC REPL_FWD_DC_NBR INT,
# MAGIC SQ_FEET_RETAIL DOUBLE,
# MAGIC SQ_FEET_TOTAL DOUBLE,
# MAGIC SITE_ADDRESS STRING,
# MAGIC SITE_CITY STRING,
# MAGIC STATE_CD STRING,
# MAGIC COUNTRY_CD STRING,
# MAGIC POSTAL_CD STRING,
# MAGIC SITE_MAIN_TELE_NO STRING,
# MAGIC SITE_GROOM_TELE_NO STRING,
# MAGIC SITE_EMAIL_ADDRESS STRING,
# MAGIC SITE_SALES_FLAG STRING,
# MAGIC EQUINE_MERCH_ID TINYINT,
# MAGIC EQUINE_SITE_ID TINYINT,
# MAGIC EQUINE_SITE_OPEN_DT TIMESTAMP,
# MAGIC GEO_LATITUDE_NBR DECIMAL(12),
# MAGIC GEO_LONGITUDE_NBR DECIMAL(12),
# MAGIC PETSMART_DMA_CD STRING,
# MAGIC LOYALTY_PGM_TYPE_ID TINYINT,
# MAGIC LOYALTY_PGM_STATUS_ID TINYINT,
# MAGIC LOYALTY_PGM_START_DT TIMESTAMP,
# MAGIC LOYALTY_PGM_CHANGE_DT TIMESTAMP,
# MAGIC BP_COMPANY_NBR SMALLINT,
# MAGIC BP_GL_ACCT SMALLINT,
# MAGIC TP_LOC_FLAG STRING,
# MAGIC TP_ACTIVE_CNT TINYINT,
# MAGIC PROMO_LABEL_CD STRING,
# MAGIC PARENT_LOCATION_ID INT,
# MAGIC LOCATION_NBR STRING,
# MAGIC TIME_ZONE_ID STRING,
# MAGIC DELV_SERVICE_CLASS_ID STRING,
# MAGIC PICK_SERVICE_CLASS_ID STRING,
# MAGIC SITE_LOGIN_ID STRING,
# MAGIC SITE_MANAGER_ID INT,
# MAGIC SITE_OPEN_YRS_AMT DECIMAL(5),
# MAGIC HOTEL_FLAG TINYINT,
# MAGIC DAYCAMP_FLAG TINYINT,
# MAGIC VET_FLAG TINYINT,
# MAGIC DIST_MGR_NAME STRING,
# MAGIC DIST_SVC_MGR_NAME STRING,
# MAGIC REGION_VP_NAME STRING,
# MAGIC REGION_TRAINER_NAME STRING,
# MAGIC ASSET_PROTECT_NAME STRING,
# MAGIC SITE_COUNTY STRING,
# MAGIC SITE_FAX_NO STRING,
# MAGIC SFT_OPEN_DT TIMESTAMP,
# MAGIC DM_EMAIL_ADDRESS STRING,
# MAGIC DSM_EMAIL_ADDRESS STRING,
# MAGIC RVP_EMAIL_ADDRESS STRING,
# MAGIC TRADE_AREA STRING,
# MAGIC FDLPS_NAME STRING,
# MAGIC FDLPS_EMAIL STRING,
# MAGIC OVERSITE_MGR_NAME STRING,
# MAGIC OVERSITE_MGR_EMAIL STRING,
# MAGIC SAFETY_DIRECTOR_NAME STRING,
# MAGIC SAFETY_DIRECTOR_EMAIL STRING,
# MAGIC RETAIL_MANAGER_SAFETY_NAME STRING,
# MAGIC RETAIL_MANAGER_SAFETY_EMAIL STRING,
# MAGIC AREA_DIRECTOR_NAME STRING,
# MAGIC AREA_DIRECTOR_EMAIL STRING,
# MAGIC DC_GENERAL_MANAGER_NAME STRING,
# MAGIC DC_GENERAL_MANAGER_EMAIL STRING,
# MAGIC ASST_DC_GENERAL_MANAGER_NAME1 STRING,
# MAGIC ASST_DC_GENERAL_MANAGER_EMAIL1 STRING,
# MAGIC ASST_DC_GENERAL_MANAGER_NAME2 STRING,
# MAGIC ASST_DC_GENERAL_MANAGER_EMAIL2 STRING,
# MAGIC REGIONAL_DC_SAFETY_MGR_NAME STRING,
# MAGIC REGIONAL_DC_SAFETY_MGR_EMAIL STRING,
# MAGIC DC_PEOPLE_SUPERVISOR_NAME STRING,
# MAGIC DC_PEOPLE_SUPERVISOR_EMAIL STRING,
# MAGIC PEOPLE_MANAGER_NAME STRING,
# MAGIC PEOPLE_MANAGER_EMAIL STRING,
# MAGIC ASSET_PROT_DIR_NAME STRING,
# MAGIC ASSET_PROT_DIR_EMAIL STRING,
# MAGIC SR_REG_ASSET_PROT_MGR_NAME STRING,
# MAGIC SR_REG_ASSET_PROT_MGR_EMAIL STRING,
# MAGIC REG_ASSET_PROT_MGR_NAME STRING,
# MAGIC REG_ASSET_PROT_MGR_EMAIL STRING,
# MAGIC ASSET_PROTECT_EMAIL STRING,
# MAGIC TP_START_DT TIMESTAMP,
# MAGIC OPEN_DT TIMESTAMP,
# MAGIC GR_OPEN_DT TIMESTAMP,
# MAGIC CLOSE_DT TIMESTAMP,
# MAGIC HOTEL_OPEN_DT TIMESTAMP,
# MAGIC ADD_DT TIMESTAMP,
# MAGIC DELETE_DT TIMESTAMP,
# MAGIC UPDATE_DT TIMESTAMP,
# MAGIC LOAD_DT TIMESTAMP) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.WM_E_CONSOL_PERF_SMRY(LOCATION_ID INT,
# MAGIC WM_PERF_SMRY_TRAN_ID DECIMAL(20),
# MAGIC WM_WHSE STRING,
# MAGIC WM_LOGIN_USER_ID STRING,
# MAGIC WM_JOB_FUNCTION_NAME STRING,
# MAGIC CLOCK_IN_TSTMP TIMESTAMP,
# MAGIC CLOCK_OUT_TSTMP TIMESTAMP,
# MAGIC CLOCK_IN_STATUS DECIMAL(3),
# MAGIC START_TSTMP TIMESTAMP,
# MAGIC END_TSTMP TIMESTAMP,
# MAGIC WHSE_TSTMP TIMESTAMP,
# MAGIC TEAM_BEGIN_TSTMP TIMESTAMP,
# MAGIC WM_SHIFT_CD STRING,
# MAGIC WM_REPORT_SHIFT_CD STRING,
# MAGIC WM_USER_ID STRING,
# MAGIC WM_SPVSR_LOGIN_USER_ID STRING,
# MAGIC WM_DEPT_CD STRING,
# MAGIC WM_RESOURCE_GROUP_ID STRING,
# MAGIC WM_COMP_ASSIGNMENT_ID STRING,
# MAGIC WM_REFLECTIVE_CD STRING,
# MAGIC WM_EMP_PERF_SMRY_ID DECIMAL(20),
# MAGIC WM_LOCN_GRP_ATTR STRING,
# MAGIC WM_VERSION_ID DECIMAL(6),
# MAGIC WM_OPS_CD STRING,
# MAGIC WM_TEAM_CD STRING,
# MAGIC WM_REF_NBR STRING,
# MAGIC DEFAULT_JF_FLAG DECIMAL(9),
# MAGIC EVENT_CNT DECIMAL(9),
# MAGIC TOTAL_SAM DECIMAL(20),
# MAGIC TOTAL_PAM DECIMAL(13),
# MAGIC TOTAL_TIME DECIMAL(13),
# MAGIC TOTAL_QTY DECIMAL(13),
# MAGIC OSDL DECIMAL(13),
# MAGIC OSIL DECIMAL(13),
# MAGIC NSDL DECIMAL(13),
# MAGIC SIL DECIMAL(13),
# MAGIC UDIL DECIMAL(13),
# MAGIC UIL DECIMAL(13),
# MAGIC ADJ_OSDL DECIMAL(13),
# MAGIC ADJ_OSIL DECIMAL(13),
# MAGIC ADJ_UDIL DECIMAL(13),
# MAGIC ADJ_NSDL DECIMAL(13),
# MAGIC PAID_BRK DECIMAL(13),
# MAGIC UNPAID_BRK DECIMAL(13),
# MAGIC REF_OSDL DECIMAL(13),
# MAGIC REF_OSIL DECIMAL(13),
# MAGIC REF_UDIL DECIMAL(13),
# MAGIC REF_NSDL DECIMAL(13),
# MAGIC REF_ADJ_OSDL DECIMAL(13),
# MAGIC REF_ADJ_OSIL DECIMAL(13),
# MAGIC REF_ADJ_UDIL DECIMAL(13),
# MAGIC REF_ADJ_NSDL DECIMAL(13),
# MAGIC REF_SAM DECIMAL(13),
# MAGIC REF_PAM DECIMAL(13),
# MAGIC LABOR_COST_RATE DECIMAL(20),
# MAGIC THRUPUT_MIN DECIMAL(20),
# MAGIC PAID_OVERLAP_OSDL DECIMAL(20),
# MAGIC UNPAID_OVERLAP_OSDL DECIMAL(20),
# MAGIC PAID_OVERLAP_NSDL DECIMAL(20),
# MAGIC UNPAID_OVERLAP_NSDL DECIMAL(20),
# MAGIC PAID_OVERLAP_OSIL DECIMAL(20),
# MAGIC UNPAID_OVERLAP_OSIL DECIMAL(20),
# MAGIC PAID_OVERLAP_UDIL DECIMAL(20),
# MAGIC UNPAID_OVERLAP_UDIL DECIMAL(20),
# MAGIC DISPLAY_UOM_QTY DECIMAL(20),
# MAGIC DISPLAY_UOM STRING,
# MAGIC MISC_1 STRING,
# MAGIC MISC_2 STRING,
# MAGIC MISC_TXT_1 STRING,
# MAGIC MISC_TXT_2 STRING,
# MAGIC MISC_NBR_1 DECIMAL(13),
# MAGIC MISC_NUM_1 DECIMAL(20),
# MAGIC MISC_NUM_2 DECIMAL(20),
# MAGIC LEVEL_1 STRING,
# MAGIC LEVEL_2 STRING,
# MAGIC LEVEL_3 STRING,
# MAGIC LEVEL_4 STRING,
# MAGIC LEVEL_5 STRING,
# MAGIC WM_EVNT_CTGRY_1 STRING,
# MAGIC WM_EVNT_CTGRY_2 STRING,
# MAGIC WM_EVNT_CTGRY_3 STRING,
# MAGIC WM_EVNT_CTGRY_4 STRING,
# MAGIC WM_EVNT_CTGRY_5 STRING,
# MAGIC WM_CREATE_TSTMP TIMESTAMP,
# MAGIC WM_MOD_TSTMP TIMESTAMP,
# MAGIC UPDATE_TSTMP TIMESTAMP,
# MAGIC LOAD_TSTMP TIMESTAMP) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.WM_E_CONSOL_PERF_SMRY_PRE(DC_NBR DECIMAL(3),
# MAGIC PERF_SMRY_TRAN_ID DECIMAL(20),
# MAGIC WHSE STRING,
# MAGIC LOGIN_USER_ID STRING,
# MAGIC JOB_FUNCTION_NAME STRING,
# MAGIC SPVSR_LOGIN_USER_ID STRING,
# MAGIC DEPT_CODE STRING,
# MAGIC CLOCK_IN_DATE TIMESTAMP,
# MAGIC CLOCK_IN_STATUS DECIMAL(3),
# MAGIC TOTAL_SAM DECIMAL(20),
# MAGIC TOTAL_PAM DECIMAL(13),
# MAGIC TOTAL_TIME DECIMAL(13),
# MAGIC OSDL DECIMAL(13),
# MAGIC OSIL DECIMAL(13),
# MAGIC NSDL DECIMAL(13),
# MAGIC SIL DECIMAL(13),
# MAGIC UDIL DECIMAL(13),
# MAGIC UIL DECIMAL(13),
# MAGIC ADJ_OSDL DECIMAL(13),
# MAGIC ADJ_OSIL DECIMAL(13),
# MAGIC ADJ_UDIL DECIMAL(13),
# MAGIC ADJ_NSDL DECIMAL(13),
# MAGIC PAID_BRK DECIMAL(13),
# MAGIC UNPAID_BRK DECIMAL(13),
# MAGIC REF_OSDL DECIMAL(13),
# MAGIC REF_OSIL DECIMAL(13),
# MAGIC REF_UDIL DECIMAL(13),
# MAGIC REF_NSDL DECIMAL(13),
# MAGIC REF_ADJ_OSDL DECIMAL(13),
# MAGIC REF_ADJ_OSIL DECIMAL(13),
# MAGIC REF_ADJ_UDIL DECIMAL(13),
# MAGIC REF_ADJ_NSDL DECIMAL(13),
# MAGIC MISC_NUMBER_1 DECIMAL(13),
# MAGIC CREATE_DATE_TIME TIMESTAMP,
# MAGIC MOD_DATE_TIME TIMESTAMP,
# MAGIC USER_ID STRING,
# MAGIC MISC_1 STRING,
# MAGIC MISC_2 STRING,
# MAGIC CLOCK_OUT_DATE TIMESTAMP,
# MAGIC SHIFT_CODE STRING,
# MAGIC EVENT_COUNT DECIMAL(9),
# MAGIC START_DATE_TIME TIMESTAMP,
# MAGIC END_DATE_TIME TIMESTAMP,
# MAGIC LEVEL_1 STRING,
# MAGIC LEVEL_2 STRING,
# MAGIC LEVEL_3 STRING,
# MAGIC LEVEL_4 STRING,
# MAGIC LEVEL_5 STRING,
# MAGIC WHSE_DATE TIMESTAMP,
# MAGIC OPS_CODE STRING,
# MAGIC REF_SAM DECIMAL(13),
# MAGIC REF_PAM DECIMAL(13),
# MAGIC REPORT_SHIFT STRING,
# MAGIC MISC_TXT_1 STRING,
# MAGIC MISC_TXT_2 STRING,
# MAGIC MISC_NUM_1 DECIMAL(20),
# MAGIC MISC_NUM_2 DECIMAL(20),
# MAGIC EVNT_CTGRY_1 STRING,
# MAGIC EVNT_CTGRY_2 STRING,
# MAGIC EVNT_CTGRY_3 STRING,
# MAGIC EVNT_CTGRY_4 STRING,
# MAGIC EVNT_CTGRY_5 STRING,
# MAGIC LABOR_COST_RATE DECIMAL(20),
# MAGIC PAID_OVERLAP_OSDL DECIMAL(20),
# MAGIC UNPAID_OVERLAP_OSDL DECIMAL(20),
# MAGIC PAID_OVERLAP_NSDL DECIMAL(20),
# MAGIC UNPAID_OVERLAP_NSDL DECIMAL(20),
# MAGIC PAID_OVERLAP_OSIL DECIMAL(20),
# MAGIC UNPAID_OVERLAP_OSIL DECIMAL(20),
# MAGIC PAID_OVERLAP_UDIL DECIMAL(20),
# MAGIC UNPAID_OVERLAP_UDIL DECIMAL(20),
# MAGIC VERSION_ID DECIMAL(6),
# MAGIC TEAM_CODE STRING,
# MAGIC DEFAULT_JF_FLAG DECIMAL(9),
# MAGIC EMP_PERF_SMRY_ID DECIMAL(20),
# MAGIC TOTAL_QTY DECIMAL(13),
# MAGIC REF_NBR STRING,
# MAGIC TEAM_BEGIN_TIME TIMESTAMP,
# MAGIC THRUPUT_MIN DECIMAL(20),
# MAGIC DISPLAY_UOM_QTY DECIMAL(20),
# MAGIC DISPLAY_UOM STRING,
# MAGIC LOCN_GRP_ATTR STRING,
# MAGIC RESOURCE_GROUP_ID STRING,
# MAGIC COMP_ASSIGNMENT_ID STRING,
# MAGIC REFLECTIVE_CODE STRING,
# MAGIC LOAD_TSTMP TIMESTAMP) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.WM_UCL_USER(LOCATION_ID INT,
# MAGIC WM_UCL_USER_ID DECIMAL(18),
# MAGIC WM_COMPANY_ID DECIMAL(9),
# MAGIC WM_LOCATION_ID DECIMAL(18),
# MAGIC WM_LOCALE_ID DECIMAL(4),
# MAGIC WM_USER_TYPE_ID DECIMAL(4),
# MAGIC ACTIVE_FLAG DECIMAL(1),
# MAGIC USER_NAME STRING,
# MAGIC TAX_ID_NBR STRING,
# MAGIC COMMON_NAME STRING,
# MAGIC USER_PREFIX STRING,
# MAGIC USER_TITLE STRING,
# MAGIC USER_FIRST_NAME STRING,
# MAGIC USER_MIDDLE_NAME STRING,
# MAGIC USER_LAST_NAME STRING,
# MAGIC BIRTH_DT DATE,
# MAGIC GENDER_ID STRING,
# MAGIC EMPLOYEE_START_DT DATE,
# MAGIC ADDR_1 STRING,
# MAGIC ADDR_2 STRING,
# MAGIC CITY STRING,
# MAGIC STATE_PROV_CD STRING,
# MAGIC POSTAL_CD STRING,
# MAGIC COUNTRY_CD STRING,
# MAGIC USER_EMAIL_1 STRING,
# MAGIC USER_EMAIL_2 STRING,
# MAGIC PHONE_NBR STRING,
# MAGIC FAX_NBR STRING,
# MAGIC WM_EXTERNAL_USER_ID STRING,
# MAGIC COPY_FROM_USER STRING,
# MAGIC WM_SECURITY_POLICY_GROUP_ID DECIMAL(10),
# MAGIC DEFAULT_WM_BUSINESS_UNIT_ID DECIMAL(9),
# MAGIC DEFAULT_WM_WHSE_REGION_ID DECIMAL(9),
# MAGIC WM_CHANNEL_ID DECIMAL(18),
# MAGIC WM_COMM_METHOD_ID_DURING_BH_1 DECIMAL(4),
# MAGIC WM_COMM_METHOD_ID_DURING_BH_2 DECIMAL(4),
# MAGIC WM_COMM_METHOD_ID_AFTER_BH_1 DECIMAL(4),
# MAGIC WM_COMM_METHOD_ID_AFTER_BH_2 DECIMAL(4),
# MAGIC PASSWORD_MANAGED_INTERNALLY_FLAG DECIMAL(1),
# MAGIC LOGGED_IN_FLAG DECIMAL(1),
# MAGIC LAST_LOGIN_TSTMP TIMESTAMP,
# MAGIC NUMBER_OF_INVALID_LOGINS DECIMAL(4),
# MAGIC PASSWORD_RESET_TSTMP TIMESTAMP,
# MAGIC LAST_PASSWORD_CHANGE_TSTMP TIMESTAMP,
# MAGIC WM_HIBERNATE_VERSION DECIMAL(10),
# MAGIC WM_CREATED_SOURCE_TYPE_ID DECIMAL(4),
# MAGIC WM_CREATED_SOURCE STRING,
# MAGIC WM_CREATED_TSTMP TIMESTAMP,
# MAGIC WM_LAST_UPDATED_SOURCE_TYPE_ID DECIMAL(4),
# MAGIC WM_LAST_UPDATED_SOURCE STRING,
# MAGIC WM_LAST_UPDATED_TSTMP TIMESTAMP,
# MAGIC UPDATE_TSTMP TIMESTAMP,
# MAGIC LOAD_TSTMP TIMESTAMP) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.WM_UCL_USER_PRE(DC_NBR DECIMAL(3),
# MAGIC UCL_USER_ID DECIMAL(18),
# MAGIC COMPANY_ID DECIMAL(9),
# MAGIC USER_NAME STRING,
# MAGIC USER_PASSWORD STRING,
# MAGIC IS_ACTIVE DECIMAL(4),
# MAGIC CREATED_SOURCE_TYPE_ID DECIMAL(4),
# MAGIC CREATED_SOURCE STRING,
# MAGIC CREATED_DTTM TIMESTAMP,
# MAGIC LAST_UPDATED_SOURCE_TYPE_ID DECIMAL(4),
# MAGIC LAST_UPDATED_SOURCE STRING,
# MAGIC LAST_UPDATED_DTTM TIMESTAMP,
# MAGIC USER_TYPE_ID DECIMAL(4),
# MAGIC LOCALE_ID DECIMAL(4),
# MAGIC LOCATION_ID DECIMAL(18),
# MAGIC USER_FIRST_NAME STRING,
# MAGIC USER_MIDDLE_NAME STRING,
# MAGIC USER_LAST_NAME STRING,
# MAGIC USER_PREFIX STRING,
# MAGIC USER_TITLE STRING,
# MAGIC TELEPHONE_NUMBER STRING,
# MAGIC FAX_NUMBER STRING,
# MAGIC ADDRESS_1 STRING,
# MAGIC ADDRESS_2 STRING,
# MAGIC CITY STRING,
# MAGIC STATE_PROV_CODE STRING,
# MAGIC POSTAL_CODE STRING,
# MAGIC COUNTRY_CODE STRING,
# MAGIC USER_EMAIL_1 STRING,
# MAGIC USER_EMAIL_2 STRING,
# MAGIC COMM_METHOD_ID_DURING_BH_1 DECIMAL(4),
# MAGIC COMM_METHOD_ID_DURING_BH_2 DECIMAL(4),
# MAGIC COMM_METHOD_ID_AFTER_BH_1 DECIMAL(4),
# MAGIC COMM_METHOD_ID_AFTER_BH_2 DECIMAL(4),
# MAGIC COMMON_NAME STRING,
# MAGIC LAST_PASSWORD_CHANGE_DTTM TIMESTAMP,
# MAGIC LOGGED_IN DECIMAL(9),
# MAGIC LAST_LOGIN_DTTM TIMESTAMP,
# MAGIC DEFAULT_BUSINESS_UNIT_ID DECIMAL(9),
# MAGIC DEFAULT_WHSE_REGION_ID DECIMAL(9),
# MAGIC CHANNEL_ID DECIMAL(18),
# MAGIC HIBERNATE_VERSION DECIMAL(10),
# MAGIC NUMBER_OF_INVALID_LOGINS DECIMAL(4),
# MAGIC TAX_ID_NBR STRING,
# MAGIC EMP_START_DATE TIMESTAMP,
# MAGIC BIRTH_DATE TIMESTAMP,
# MAGIC GENDER_ID STRING,
# MAGIC PASSWORD_RESET_DATE_TIME TIMESTAMP,
# MAGIC PASSWORD_TOKEN STRING,
# MAGIC ISPASSWORDMANAGEDINTERNALLY DECIMAL(1),
# MAGIC COPY_FROM_USER STRING,
# MAGIC EXTERNAL_USER_ID STRING,
# MAGIC SECURITY_POLICY_GROUP_ID DECIMAL(10),
# MAGIC LOAD_TSTMP TIMESTAMP) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.WM_E_DEPT(LOCATION_ID INT,
# MAGIC WM_DEPT_ID DECIMAL(9),
# MAGIC WM_WHSE STRING,
# MAGIC WM_DEPT_CD STRING,
# MAGIC WM_DEPT_DESC STRING,
# MAGIC PERF_GOAL DECIMAL(9),
# MAGIC MISC_TXT_1 STRING,
# MAGIC MISC_TXT_2 STRING,
# MAGIC MISC_NUM_1 DECIMAL(20),
# MAGIC MISC_NUM_2 DECIMAL(20),
# MAGIC WM_USER_ID STRING,
# MAGIC WM_VERSION_ID DECIMAL(6),
# MAGIC WM_CREATED_TSTMP TIMESTAMP,
# MAGIC WM_LAST_UPDATED_TSTMP TIMESTAMP,
# MAGIC WM_CREATE_TSTMP TIMESTAMP,
# MAGIC WM_MOD_TSTMP TIMESTAMP,
# MAGIC UPDATE_TSTMP TIMESTAMP,
# MAGIC LOAD_TSTMP TIMESTAMP) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.WM_E_DEPT_PRE(DC_NBR DECIMAL(3),
# MAGIC DEPT_ID DECIMAL(9),
# MAGIC DEPT_CODE STRING,
# MAGIC DESCRIPTION STRING,
# MAGIC CREATE_DATE_TIME TIMESTAMP,
# MAGIC MOD_DATE_TIME TIMESTAMP,
# MAGIC USER_ID STRING,
# MAGIC WHSE STRING,
# MAGIC MISC_TXT_1 STRING,
# MAGIC MISC_TXT_2 STRING,
# MAGIC MISC_NUM_1 DECIMAL(20),
# MAGIC MISC_NUM_2 DECIMAL(20),
# MAGIC PERF_GOAL DECIMAL(9),
# MAGIC VERSION_ID DECIMAL(6),
# MAGIC CREATED_DTTM TIMESTAMP,
# MAGIC LAST_UPDATED_DTTM TIMESTAMP,
# MAGIC LOAD_TSTMP TIMESTAMP) USING DELTA;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS DELTA_TRAINING.DAYS(DAY_DT TIMESTAMP,
# MAGIC BUSINESS_DAY_FLAG STRING,
# MAGIC HOLIDAY_FLAG STRING,
# MAGIC DAY_OF_WK_NAME STRING,
# MAGIC DAY_OF_WK_NAME_ABBR STRING,
# MAGIC DAY_OF_WK_NBR TINYINT,
# MAGIC CAL_DAY_OF_MO_NBR TINYINT,
# MAGIC CAL_DAY_OF_YR_NBR SMALLINT,
# MAGIC CAL_WK INT,
# MAGIC CAL_WK_NBR TINYINT,
# MAGIC CAL_MO INT,
# MAGIC CAL_MO_NBR TINYINT,
# MAGIC CAL_MO_NAME STRING,
# MAGIC CAL_MO_NAME_ABBR STRING,
# MAGIC CAL_QTR INT,
# MAGIC CAL_QTR_NBR TINYINT,
# MAGIC CAL_HALF INT,
# MAGIC CAL_YR SMALLINT,
# MAGIC FISCAL_DAY_OF_MO_NBR TINYINT,
# MAGIC FISCAL_DAY_OF_YR_NBR SMALLINT,
# MAGIC FISCAL_WK INT,
# MAGIC FISCAL_WK_NBR TINYINT,
# MAGIC FISCAL_MO INT,
# MAGIC FISCAL_MO_NBR TINYINT,
# MAGIC FISCAL_MO_NAME STRING,
# MAGIC FISCAL_MO_NAME_ABBR STRING,
# MAGIC FISCAL_QTR INT,
# MAGIC FISCAL_QTR_NBR TINYINT,
# MAGIC FISCAL_HALF INT,
# MAGIC FISCAL_YR SMALLINT,
# MAGIC LYR_WEEK_DT TIMESTAMP,
# MAGIC LWK_WEEK_DT TIMESTAMP,
# MAGIC WEEK_DT TIMESTAMP,
# MAGIC EST_TIME_CONV_AMT DECIMAL(6),
# MAGIC EST_TIME_CONV_HRS TINYINT,
# MAGIC ES0_TIME_CONV_AMT DECIMAL(6),
# MAGIC ES0_TIME_CONV_HRS TINYINT,
# MAGIC CST_TIME_CONV_AMT DECIMAL(6),
# MAGIC CST_TIME_CONV_HRS TINYINT,
# MAGIC CS0_TIME_CONV_AMT DECIMAL(6),
# MAGIC CS0_TIME_CONV_HRS TINYINT,
# MAGIC MST_TIME_CONV_AMT DECIMAL(6),
# MAGIC MST_TIME_CONV_HRS TINYINT,
# MAGIC MS0_TIME_CONV_AMT DECIMAL(6),
# MAGIC MS0_TIME_CONV_HRS TINYINT,
# MAGIC PST_TIME_CONV_AMT DECIMAL(6),
# MAGIC PST_TIME_CONV_HRS TINYINT) USING DELTA;