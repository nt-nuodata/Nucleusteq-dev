# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ../WorkflowUtility

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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Facility_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Facility_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_FACILITY_0


query_0 = f"""SELECT
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  BUSINESS_PARTNER_ID AS BUSINESS_PARTNER_ID,
  NAME AS NAME,
  DESCRIPTION AS DESCRIPTION,
  ADDRESS_1 AS ADDRESS_1,
  ADDRESS_2 AS ADDRESS_2,
  ADDRESS_3 AS ADDRESS_3,
  ADDRESS_KEY_1 AS ADDRESS_KEY_1,
  CITY AS CITY,
  STATE_PROV AS STATE_PROV,
  POSTAL_CODE AS POSTAL_CODE,
  COUNTY AS COUNTY,
  COUNTRY_CODE AS COUNTRY_CODE,
  LONGITUDE AS LONGITUDE,
  LATITUDE AS LATITUDE,
  EIN_NBR AS EIN_NBR,
  IATA_CODE AS IATA_CODE,
  GLOBAL_LOCN_NBR AS GLOBAL_LOCN_NBR,
  TAX_ID AS TAX_ID,
  DUNS_NBR AS DUNS_NBR,
  INBOUNDS_RS_AREA_ID AS INBOUNDS_RS_AREA_ID,
  OUTBOUND_RS_AREA_ID AS OUTBOUND_RS_AREA_ID,
  INBOUND_REGION_ID AS INBOUND_REGION_ID,
  OUTBOUND_REGION_ID AS OUTBOUND_REGION_ID,
  FACILITY_TZ AS FACILITY_TZ,
  DROP_INDICATOR AS DROP_INDICATOR,
  HOOK_INDICATOR AS HOOK_INDICATOR,
  HANDLER AS HANDLER,
  IS_SHIP_APPT_REQD AS IS_SHIP_APPT_REQD,
  IS_RCV_APPT_REQD AS IS_RCV_APPT_REQD,
  TRACK_ONTIME_INDICATOR AS TRACK_ONTIME_INDICATOR,
  ONTIME_PERF_METHOD AS ONTIME_PERF_METHOD,
  LOAD_FACTOR_SIZE_VALUE AS LOAD_FACTOR_SIZE_VALUE,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ALLOW_OVERLAPPING_APPS AS ALLOW_OVERLAPPING_APPS,
  EARLY_TOLERANCE_POINT AS EARLY_TOLERANCE_POINT,
  LATE_TOLERANCE_POINT AS LATE_TOLERANCE_POINT,
  EARLY_TOLERANCE_WINDOW AS EARLY_TOLERANCE_WINDOW,
  LATE_TOLERANCE_WINDOW AS LATE_TOLERANCE_WINDOW,
  FACILITY_TYPE_BITS AS FACILITY_TYPE_BITS,
  PP_HANDLING_TIME AS PP_HANDLING_TIME,
  STOP_CONSTRAINT_BITS AS STOP_CONSTRAINT_BITS,
  IS_DOCK_SCHED_FAC AS IS_DOCK_SCHED_FAC,
  ASN_COMMUNICATION_METHOD AS ASN_COMMUNICATION_METHOD,
  PICK_TIME AS PICK_TIME,
  DISPATCH_INITIATION_TIME AS DISPATCH_INITIATION_TIME,
  PAPERWORK_TIME AS PAPERWORK_TIME,
  VEHICLE_CHECK_TIME AS VEHICLE_CHECK_TIME,
  VISIT_GAP AS VISIT_GAP,
  MIN_HANDLING_TIME AS MIN_HANDLING_TIME,
  IS_CREDIT_AVAILABLE AS IS_CREDIT_AVAILABLE,
  BUSINESS_GROUP_ID_1 AS BUSINESS_GROUP_ID_1,
  BUSINESS_GROUP_ID_2 AS BUSINESS_GROUP_ID_2,
  IS_OPERATIONAL AS IS_OPERATIONAL,
  IS_MAINTENANCE_FACILITY AS IS_MAINTENANCE_FACILITY,
  FIXED_TRUNK_HANDLING_TIME AS FIXED_TRUNK_HANDLING_TIME,
  DRIVER_CHECK_IN_TIME AS DRIVER_CHECK_IN_TIME,
  DISPATCH_DTTM AS DISPATCH_DTTM,
  DSP_DR_CONFIGURATION_ID AS DSP_DR_CONFIGURATION_ID,
  DROP_HOOK_TIME AS DROP_HOOK_TIME,
  LOADING_END_TIME AS LOADING_END_TIME,
  DRIVER_DEBRIEF_TIME AS DRIVER_DEBRIEF_TIME,
  ACCOUNT_CODE_NUMBER AS ACCOUNT_CODE_NUMBER,
  LOAD_FACTOR_MOT_ID AS LOAD_FACTOR_MOT_ID,
  LOAD_UNLOAD_MOT_ID AS LOAD_UNLOAD_MOT_ID,
  MIN_TRAIL_FILL AS MIN_TRAIL_FILL,
  TANDEM_DROP_HOOK_TIME AS TANDEM_DROP_HOOK_TIME,
  STORE_TYPE AS STORE_TYPE,
  DRIVEIN_TIME AS DRIVEIN_TIME,
  CHECKIN_TIME AS CHECKIN_TIME,
  DRIVEOUT_TIME AS DRIVEOUT_TIME,
  CHECKOUT_TIME AS CHECKOUT_TIME,
  CUTOFF_DTTM AS CUTOFF_DTTM,
  MAX_TRAILER_YARD_TIME AS MAX_TRAILER_YARD_TIME,
  HAND_OVER_TIME AS HAND_OVER_TIME,
  OVER_BOOK_PERCENTAGE AS OVER_BOOK_PERCENTAGE,
  RECOMMENDATION_CRITERIA AS RECOMMENDATION_CRITERIA,
  NBR_OF_SLOTS_TO_SHOW AS NBR_OF_SLOTS_TO_SHOW,
  RESTRICT_FLEET_ASMT_TIME AS RESTRICT_FLEET_ASMT_TIME,
  GROUP_ID AS GROUP_ID,
  IS_VTBP AS IS_VTBP,
  AUTO_CREATE_SHIPMENT AS AUTO_CREATE_SHIPMENT,
  LOGO_IMAGE_PATH AS LOGO_IMAGE_PATH,
  GEN_LAST_SHIPMENT_LEG AS GEN_LAST_SHIPMENT_LEG,
  DEF_DRIVER_TYPE_ID AS DEF_DRIVER_TYPE_ID,
  DEF_TRACTOR_ID AS DEF_TRACTOR_ID,
  DEF_EQUIPMENT_ID AS DEF_EQUIPMENT_ID,
  DRIVER_AVAIL_HRS AS DRIVER_AVAIL_HRS,
  WHSE AS WHSE,
  OPEN_DATE AS OPEN_DATE,
  CLOSE_DATE AS CLOSE_DATE,
  HOLD_DATE AS HOLD_DATE,
  GRP AS GRP,
  CHAIN AS CHAIN,
  ZONE AS ZONE,
  TERRITORY AS TERRITORY,
  REGION AS REGION,
  DISTRICT AS DISTRICT,
  SHIP_MON AS SHIP_MON,
  SHIP_TUE AS SHIP_TUE,
  SHIP_WED AS SHIP_WED,
  SHIP_THU AS SHIP_THU,
  SHIP_FRI AS SHIP_FRI,
  SHIP_SAT AS SHIP_SAT,
  SHIP_SU AS SHIP_SU,
  ACCEPT_IRREG AS ACCEPT_IRREG,
  WAVE_LABEL_TYPE AS WAVE_LABEL_TYPE,
  PKG_SLIP_TYPE AS PKG_SLIP_TYPE,
  PRINT_CODE AS PRINT_CODE,
  CARTON_CNT_TYPE AS CARTON_CNT_TYPE,
  SHIP_VIA AS SHIP_VIA,
  RTE_NBR AS RTE_NBR,
  RTE_ATTR AS RTE_ATTR,
  RTE_TO AS RTE_TO,
  RTE_TYPE_1 AS RTE_TYPE_1,
  RTE_TYPE_2 AS RTE_TYPE_2,
  RTE_ZIP AS RTE_ZIP,
  SPL_INSTR_CODE_1 AS SPL_INSTR_CODE_1,
  SPL_INSTR_CODE_2 AS SPL_INSTR_CODE_2,
  SPL_INSTR_CODE_3 AS SPL_INSTR_CODE_3,
  SPL_INSTR_CODE_4 AS SPL_INSTR_CODE_4,
  SPL_INSTR_CODE_5 AS SPL_INSTR_CODE_5,
  SPL_INSTR_CODE_6 AS SPL_INSTR_CODE_6,
  SPL_INSTR_CODE_7 AS SPL_INSTR_CODE_7,
  SPL_INSTR_CODE_8 AS SPL_INSTR_CODE_8,
  SPL_INSTR_CODE_9 AS SPL_INSTR_CODE_9,
  SPL_INSTR_CODE_10 AS SPL_INSTR_CODE_10,
  ASSIGN_MERCH_TYPE AS ASSIGN_MERCH_TYPE,
  ASSIGN_MERCH_GROUP AS ASSIGN_MERCH_GROUP,
  ASSIGN_STORE_DEPT AS ASSIGN_STORE_DEPT,
  CARTON_LABEL_TYPE AS CARTON_LABEL_TYPE,
  CARTON_CUBNG_INDIC AS CARTON_CUBNG_INDIC,
  MAX_CTN AS MAX_CTN,
  MAX_PLT AS MAX_PLT,
  BUSN_UNIT_CODE AS BUSN_UNIT_CODE,
  USE_INBD_LPN_AS_OUTBD_LPN AS USE_INBD_LPN_AS_OUTBD_LPN,
  PRINT_COO AS PRINT_COO,
  PRINT_INV AS PRINT_INV,
  PRINT_SED AS PRINT_SED,
  PRINT_CANADIAN_CUST_INVC_FLAG AS PRINT_CANADIAN_CUST_INVC_FLAG,
  PRINT_DOCK_RCPT_FLAG AS PRINT_DOCK_RCPT_FLAG,
  PRINT_NAFTA_COO_FLAG AS PRINT_NAFTA_COO_FLAG,
  PRINT_OCEAN_BOL_FLAG AS PRINT_OCEAN_BOL_FLAG,
  PRINT_PKG_LIST_FLAG AS PRINT_PKG_LIST_FLAG,
  PRINT_SHPR_LTR_OF_INSTR_FLAG AS PRINT_SHPR_LTR_OF_INSTR_FLAG,
  AUDIT_TRANSACTION AS AUDIT_TRANSACTION,
  AUDIT_PARTY_ID AS AUDIT_PARTY_ID,
  CAPTURE_OTHER_MA AS CAPTURE_OTHER_MA,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  STORE_TYPE_GROUPING AS STORE_TYPE_GROUPING,
  LOAD_RATE AS LOAD_RATE,
  UNLOAD_RATE AS UNLOAD_RATE,
  HANDLING_RATE AS HANDLING_RATE,
  LOAD_UNLOAD_SIZE_UOM_ID AS LOAD_UNLOAD_SIZE_UOM_ID,
  LOAD_FACTOR_SIZE_UOM_ID AS LOAD_FACTOR_SIZE_UOM_ID,
  PARCEL_LENGTH_RATIO AS PARCEL_LENGTH_RATIO,
  PARCEL_WIDTH_RATIO AS PARCEL_WIDTH_RATIO,
  PARCEL_HEIGHT_RATIO AS PARCEL_HEIGHT_RATIO,
  METER_NUMBER AS METER_NUMBER,
  DIRECT_DELIVERY_ALLOWED AS DIRECT_DELIVERY_ALLOWED,
  TRACK_EQUIP_ID_FLAG AS TRACK_EQUIP_ID_FLAG,
  STAT_CODE AS STAT_CODE,
  HANDLES_NON_MACHINEABLE AS HANDLES_NON_MACHINEABLE,
  MAINTAINS_PERPETUAL_INVTY AS MAINTAINS_PERPETUAL_INVTY,
  IS_CUST_OWNED_FACILITY AS IS_CUST_OWNED_FACILITY,
  FLOWTHRU_ALLOC_SORT_PRTY AS FLOWTHRU_ALLOC_SORT_PRTY,
  RLS_HOLD_DATE AS RLS_HOLD_DATE,
  RE_COMPUTE_FEASIBLE_EQUIPMENT AS RE_COMPUTE_FEASIBLE_EQUIPMENT,
  SMARTPOST_DC_NUMBER AS SMARTPOST_DC_NUMBER,
  ADDRESS_TYPE AS ADDRESS_TYPE,
  SPLC_CODE AS SPLC_CODE,
  ERPC_CODE AS ERPC_CODE,
  R260_CODE AS R260_CODE,
  MAX_WAIT_TIME AS MAX_WAIT_TIME,
  FREE_WAIT_TIME AS FREE_WAIT_TIME,
  IATA_SCR_NBR AS IATA_SCR_NBR,
  PICKUP_AT_STORE AS PICKUP_AT_STORE,
  SHIP_TO_STORE AS SHIP_TO_STORE,
  SHIP_FROM_FACILITY AS SHIP_FROM_FACILITY,
  TRANS_PLAN_FLOW AS TRANS_PLAN_FLOW,
  AVG_HNDLG_COST_PER_LN AS AVG_HNDLG_COST_PER_LN,
  AVG_HNDLG_COST_PER_LN_CURCODE AS AVG_HNDLG_COST_PER_LN_CURCODE,
  PENALTY_COST AS PENALTY_COST,
  PENALTY_COST_CURRENCY_CODE AS PENALTY_COST_CURRENCY_CODE
FROM
  FACILITY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_FACILITY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_FACILITY_1


query_1 = f"""SELECT
  FACILITY_ID AS FACILITY_ID,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  FACILITY_TYPE_BITS AS FACILITY_TYPE_BITS,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_FACILITY_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_FACILITY_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  FACILITY_ID AS FACILITY_ID,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  FACILITY_TYPE_BITS AS FACILITY_TYPE_BITS,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_FACILITY_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, OMS_FACILITY_PRE


spark.sql("""INSERT INTO
  OMS_FACILITY_PRE
SELECT
  FACILITY_ID AS FACILITY_ID,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  FACILITY_TYPE_BITS AS FACILITY_TYPE_BITS,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Facility_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Facility_Pre", mainWorkflowId, parentName)
