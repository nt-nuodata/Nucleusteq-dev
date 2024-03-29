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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Shipment_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Shipment_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SHIPMENT_0


query_0 = f"""SELECT
  SHIPMENT_ID AS SHIPMENT_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  RS_AREA_ID AS RS_AREA_ID,
  TC_SHIPMENT_ID AS TC_SHIPMENT_ID,
  PP_SHIPMENT_ID AS PP_SHIPMENT_ID,
  SHIPMENT_STATUS AS SHIPMENT_STATUS,
  UPDATE_SENT AS UPDATE_SENT,
  BUSINESS_PROCESS AS BUSINESS_PROCESS,
  CREATION_TYPE AS CREATION_TYPE,
  CONS_RUN_ID AS CONS_RUN_ID,
  IS_CANCELLED AS IS_CANCELLED,
  RS_TYPE AS RS_TYPE,
  RS_CONFIG_ID AS RS_CONFIG_ID,
  RS_CONFIG_CYCLE_ID AS RS_CONFIG_CYCLE_ID,
  CONFIG_CYCLE_SEQ AS CONFIG_CYCLE_SEQ,
  CYCLE_DEADLINE_DTTM AS CYCLE_DEADLINE_DTTM,
  CYCLE_EXECUTION_DTTM AS CYCLE_EXECUTION_DTTM,
  LAST_RS_NOTIFICATION_DTTM AS LAST_RS_NOTIFICATION_DTTM,
  CFMF_STATUS AS CFMF_STATUS,
  AVAILABLE_DTTM AS AVAILABLE_DTTM,
  CMID AS CMID,
  CM_DISCOUNT AS CM_DISCOUNT,
  O_FACILITY_NUMBER AS O_FACILITY_NUMBER,
  O_FACILITY_ID AS O_FACILITY_ID,
  O_ADDRESS AS O_ADDRESS,
  O_CITY AS O_CITY,
  O_STATE_PROV AS O_STATE_PROV,
  O_POSTAL_CODE AS O_POSTAL_CODE,
  O_COUNTY AS O_COUNTY,
  O_COUNTRY_CODE AS O_COUNTRY_CODE,
  D_FACILITY_NUMBER AS D_FACILITY_NUMBER,
  D_FACILITY_ID AS D_FACILITY_ID,
  D_ADDRESS AS D_ADDRESS,
  D_CITY AS D_CITY,
  D_STATE_PROV AS D_STATE_PROV,
  D_POSTAL_CODE AS D_POSTAL_CODE,
  D_COUNTY AS D_COUNTY,
  D_COUNTRY_CODE AS D_COUNTRY_CODE,
  DISTANCE AS DISTANCE,
  DIRECT_DISTANCE AS DIRECT_DISTANCE,
  OUT_OF_ROUTE_DISTANCE AS OUT_OF_ROUTE_DISTANCE,
  DISTANCE_UOM AS DISTANCE_UOM,
  BUSINESS_PARTNER_ID AS BUSINESS_PARTNER_ID,
  COMMODITY_CLASS AS COMMODITY_CLASS,
  IS_HAZMAT AS IS_HAZMAT,
  IS_PERISHABLE AS IS_PERISHABLE,
  BILLING_METHOD AS BILLING_METHOD,
  DSG_CARRIER_CODE AS DSG_CARRIER_CODE,
  REC_CMID AS REC_CMID,
  REC_CM_SHIPMENT_ID AS REC_CM_SHIPMENT_ID,
  ASSIGNED_CARRIER_CODE AS ASSIGNED_CARRIER_CODE,
  SHIPMENT_REF_ID AS SHIPMENT_REF_ID,
  BILL_TO_CODE AS BILL_TO_CODE,
  BILL_TO_TITLE AS BILL_TO_TITLE,
  BILL_TO_NAME AS BILL_TO_NAME,
  BILL_TO_ADDRESS AS BILL_TO_ADDRESS,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILL_TO_STATE_PROV AS BILL_TO_STATE_PROV,
  BILL_TO_POSTAL_CODE AS BILL_TO_POSTAL_CODE,
  BILL_TO_COUNTRY_CODE AS BILL_TO_COUNTRY_CODE,
  BILL_TO_PHONE_NUMBER AS BILL_TO_PHONE_NUMBER,
  PURCHASE_ORDER AS PURCHASE_ORDER,
  PRO_NUMBER AS PRO_NUMBER,
  TRANS_RESP_CODE AS TRANS_RESP_CODE,
  BASELINE_COST AS BASELINE_COST,
  ESTIMATED_COST AS ESTIMATED_COST,
  LINEHAUL_COST AS LINEHAUL_COST,
  ACCESSORIAL_COST AS ACCESSORIAL_COST,
  STOP_COST AS STOP_COST,
  ESTIMATED_SAVINGS AS ESTIMATED_SAVINGS,
  SPOT_CHARGE AS SPOT_CHARGE,
  CURRENCY_CODE AS CURRENCY_CODE,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  NUM_STOPS AS NUM_STOPS,
  NUM_DOCKS AS NUM_DOCKS,
  ON_TIME_INDICATOR AS ON_TIME_INDICATOR,
  HAS_NOTES AS HAS_NOTES,
  HAS_ALERTS AS HAS_ALERTS,
  HAS_IMPORT_ERROR AS HAS_IMPORT_ERROR,
  HAS_SOFT_CHECK_ERROR AS HAS_SOFT_CHECK_ERROR,
  HAS_TRACKING_MSG AS HAS_TRACKING_MSG,
  TRACKING_MSG_PROBLEM AS TRACKING_MSG_PROBLEM,
  REC_LINEHAUL_COST AS REC_LINEHAUL_COST,
  REC_ACCESSORIAL_COST AS REC_ACCESSORIAL_COST,
  REC_TOTAL_COST AS REC_TOTAL_COST,
  CYCLE_RESP_DEADLINE_TZ AS CYCLE_RESP_DEADLINE_TZ,
  TENDER_RESP_DEADLINE_TZ AS TENDER_RESP_DEADLINE_TZ,
  PICKUP_TZ AS PICKUP_TZ,
  DELIVERY_TZ AS DELIVERY_TZ,
  ASSIGNED_LANE_ID AS ASSIGNED_LANE_ID,
  ASSIGNED_LANE_DETAIL_ID AS ASSIGNED_LANE_DETAIL_ID,
  REC_SPOT_CHARGE AS REC_SPOT_CHARGE,
  REC_LANE_ID AS REC_LANE_ID,
  REC_LANE_DETAIL_ID AS REC_LANE_DETAIL_ID,
  TENDER_DTTM AS TENDER_DTTM,
  O_STOP_LOCATION_NAME AS O_STOP_LOCATION_NAME,
  D_STOP_LOCATION_NAME AS D_STOP_LOCATION_NAME,
  ASSIGNED_CM_SHIPMENT_ID AS ASSIGNED_CM_SHIPMENT_ID,
  REC_CM_DISCOUNT AS REC_CM_DISCOUNT,
  REC_STOP_COST AS REC_STOP_COST,
  LAST_SELECTOR_RUN_DTTM AS LAST_SELECTOR_RUN_DTTM,
  LAST_CM_OPTION_GEN_DTTM AS LAST_CM_OPTION_GEN_DTTM,
  IS_CM_OPTION_GEN_ACTIVE AS IS_CM_OPTION_GEN_ACTIVE,
  RS_CYCLE_REMAINING AS RS_CYCLE_REMAINING,
  IS_TIME_FEAS_ENABLED AS IS_TIME_FEAS_ENABLED,
  EXT_SYS_SHIPMENT_ID AS EXT_SYS_SHIPMENT_ID,
  EXTRACTION_DTTM AS EXTRACTION_DTTM,
  ASSIGNED_BROKER_CARRIER_CODE AS ASSIGNED_BROKER_CARRIER_CODE,
  USE_BROKER_AS_CARRIER AS USE_BROKER_AS_CARRIER,
  BROKER_REF AS BROKER_REF,
  RATING_QUALIFIER AS RATING_QUALIFIER,
  STATUS_CHANGE_DTTM AS STATUS_CHANGE_DTTM,
  BUDG_TOTAL_COST AS BUDG_TOTAL_COST,
  NORMALIZED_MARGIN AS NORMALIZED_MARGIN,
  NORMALIZED_TOTAL_COST AS NORMALIZED_TOTAL_COST,
  BUDG_NORMALIZED_TOTAL_COST AS BUDG_NORMALIZED_TOTAL_COST,
  REC_BUDG_LINEHAUL_COST AS REC_BUDG_LINEHAUL_COST,
  REC_BUDG_STOP_COST AS REC_BUDG_STOP_COST,
  REC_BUDG_CM_DISCOUNT AS REC_BUDG_CM_DISCOUNT,
  REC_BUDG_ACCESSORIAL_COST AS REC_BUDG_ACCESSORIAL_COST,
  REC_BUDG_TOTAL_COST AS REC_BUDG_TOTAL_COST,
  REC_MARGIN AS REC_MARGIN,
  REC_NORMALIZED_MARGIN AS REC_NORMALIZED_MARGIN,
  REC_NORMALIZED_TOTAL_COST AS REC_NORMALIZED_TOTAL_COST,
  REC_BUDG_NORMALIZED_TOTAL_COST AS REC_BUDG_NORMALIZED_TOTAL_COST,
  BUDG_CURRENCY_CODE AS BUDG_CURRENCY_CODE,
  SPOT_CHARGE_CURRENCY_CODE AS SPOT_CHARGE_CURRENCY_CODE,
  REC_CURRENCY_CODE AS REC_CURRENCY_CODE,
  REC_BUDG_CURRENCY_CODE AS REC_BUDG_CURRENCY_CODE,
  REC_SPOT_CHARGE_CURRENCY_CODE AS REC_SPOT_CHARGE_CURRENCY_CODE,
  RATING_LANE_ID AS RATING_LANE_ID,
  FRT_REV_RATING_LANE_ID AS FRT_REV_RATING_LANE_ID,
  REC_RATING_LANE_ID AS REC_RATING_LANE_ID,
  REC_BUDG_RATING_LANE_ID AS REC_BUDG_RATING_LANE_ID,
  RATING_LANE_DETAIL_ID AS RATING_LANE_DETAIL_ID,
  FRT_REV_RATING_LANE_DETAIL_ID AS FRT_REV_RATING_LANE_DETAIL_ID,
  REC_RATING_LANE_DETAIL_ID AS REC_RATING_LANE_DETAIL_ID,
  REC_BUDG_RATING_LANE_DETAIL_ID AS REC_BUDG_RATING_LANE_DETAIL_ID,
  BUDG_CM_DISCOUNT AS BUDG_CM_DISCOUNT,
  NORMALIZED_BASELINE_COST AS NORMALIZED_BASELINE_COST,
  BASELINE_COST_CURRENCY_CODE AS BASELINE_COST_CURRENCY_CODE,
  REPORTED_COST AS REPORTED_COST,
  TRACTOR_NUMBER AS TRACTOR_NUMBER,
  IS_AUTO_DELIVERED AS IS_AUTO_DELIVERED,
  SHIPMENT_TYPE AS SHIPMENT_TYPE,
  ORIG_BUDG_TOTAL_COST AS ORIG_BUDG_TOTAL_COST,
  ACCESSORIAL_COST_TO_CARRIER AS ACCESSORIAL_COST_TO_CARRIER,
  CARRIER_CHARGE AS CARRIER_CHARGE,
  ACTUAL_COST AS ACTUAL_COST,
  EARNED_INCOME AS EARNED_INCOME,
  SENT_TO_PKMS AS SENT_TO_PKMS,
  IS_SHIPMENT_RECONCILED AS IS_SHIPMENT_RECONCILED,
  DELIVERY_REQ AS DELIVERY_REQ,
  DROPOFF_PICKUP AS DROPOFF_PICKUP,
  PACKAGING AS PACKAGING,
  SENT_TO_PKMS_DTTM AS SENT_TO_PKMS_DTTM,
  WMS_STATUS_CODE AS WMS_STATUS_CODE,
  SPOT_CHARGE_AND_PAYEE_ACC AS SPOT_CHARGE_AND_PAYEE_ACC,
  SPOT_CHARGE_AND_PAYEE_ACC_CC AS SPOT_CHARGE_AND_PAYEE_ACC_CC,
  NORM_SPOT_CHARGE_AND_PAYEE_ACC AS NORM_SPOT_CHARGE_AND_PAYEE_ACC,
  FACILITY_SCHEDULE_ID AS FACILITY_SCHEDULE_ID,
  EARNED_INCOME_CURRENCY_CODE AS EARNED_INCOME_CURRENCY_CODE,
  ACTUAL_COST_CURRENCY_CODE AS ACTUAL_COST_CURRENCY_CODE,
  SHIPMENT_RECON_DTTM AS SHIPMENT_RECON_DTTM,
  PLN_RATING_LANE_ID AS PLN_RATING_LANE_ID,
  PLN_RATING_LANE_DETAIL_ID AS PLN_RATING_LANE_DETAIL_ID,
  PLN_TOTAL_COST AS PLN_TOTAL_COST,
  PLN_LINEHAUL_COST AS PLN_LINEHAUL_COST,
  PLN_TOTAL_ACCESSORIAL_COST AS PLN_TOTAL_ACCESSORIAL_COST,
  PLN_STOP_OFF_COST AS PLN_STOP_OFF_COST,
  PLN_NORMALIZED_TOTAL_COST AS PLN_NORMALIZED_TOTAL_COST,
  PLN_CURRENCY_CODE AS PLN_CURRENCY_CODE,
  PLN_ACCESSORL_COST_TO_CARRIER AS PLN_ACCESSORL_COST_TO_CARRIER,
  PLN_CARRIER_CHARGE AS PLN_CARRIER_CHARGE,
  WAYPOINT_TOTAL_COST AS WAYPOINT_TOTAL_COST,
  WAYPOINT_HANDLING_COST AS WAYPOINT_HANDLING_COST,
  GRS_OPERATION AS GRS_OPERATION,
  IS_GRS_OPT_CYCLE_RUNNING AS IS_GRS_OPT_CYCLE_RUNNING,
  IS_MANUAL_ASSIGN AS IS_MANUAL_ASSIGN,
  LAST_RUN_GRS_DTTM AS LAST_RUN_GRS_DTTM,
  GRS_MAX_SHIPMENT_STATUS AS GRS_MAX_SHIPMENT_STATUS,
  PRIORITY_TYPE AS PRIORITY_TYPE,
  MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  PROD_SCHED_REF_NUMBER AS PROD_SCHED_REF_NUMBER,
  COMMODITY_CODE_ID AS COMMODITY_CODE_ID,
  UN_NUMBER_ID AS UN_NUMBER_ID,
  CONTRACT_NUMBER AS CONTRACT_NUMBER,
  BOOKING_REF_SHIPPER AS BOOKING_REF_SHIPPER,
  BOOKING_REF_CARRIER AS BOOKING_REF_CARRIER,
  BK_RESOURCE_REF_EXTERNAL AS BK_RESOURCE_REF_EXTERNAL,
  BK_RESOURCE_NAME_EXTERNAL AS BK_RESOURCE_NAME_EXTERNAL,
  BK_O_FACILITY_ID AS BK_O_FACILITY_ID,
  BK_D_FACILITY_ID AS BK_D_FACILITY_ID,
  BK_MASTER_AIRWAY_BILL AS BK_MASTER_AIRWAY_BILL,
  BK_FORWARDER_AIRWAY_BILL AS BK_FORWARDER_AIRWAY_BILL,
  ASSIGNED_SCNDR_CARRIER_CODE AS ASSIGNED_SCNDR_CARRIER_CODE,
  CUSTOMER_CREDIT_LIMIT_ID AS CUSTOMER_CREDIT_LIMIT_ID,
  SHIPMENT_LEG_TYPE AS SHIPMENT_LEG_TYPE,
  SHIPMENT_CLOSED_INDICATOR AS SHIPMENT_CLOSED_INDICATOR,
  OCEAN_ROUTING_STAGE AS OCEAN_ROUTING_STAGE,
  BK_DEPARTURE_DTTM AS BK_DEPARTURE_DTTM,
  BK_DEPARTURE_TZ AS BK_DEPARTURE_TZ,
  BK_ARRIVAL_DTTM AS BK_ARRIVAL_DTTM,
  BK_ARRIVAL_TZ AS BK_ARRIVAL_TZ,
  BK_PICKUP_DTTM AS BK_PICKUP_DTTM,
  BK_PICKUP_TZ AS BK_PICKUP_TZ,
  BK_CUTOFF_DTTM AS BK_CUTOFF_DTTM,
  BK_CUTOFF_TZ AS BK_CUTOFF_TZ,
  NUM_CHARGE_LAYOVERS AS NUM_CHARGE_LAYOVERS,
  RATE_TYPE AS RATE_TYPE,
  RATE_UOM AS RATE_UOM,
  PICK_START_DTTM AS PICK_START_DTTM,
  PAPERWORK_START_DTTM AS PAPERWORK_START_DTTM,
  VEHICLE_CHECK_START_DTTM AS VEHICLE_CHECK_START_DTTM,
  SHIPMENT_START_DTTM AS SHIPMENT_START_DTTM,
  SHIPMENT_END_DTTM AS SHIPMENT_END_DTTM,
  FEASIBLE_DRIVER_TYPE AS FEASIBLE_DRIVER_TYPE,
  WAVE_ID AS WAVE_ID,
  ESTIMATED_DISPATCH_DTTM AS ESTIMATED_DISPATCH_DTTM,
  TOTAL_TIME AS TOTAL_TIME,
  LOC_REFERENCE AS LOC_REFERENCE,
  BK_O_FACILITY_ALIAS_ID AS BK_O_FACILITY_ALIAS_ID,
  BK_D_FACILITY_ALIAS_ID AS BK_D_FACILITY_ALIAS_ID,
  EQUIP_UTIL_PER AS EQUIP_UTIL_PER,
  MOVE_TYPE AS MOVE_TYPE,
  DRIVER_TYPE_ID AS DRIVER_TYPE_ID,
  EQUIPMENT_TYPE AS EQUIPMENT_TYPE,
  RADIAL_DISTANCE AS RADIAL_DISTANCE,
  RADIAL_DISTANCE_UOM AS RADIAL_DISTANCE_UOM,
  DESIGNATED_TRACTOR_CODE AS DESIGNATED_TRACTOR_CODE,
  DESIGNATED_DRIVER_TYPE AS DESIGNATED_DRIVER_TYPE,
  IS_WAVE_MAN_CHANGED AS IS_WAVE_MAN_CHANGED,
  RETAIN_CONSOLIDATOR_TIMES AS RETAIN_CONSOLIDATOR_TIMES,
  TARIFF AS TARIFF,
  MIN_RATE AS MIN_RATE,
  FIRST_UPDATE_SENT_TO_PKMS AS FIRST_UPDATE_SENT_TO_PKMS,
  EVENT_IND_TYPEID AS EVENT_IND_TYPEID,
  TANDEM_PATH_ID AS TANDEM_PATH_ID,
  O_TANDEM_FACILITY AS O_TANDEM_FACILITY,
  D_TANDEM_FACILITY AS D_TANDEM_FACILITY,
  O_TANDEM_FACILITY_ALIAS AS O_TANDEM_FACILITY_ALIAS,
  D_TANDEM_FACILITY_ALIAS AS D_TANDEM_FACILITY_ALIAS,
  DELAY_TYPE AS DELAY_TYPE,
  ASSIGNED_CARRIER_ID AS ASSIGNED_CARRIER_ID,
  DSG_CARRIER_ID AS DSG_CARRIER_ID,
  REC_CARRIER_ID AS REC_CARRIER_ID,
  FEASIBLE_CARRIER_ID AS FEASIBLE_CARRIER_ID,
  PAYEE_CARRIER_ID AS PAYEE_CARRIER_ID,
  SCNDR_CARRIER_ID AS SCNDR_CARRIER_ID,
  BROKER_CARRIER_ID AS BROKER_CARRIER_ID,
  ASSIGNED_SCNDR_CARRIER_ID AS ASSIGNED_SCNDR_CARRIER_ID,
  DSG_SCNDR_CARRIER_ID AS DSG_SCNDR_CARRIER_ID,
  ASSIGNED_BROKER_CARRIER_ID AS ASSIGNED_BROKER_CARRIER_ID,
  LH_PAYEE_CARRIER_ID AS LH_PAYEE_CARRIER_ID,
  REC_BROKER_CARRIER_ID AS REC_BROKER_CARRIER_ID,
  ASSIGNED_EQUIPMENT_ID AS ASSIGNED_EQUIPMENT_ID,
  DSG_EQUIPMENT_ID AS DSG_EQUIPMENT_ID,
  FEASIBLE_EQUIPMENT_ID AS FEASIBLE_EQUIPMENT_ID,
  FEASIBLE_EQUIPMENT2_ID AS FEASIBLE_EQUIPMENT2_ID,
  REC_EQUIPMENT_ID AS REC_EQUIPMENT_ID,
  ASSIGNED_MOT_ID AS ASSIGNED_MOT_ID,
  DSG_MOT_ID AS DSG_MOT_ID,
  FEASIBLE_MOT_ID AS FEASIBLE_MOT_ID,
  REC_MOT_ID AS REC_MOT_ID,
  PRODUCT_CLASS_ID AS PRODUCT_CLASS_ID,
  PROTECTION_LEVEL_ID AS PROTECTION_LEVEL_ID,
  ASSIGNED_SERVICE_LEVEL_ID AS ASSIGNED_SERVICE_LEVEL_ID,
  DSG_SERVICE_LEVEL_ID AS DSG_SERVICE_LEVEL_ID,
  FEASIBLE_SERVICE_LEVEL_ID AS FEASIBLE_SERVICE_LEVEL_ID,
  REC_SERVICE_LEVEL_ID AS REC_SERVICE_LEVEL_ID,
  HAS_EM_NOTIFY_FLAG AS HAS_EM_NOTIFY_FLAG,
  REGION_ID AS REGION_ID,
  INBOUND_REGION_ID AS INBOUND_REGION_ID,
  OUTBOUND_REGION_ID AS OUTBOUND_REGION_ID,
  FINANCIAL_WT AS FINANCIAL_WT,
  IS_BOOKING_REQUIRED AS IS_BOOKING_REQUIRED,
  TOTAL_COST_EXCL_TAX AS TOTAL_COST_EXCL_TAX,
  TOTAL_TAX_AMOUNT AS TOTAL_TAX_AMOUNT,
  IS_ASSOCIATED_TO_OUTBOUND AS IS_ASSOCIATED_TO_OUTBOUND,
  RECEIVED_DTTM AS RECEIVED_DTTM,
  BOOKING_ID AS BOOKING_ID,
  IS_MISROUTED AS IS_MISROUTED,
  FEASIBLE_CARRIER_CODE AS FEASIBLE_CARRIER_CODE,
  SHIPMENT_WIN_ADJ_FLAG AS SHIPMENT_WIN_ADJ_FLAG,
  DT_PARAM_SET_ID AS DT_PARAM_SET_ID,
  MERCHANDIZING_DEPARTMENT_ID AS MERCHANDIZING_DEPARTMENT_ID,
  STATIC_ROUTE_ID AS STATIC_ROUTE_ID,
  IS_FILO AS IS_FILO,
  IS_COOLER_AT_NOSE AS IS_COOLER_AT_NOSE,
  AUTH_NBR AS AUTH_NBR,
  CONS_LOCN_ID AS CONS_LOCN_ID,
  RTE_TYPE AS RTE_TYPE,
  TRLR_TYPE AS TRLR_TYPE,
  TRLR_SIZE AS TRLR_SIZE,
  CONS_ADDR_CODE AS CONS_ADDR_CODE,
  HUB_ID AS HUB_ID,
  TRLR_GEN_CODE AS TRLR_GEN_CODE,
  MAX_NBR_OF_CTNS AS MAX_NBR_OF_CTNS,
  APPT_DOOR_SCHED_TYPE AS APPT_DOOR_SCHED_TYPE,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DSG_SCNDR_CARRIER_CODE AS DSG_SCNDR_CARRIER_CODE,
  LH_PAYEE_CARRIER_CODE AS LH_PAYEE_CARRIER_CODE,
  REC_BROKER_CARRIER_CODE AS REC_BROKER_CARRIER_CODE,
  REC_CARRIER_CODE AS REC_CARRIER_CODE,
  LPN_ASSIGNMENT_STOPPED AS LPN_ASSIGNMENT_STOPPED,
  SEAL_NUMBER AS SEAL_NUMBER,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  SHIP_GROUP_ID AS SHIP_GROUP_ID,
  RATE AS RATE,
  MONETARY_VALUE AS MONETARY_VALUE,
  DOOR AS DOOR,
  RTE_TYPE_1 AS RTE_TYPE_1,
  RTE_TYPE_2 AS RTE_TYPE_2,
  SCHEDULED_PICKUP_DTTM AS SCHEDULED_PICKUP_DTTM,
  MANIFEST_ID AS MANIFEST_ID,
  DAYS_TO_DELIVER AS DAYS_TO_DELIVER,
  SED_GENERATED_FLAG AS SED_GENERATED_FLAG,
  SERV_AREA_CODE AS SERV_AREA_CODE,
  TENDER_RESP_DEADLINE_DTTM AS TENDER_RESP_DEADLINE_DTTM,
  PRINT_CONS_BOL AS PRINT_CONS_BOL,
  LEFT_WT AS LEFT_WT,
  RIGHT_WT AS RIGHT_WT,
  LANE_NAME AS LANE_NAME,
  DECLARED_VALUE AS DECLARED_VALUE,
  DV_CURRENCY_CODE AS DV_CURRENCY_CODE,
  COD_AMOUNT AS COD_AMOUNT,
  CUSTOMER_ID AS CUSTOMER_ID,
  CUST_FRGT_CHARGE AS CUST_FRGT_CHARGE,
  COD_CURRENCY_CODE AS COD_CURRENCY_CODE,
  PLANNED_WEIGHT AS PLANNED_WEIGHT,
  WEIGHT_UOM_ID_BASE AS WEIGHT_UOM_ID_BASE,
  PLANNED_VOLUME AS PLANNED_VOLUME,
  VOLUME_UOM_ID_BASE AS VOLUME_UOM_ID_BASE,
  SIZE1_VALUE AS SIZE1_VALUE,
  SIZE1_UOM_ID AS SIZE1_UOM_ID,
  SIZE2_VALUE AS SIZE2_VALUE,
  SIZE2_UOM_ID AS SIZE2_UOM_ID,
  ASSIGNED_SHIP_VIA AS ASSIGNED_SHIP_VIA,
  INSURANCE_STATUS AS INSURANCE_STATUS,
  RTE_SWC_NBR AS RTE_SWC_NBR,
  RTE_TO AS RTE_TO,
  BILL_OF_LADING_NUMBER AS BILL_OF_LADING_NUMBER,
  TRAILER_NUMBER AS TRAILER_NUMBER,
  TRANS_PLAN_OWNER AS TRANS_PLAN_OWNER,
  TOTAL_REVENUE_CURRENCY_CODE AS TOTAL_REVENUE_CURRENCY_CODE,
  TOTAL_REVENUE AS TOTAL_REVENUE,
  FRT_REV_SPOT_CHARGE_CURR_CODE AS FRT_REV_SPOT_CHARGE_CURR_CODE,
  NORMALIZED_TOTAL_REVENUE AS NORMALIZED_TOTAL_REVENUE,
  FRT_REV_SPOT_CHARGE AS FRT_REV_SPOT_CHARGE,
  DSG_VOYAGE_FLIGHT AS DSG_VOYAGE_FLIGHT,
  FEASIBLE_VOYAGE_FLIGHT AS FEASIBLE_VOYAGE_FLIGHT,
  CURRENCY_DTTM AS CURRENCY_DTTM,
  FRT_REV_LINEHAUL_CHARGE AS FRT_REV_LINEHAUL_CHARGE,
  FRT_REV_STOP_CHARGE AS FRT_REV_STOP_CHARGE,
  FRT_REV_ACCESSORIAL_CHARGE AS FRT_REV_ACCESSORIAL_CHARGE,
  MARGIN AS MARGIN,
  TOTAL_COST AS TOTAL_COST,
  INCOTERM_ID AS INCOTERM_ID,
  ORDER_QTY AS ORDER_QTY,
  QTY_UOM_ID AS QTY_UOM_ID,
  REF_SHIPMENT_NBR AS REF_SHIPMENT_NBR,
  STAGING_LOCN_ID AS STAGING_LOCN_ID,
  LOADING_SEQ_ORD AS LOADING_SEQ_ORD,
  HAZMAT_CERT_CONTACT AS HAZMAT_CERT_CONTACT,
  HAZMAT_CERT_DECLARATION AS HAZMAT_CERT_DECLARATION
FROM
  SHIPMENT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SHIPMENT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SHIPMENT_1


query_1 = f"""SELECT
  SHIPMENT_ID AS SHIPMENT_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  TC_SHIPMENT_ID AS TC_SHIPMENT_ID,
  SHIPMENT_STATUS AS SHIPMENT_STATUS,
  UPDATE_SENT AS UPDATE_SENT,
  CREATION_TYPE AS CREATION_TYPE,
  IS_CANCELLED AS IS_CANCELLED,
  AVAILABLE_DTTM AS AVAILABLE_DTTM,
  O_FACILITY_NUMBER AS O_FACILITY_NUMBER,
  O_FACILITY_ID AS O_FACILITY_ID,
  O_ADDRESS AS O_ADDRESS,
  O_CITY AS O_CITY,
  O_STATE_PROV AS O_STATE_PROV,
  O_POSTAL_CODE AS O_POSTAL_CODE,
  O_COUNTY AS O_COUNTY,
  O_COUNTRY_CODE AS O_COUNTRY_CODE,
  D_ADDRESS AS D_ADDRESS,
  D_CITY AS D_CITY,
  D_STATE_PROV AS D_STATE_PROV,
  D_POSTAL_CODE AS D_POSTAL_CODE,
  D_COUNTY AS D_COUNTY,
  D_COUNTRY_CODE AS D_COUNTRY_CODE,
  IS_PERISHABLE AS IS_PERISHABLE,
  BILLING_METHOD AS BILLING_METHOD,
  ASSIGNED_CARRIER_CODE AS ASSIGNED_CARRIER_CODE,
  TRANS_RESP_CODE AS TRANS_RESP_CODE,
  CURRENCY_CODE AS CURRENCY_CODE,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  NUM_STOPS AS NUM_STOPS,
  NUM_DOCKS AS NUM_DOCKS,
  HAS_NOTES AS HAS_NOTES,
  HAS_ALERTS AS HAS_ALERTS,
  HAS_IMPORT_ERROR AS HAS_IMPORT_ERROR,
  HAS_SOFT_CHECK_ERROR AS HAS_SOFT_CHECK_ERROR,
  HAS_TRACKING_MSG AS HAS_TRACKING_MSG,
  TRACKING_MSG_PROBLEM AS TRACKING_MSG_PROBLEM,
  PICKUP_TZ AS PICKUP_TZ,
  DELIVERY_TZ AS DELIVERY_TZ,
  O_STOP_LOCATION_NAME AS O_STOP_LOCATION_NAME,
  D_STOP_LOCATION_NAME AS D_STOP_LOCATION_NAME,
  USE_BROKER_AS_CARRIER AS USE_BROKER_AS_CARRIER,
  STATUS_CHANGE_DTTM AS STATUS_CHANGE_DTTM,
  REPORTED_COST AS REPORTED_COST,
  IS_AUTO_DELIVERED AS IS_AUTO_DELIVERED,
  SHIPMENT_TYPE AS SHIPMENT_TYPE,
  IS_SHIPMENT_RECONCILED AS IS_SHIPMENT_RECONCILED,
  DELIVERY_REQ AS DELIVERY_REQ,
  WAYPOINT_TOTAL_COST AS WAYPOINT_TOTAL_COST,
  WAYPOINT_HANDLING_COST AS WAYPOINT_HANDLING_COST,
  IS_MANUAL_ASSIGN AS IS_MANUAL_ASSIGN,
  PRIORITY_TYPE AS PRIORITY_TYPE,
  MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  SHIPMENT_LEG_TYPE AS SHIPMENT_LEG_TYPE,
  SHIPMENT_CLOSED_INDICATOR AS SHIPMENT_CLOSED_INDICATOR,
  NUM_CHARGE_LAYOVERS AS NUM_CHARGE_LAYOVERS,
  PICK_START_DTTM AS PICK_START_DTTM,
  TOTAL_TIME AS TOTAL_TIME,
  RETAIN_CONSOLIDATOR_TIMES AS RETAIN_CONSOLIDATOR_TIMES,
  EVENT_IND_TYPEID AS EVENT_IND_TYPEID,
  DELAY_TYPE AS DELAY_TYPE,
  ASSIGNED_CARRIER_ID AS ASSIGNED_CARRIER_ID,
  HAS_EM_NOTIFY_FLAG AS HAS_EM_NOTIFY_FLAG,
  REGION_ID AS REGION_ID,
  INBOUND_REGION_ID AS INBOUND_REGION_ID,
  OUTBOUND_REGION_ID AS OUTBOUND_REGION_ID,
  SHIPMENT_WIN_ADJ_FLAG AS SHIPMENT_WIN_ADJ_FLAG,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  TOTAL_COST AS TOTAL_COST,
  ORDER_QTY AS ORDER_QTY,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SHIPMENT_0
WHERE
  TRUNC(Shortcut_to_SHIPMENT_0.LAST_UPDATED_DTTM) >= TRUNC(now()) -90"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SHIPMENT_1")

# COMMAND ----------
# DBTITLE 1, EXP_TRANS_2


query_2 = f"""SELECT
  SHIPMENT_ID AS SHIPMENT_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  TC_SHIPMENT_ID AS TC_SHIPMENT_ID,
  SHIPMENT_STATUS AS SHIPMENT_STATUS,
  UPDATE_SENT AS UPDATE_SENT,
  CREATION_TYPE AS CREATION_TYPE,
  IS_CANCELLED AS IS_CANCELLED,
  AVAILABLE_DTTM AS AVAILABLE_DTTM,
  O_FACILITY_NUMBER AS O_FACILITY_NUMBER,
  O_FACILITY_ID AS O_FACILITY_ID,
  O_ADDRESS AS O_ADDRESS,
  O_CITY AS O_CITY,
  O_STATE_PROV AS O_STATE_PROV,
  O_POSTAL_CODE AS O_POSTAL_CODE,
  O_COUNTRY_CODE AS O_COUNTRY_CODE,
  D_ADDRESS AS D_ADDRESS,
  D_CITY AS D_CITY,
  D_STATE_PROV AS D_STATE_PROV,
  D_POSTAL_CODE AS D_POSTAL_CODE,
  D_COUNTRY_CODE AS D_COUNTRY_CODE,
  IS_PERISHABLE AS IS_PERISHABLE,
  BILLING_METHOD AS BILLING_METHOD,
  ASSIGNED_CARRIER_CODE AS ASSIGNED_CARRIER_CODE,
  TRANS_RESP_CODE AS TRANS_RESP_CODE,
  CURRENCY_CODE AS CURRENCY_CODE,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  NUM_STOPS AS NUM_STOPS,
  NUM_DOCKS AS NUM_DOCKS,
  HAS_NOTES AS HAS_NOTES,
  HAS_ALERTS AS HAS_ALERTS,
  HAS_IMPORT_ERROR AS HAS_IMPORT_ERROR,
  HAS_SOFT_CHECK_ERROR AS HAS_SOFT_CHECK_ERROR,
  HAS_TRACKING_MSG AS HAS_TRACKING_MSG,
  TRACKING_MSG_PROBLEM AS TRACKING_MSG_PROBLEM,
  PICKUP_TZ AS PICKUP_TZ,
  DELIVERY_TZ AS DELIVERY_TZ,
  O_STOP_LOCATION_NAME AS O_STOP_LOCATION_NAME,
  D_STOP_LOCATION_NAME AS D_STOP_LOCATION_NAME,
  USE_BROKER_AS_CARRIER AS USE_BROKER_AS_CARRIER,
  STATUS_CHANGE_DTTM AS STATUS_CHANGE_DTTM,
  REPORTED_COST AS REPORTED_COST,
  IS_AUTO_DELIVERED AS IS_AUTO_DELIVERED,
  SHIPMENT_TYPE AS SHIPMENT_TYPE,
  IS_SHIPMENT_RECONCILED AS IS_SHIPMENT_RECONCILED,
  DELIVERY_REQ AS DELIVERY_REQ,
  WAYPOINT_TOTAL_COST AS WAYPOINT_TOTAL_COST,
  WAYPOINT_HANDLING_COST AS WAYPOINT_HANDLING_COST,
  IS_MANUAL_ASSIGN AS IS_MANUAL_ASSIGN,
  PRIORITY_TYPE AS PRIORITY_TYPE,
  MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  SHIPMENT_LEG_TYPE AS SHIPMENT_LEG_TYPE,
  SHIPMENT_CLOSED_INDICATOR AS SHIPMENT_CLOSED_INDICATOR,
  NUM_CHARGE_LAYOVERS AS NUM_CHARGE_LAYOVERS,
  PICK_START_DTTM AS PICK_START_DTTM,
  TOTAL_TIME AS TOTAL_TIME,
  RETAIN_CONSOLIDATOR_TIMES AS RETAIN_CONSOLIDATOR_TIMES,
  EVENT_IND_TYPEID AS EVENT_IND_TYPEID,
  DELAY_TYPE AS DELAY_TYPE,
  ASSIGNED_CARRIER_ID AS ASSIGNED_CARRIER_ID,
  HAS_EM_NOTIFY_FLAG AS HAS_EM_NOTIFY_FLAG,
  REGION_ID AS REGION_ID,
  INBOUND_REGION_ID AS INBOUND_REGION_ID,
  OUTBOUND_REGION_ID AS OUTBOUND_REGION_ID,
  SHIPMENT_WIN_ADJ_FLAG AS SHIPMENT_WIN_ADJ_FLAG,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  TOTAL_COST AS TOTAL_COST,
  ORDER_QTY AS ORDER_QTY,
  now() AS LOAD_TSTMP_exp,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SHIPMENT_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_TRANS_2")

# COMMAND ----------
# DBTITLE 1, OMS_SHIPMENT_PRE


spark.sql("""INSERT INTO
  OMS_SHIPMENT_PRE
SELECT
  SHIPMENT_ID AS SHIPMENT_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  TC_SHIPMENT_ID AS TC_SHIPMENT_ID,
  SHIPMENT_STATUS AS SHIPMENT_STATUS,
  UPDATE_SENT AS UPDATE_SENT,
  CREATION_TYPE AS CREATION_TYPE,
  IS_CANCELLED AS IS_CANCELLED,
  AVAILABLE_DTTM AS AVAILABLE_DTTM,
  O_FACILITY_NUMBER AS O_FACILITY_NUMBER,
  O_FACILITY_ID AS O_FACILITY_ID,
  O_ADDRESS AS O_ADDRESS,
  O_CITY AS O_CITY,
  O_STATE_PROV AS O_STATE_PROV,
  O_POSTAL_CODE AS O_POSTAL_CODE,
  O_COUNTRY_CODE AS O_COUNTRY_CODE,
  D_ADDRESS AS D_ADDRESS,
  D_CITY AS D_CITY,
  D_STATE_PROV AS D_STATE_PROV,
  D_POSTAL_CODE AS D_POSTAL_CODE,
  D_COUNTRY_CODE AS D_COUNTRY_CODE,
  IS_PERISHABLE AS IS_PERISHABLE,
  BILLING_METHOD AS BILLING_METHOD,
  ASSIGNED_CARRIER_CODE AS ASSIGNED_CARRIER_CODE,
  TRANS_RESP_CODE AS TRANS_RESP_CODE,
  CURRENCY_CODE AS CURRENCY_CODE,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  NUM_STOPS AS NUM_STOPS,
  NUM_DOCKS AS NUM_DOCKS,
  HAS_NOTES AS HAS_NOTES,
  HAS_ALERTS AS HAS_ALERTS,
  HAS_IMPORT_ERROR AS HAS_IMPORT_ERROR,
  HAS_SOFT_CHECK_ERROR AS HAS_SOFT_CHECK_ERROR,
  HAS_TRACKING_MSG AS HAS_TRACKING_MSG,
  TRACKING_MSG_PROBLEM AS TRACKING_MSG_PROBLEM,
  PICKUP_TZ AS PICKUP_TZ,
  DELIVERY_TZ AS DELIVERY_TZ,
  O_STOP_LOCATION_NAME AS O_STOP_LOCATION_NAME,
  D_STOP_LOCATION_NAME AS D_STOP_LOCATION_NAME,
  USE_BROKER_AS_CARRIER AS USE_BROKER_AS_CARRIER,
  STATUS_CHANGE_DTTM AS STATUS_CHANGE_DTTM,
  REPORTED_COST AS REPORTED_COST,
  IS_AUTO_DELIVERED AS IS_AUTO_DELIVERED,
  SHIPMENT_TYPE AS SHIPMENT_TYPE,
  IS_SHIPMENT_RECONCILED AS IS_SHIPMENT_RECONCILED,
  DELIVERY_REQ AS DELIVERY_REQ,
  WAYPOINT_TOTAL_COST AS WAYPOINT_TOTAL_COST,
  WAYPOINT_HANDLING_COST AS WAYPOINT_HANDLING_COST,
  IS_MANUAL_ASSIGN AS IS_MANUAL_ASSIGN,
  PRIORITY_TYPE AS PRIORITY_TYPE,
  MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  SHIPMENT_LEG_TYPE AS SHIPMENT_LEG_TYPE,
  SHIPMENT_CLOSED_INDICATOR AS SHIPMENT_CLOSED_INDICATOR,
  NUM_CHARGE_LAYOVERS AS NUM_CHARGE_LAYOVERS,
  PICK_START_DTTM AS PICK_START_DTTM,
  TOTAL_TIME AS TOTAL_TIME,
  RETAIN_CONSOLIDATOR_TIMES AS RETAIN_CONSOLIDATOR_TIMES,
  EVENT_IND_TYPEID AS EVENT_IND_TYPEID,
  DELAY_TYPE AS DELAY_TYPE,
  ASSIGNED_CARRIER_ID AS ASSIGNED_CARRIER_ID,
  HAS_EM_NOTIFY_FLAG AS HAS_EM_NOTIFY_FLAG,
  REGION_ID AS REGION_ID,
  INBOUND_REGION_ID AS INBOUND_REGION_ID,
  OUTBOUND_REGION_ID AS OUTBOUND_REGION_ID,
  SHIPMENT_WIN_ADJ_FLAG AS SHIPMENT_WIN_ADJ_FLAG,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  TOTAL_COST AS TOTAL_COST,
  ORDER_QTY AS ORDER_QTY,
  LOAD_TSTMP_exp AS LOAD_TSTMP
FROM
  EXP_TRANS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Shipment_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Shipment_Pre", mainWorkflowId, parentName)
