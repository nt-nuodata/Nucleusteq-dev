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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Purchase_Orders_Line_Item_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Purchase_Orders_Line_Item_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0


query_0 = f"""SELECT
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID
FROM
  OMS_PURCH_ORDER_LOAD_CTRL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1


query_1 = f"""SELECT
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  BUSINESS_PARTNER_ID AS BUSINESS_PARTNER_ID,
  EXT_SYS_LINE_ITEM_ID AS EXT_SYS_LINE_ITEM_ID,
  DESCRIPTION AS DESCRIPTION,
  O_FACILITY_ID AS O_FACILITY_ID,
  O_FACILITY_ALIAS_ID AS O_FACILITY_ALIAS_ID,
  D_FACILITY_ID AS D_FACILITY_ID,
  D_FACILITY_ALIAS_ID AS D_FACILITY_ALIAS_ID,
  D_NAME AS D_NAME,
  D_LAST_NAME AS D_LAST_NAME,
  D_ADDRESS_1 AS D_ADDRESS_1,
  D_ADDRESS_2 AS D_ADDRESS_2,
  D_ADDRESS_3 AS D_ADDRESS_3,
  D_CITY AS D_CITY,
  D_STATE_PROV AS D_STATE_PROV,
  D_POSTAL_CODE AS D_POSTAL_CODE,
  D_COUNTY AS D_COUNTY,
  D_COUNTRY_CODE AS D_COUNTRY_CODE,
  D_PHONE_NUMBER AS D_PHONE_NUMBER,
  D_FAX_NUMBER AS D_FAX_NUMBER,
  D_EMAIL AS D_EMAIL,
  BILL_FACILITY_ALIAS_ID AS BILL_FACILITY_ALIAS_ID,
  BILL_FACILITY_ID AS BILL_FACILITY_ID,
  BILL_TO_ADDRESS_1 AS BILL_TO_ADDRESS_1,
  BILL_TO_ADDRESS_2 AS BILL_TO_ADDRESS_2,
  BILL_TO_ADDRESS_3 AS BILL_TO_ADDRESS_3,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILL_TO_STATE_PROV AS BILL_TO_STATE_PROV,
  BILL_TO_COUNTY AS BILL_TO_COUNTY,
  BILL_TO_POSTAL_CODE AS BILL_TO_POSTAL_CODE,
  BILL_TO_COUNTRY_CODE AS BILL_TO_COUNTRY_CODE,
  BILL_TO_PHONE_NUMBER AS BILL_TO_PHONE_NUMBER,
  BILL_TO_FAX_NUMBER AS BILL_TO_FAX_NUMBER,
  BILL_TO_EMAIL AS BILL_TO_EMAIL,
  STORE_FACILITY_ALIAS_ID AS STORE_FACILITY_ALIAS_ID,
  STORE_FACILITY_ID AS STORE_FACILITY_ID,
  SKU_ID AS SKU_ID,
  SKU AS SKU,
  SKU_GTIN AS SKU_GTIN,
  SKU_SUB_CODE_ID AS SKU_SUB_CODE_ID,
  SKU_SUB_CODE_VALUE AS SKU_SUB_CODE_VALUE,
  SKU_ATTR_1 AS SKU_ATTR_1,
  SKU_ATTR_2 AS SKU_ATTR_2,
  SKU_ATTR_3 AS SKU_ATTR_3,
  SKU_ATTR_4 AS SKU_ATTR_4,
  SKU_ATTR_5 AS SKU_ATTR_5,
  ORIG_BUDG_COST AS ORIG_BUDG_COST,
  MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  TOTAL_MONETARY_VALUE AS TOTAL_MONETARY_VALUE,
  UNIT_MONETARY_VALUE AS UNIT_MONETARY_VALUE,
  UNIT_TAX_AMOUNT AS UNIT_TAX_AMOUNT,
  COMMODITY_CODE_ID AS COMMODITY_CODE_ID,
  UN_NUMBER_ID AS UN_NUMBER_ID,
  EPC_TRACKING_RFID_VALUE AS EPC_TRACKING_RFID_VALUE,
  PROD_SCHED_REF_NUMBER AS PROD_SCHED_REF_NUMBER,
  PICKUP_REFERENCE_NUMBER AS PICKUP_REFERENCE_NUMBER,
  DELIVERY_REFERENCE_NUMBER AS DELIVERY_REFERENCE_NUMBER,
  PROTECTION_LEVEL_ID AS PROTECTION_LEVEL_ID,
  PACKAGE_TYPE_ID AS PACKAGE_TYPE_ID,
  PRODUCT_CLASS_ID AS PRODUCT_CLASS_ID,
  DSG_SERVICE_LEVEL_ID AS DSG_SERVICE_LEVEL_ID,
  TOTAL_SIZE_ON_ORDERS AS TOTAL_SIZE_ON_ORDERS,
  STD_INBD_LPN_QTY AS STD_INBD_LPN_QTY,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  RELEASED_QTY AS RELEASED_QTY,
  CANCELLED_QTY AS CANCELLED_QTY,
  PPACK_QTY AS PPACK_QTY,
  STD_PACK_QTY AS STD_PACK_QTY,
  STD_LPN_QTY AS STD_LPN_QTY,
  STD_SUB_PACK_QTY AS STD_SUB_PACK_QTY,
  STD_BUNDL_QTY AS STD_BUNDL_QTY,
  STD_PLT_QTY AS STD_PLT_QTY,
  DELIVERY_TZ AS DELIVERY_TZ,
  PICKUP_TZ AS PICKUP_TZ,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  REQ_DLVR_DTTM AS REQ_DLVR_DTTM,
  MUST_DLVR_DTTM AS MUST_DLVR_DTTM,
  COMM_DLVR_DTTM AS COMM_DLVR_DTTM,
  COMM_SHIP_DTTM AS COMM_SHIP_DTTM,
  PROMISED_DLVR_DTTM AS PROMISED_DLVR_DTTM,
  SHIP_BY_DATE AS SHIP_BY_DATE,
  IS_READY_TO_SHIP AS IS_READY_TO_SHIP,
  IS_HAZMAT AS IS_HAZMAT,
  IS_CANCELLED AS IS_CANCELLED,
  IS_CLOSED AS IS_CLOSED,
  IS_BONDED AS IS_BONDED,
  IS_DELETED AS IS_DELETED,
  HAS_ERRORS AS HAS_ERRORS,
  IS_FLOWTHROU AS IS_FLOWTHROU,
  IS_NEVER_OUT AS IS_NEVER_OUT,
  IS_QUANTITY_LOCKED AS IS_QUANTITY_LOCKED,
  IS_VARIABLE_WEIGHT AS IS_VARIABLE_WEIGHT,
  HAS_ROUTING_REQUEST AS HAS_ROUTING_REQUEST,
  ALLOW_SUBSTITUTION AS ALLOW_SUBSTITUTION,
  EXPIRE_DATE_REQD AS EXPIRE_DATE_REQD,
  APPLY_PROMOTIONAL AS APPLY_PROMOTIONAL,
  PRE_PACK_FLAG AS PRE_PACK_FLAG,
  PROCESS_FLAG AS PROCESS_FLAG,
  ON_HOLD AS ON_HOLD,
  IS_ASSOCIATED_TO_OUTBOUND AS IS_ASSOCIATED_TO_OUTBOUND,
  MV_SIZE_UOM_ID AS MV_SIZE_UOM_ID,
  QTY_UOM_ID_BASE AS QTY_UOM_ID_BASE,
  SHIPPED_QTY AS SHIPPED_QTY,
  RECEIVED_QTY AS RECEIVED_QTY,
  INVN_TYPE AS INVN_TYPE,
  PROD_STAT AS PROD_STAT,
  BATCH_NBR AS BATCH_NBR,
  CNTRY_OF_ORGN AS CNTRY_OF_ORGN,
  PPACK_GRP_CODE AS PPACK_GRP_CODE,
  ASSORT_NBR AS ASSORT_NBR,
  MERCHANDIZING_DEPARTMENT_ID AS MERCHANDIZING_DEPARTMENT_ID,
  MERCH_TYPE AS MERCH_TYPE,
  MERCH_GRP AS MERCH_GRP,
  STORE_DEPT AS STORE_DEPT,
  SHELF_DAYS AS SHELF_DAYS,
  FRT_CLASS AS FRT_CLASS,
  GROUP_ID AS GROUP_ID,
  SUB_GROUP_ID AS SUB_GROUP_ID,
  ALLOC_TMPL_ID AS ALLOC_TMPL_ID,
  RELEASE_TMPL_ID AS RELEASE_TMPL_ID,
  WORKFLOW_ID AS WORKFLOW_ID,
  VARIANT_VALUE AS VARIANT_VALUE,
  PROCESS_TYPE AS PROCESS_TYPE,
  PRIORITY AS PRIORITY,
  STD_INBD_LPN_VOL AS STD_INBD_LPN_VOL,
  STD_INBD_LPN_WT AS STD_INBD_LPN_WT,
  OUTBD_LPN_BRK_ATTRIB AS OUTBD_LPN_BRK_ATTRIB,
  DOM_ORDER_LINE_STATUS AS DOM_ORDER_LINE_STATUS,
  EFFECTIVE_RANK AS EFFECTIVE_RANK,
  ORIG_ORD_LINE_NBR AS ORIG_ORD_LINE_NBR,
  SO_LN_FULFILL_OPTION AS SO_LN_FULFILL_OPTION,
  SUPPLY_ASN_DTL_ID AS SUPPLY_ASN_DTL_ID,
  SUPPLY_ASN_ID AS SUPPLY_ASN_ID,
  SUPPLY_PO_ID AS SUPPLY_PO_ID,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  ACTUAL_COST AS ACTUAL_COST,
  ACTUAL_CURRENCY_CODE AS ACTUAL_CURRENCY_CODE,
  BUDGETED_COST AS BUDGETED_COST,
  BUDGETED_COST_CURRENCY_CODE AS BUDGETED_COST_CURRENCY_CODE,
  DSG_MOT_ID AS DSG_MOT_ID,
  EXPIRE_DTTM AS EXPIRE_DTTM,
  IS_LOCKED AS IS_LOCKED,
  INBD_LPN_ID AS INBD_LPN_ID,
  INBD_LPNS_RCVD AS INBD_LPNS_RCVD,
  NET_NEEDS AS NET_NEEDS,
  OUTBD_LPN_EPC_TYPE AS OUTBD_LPN_EPC_TYPE,
  OUTBD_LPN_SIZE AS OUTBD_LPN_SIZE,
  OUTBD_LPN_TYPE AS OUTBD_LPN_TYPE,
  DSG_CARRIER_ID AS DSG_CARRIER_ID,
  DSG_CARRIER_CODE AS DSG_CARRIER_CODE,
  LPN_SIZE AS LPN_SIZE,
  ORDER_CONSOL_ATTR AS ORDER_CONSOL_ATTR,
  PROC_IMMD_NEEDS AS PROC_IMMD_NEEDS,
  MFG_PLANT AS MFG_PLANT,
  MFG_DATE AS MFG_DATE,
  PURCHASE_ORDERS_LINE_STATUS AS PURCHASE_ORDERS_LINE_STATUS,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  STACK_LENGTH_VALUE AS STACK_LENGTH_VALUE,
  STACK_LENGTH_STANDARD_UOM AS STACK_LENGTH_STANDARD_UOM,
  STACK_WIDTH_VALUE AS STACK_WIDTH_VALUE,
  STACK_WIDTH_STANDARD_UOM AS STACK_WIDTH_STANDARD_UOM,
  STACK_HEIGHT_VALUE AS STACK_HEIGHT_VALUE,
  STACK_HEIGHT_STANDARD_UOM AS STACK_HEIGHT_STANDARD_UOM,
  STACK_DIAMETER_VALUE AS STACK_DIAMETER_VALUE,
  STACK_DIAMETER_STANDARD_UOM AS STACK_DIAMETER_STANDARD_UOM,
  COMMODITY_CLASS AS COMMODITY_CLASS,
  TC_PO_LINE_ID AS TC_PO_LINE_ID,
  DOM_SUB_SKU_ID AS DOM_SUB_SKU_ID,
  SUPPLY_CONS_PO_ID AS SUPPLY_CONS_PO_ID,
  SUPPLY_PO_LINE_ID AS SUPPLY_PO_LINE_ID,
  PARENT_PO_LINE_ITEM_ID AS PARENT_PO_LINE_ITEM_ID,
  PARENT_TC_PO_LINE_ID AS PARENT_TC_PO_LINE_ID,
  ROOT_LINE_ITEM_ID AS ROOT_LINE_ITEM_ID,
  ORDER_QTY AS ORDER_QTY,
  WF_NODE_ID AS WF_NODE_ID,
  WF_PROCESS_STATE AS WF_PROCESS_STATE,
  ALLOC_FINALIZER AS ALLOC_FINALIZER,
  QTY_UOM_ID AS QTY_UOM_ID,
  INVENTORY_SEGMENT_ID AS INVENTORY_SEGMENT_ID,
  CUST_SKU AS CUST_SKU,
  PLT_TYPE AS PLT_TYPE,
  PACKAGE_TYPE_INSTANCE AS PACKAGE_TYPE_INSTANCE,
  ACCEPTED_DTTM AS ACCEPTED_DTTM,
  IS_DO_CREATED AS IS_DO_CREATED,
  RECEIVED_WEIGHT AS RECEIVED_WEIGHT,
  PACKED_SIZE_VALUE AS PACKED_SIZE_VALUE,
  PLANNED_WEIGHT AS PLANNED_WEIGHT,
  SHIPPED_WEIGHT AS SHIPPED_WEIGHT,
  WEIGHT_UOM_ID_BASE AS WEIGHT_UOM_ID_BASE,
  WEIGHT_UOM_ID AS WEIGHT_UOM_ID,
  PLANNED_VOLUME AS PLANNED_VOLUME,
  RECEIVED_VOLUME AS RECEIVED_VOLUME,
  SHIPPED_VOLUME AS SHIPPED_VOLUME,
  VOLUME_UOM_ID_BASE AS VOLUME_UOM_ID_BASE,
  VOLUME_UOM_ID AS VOLUME_UOM_ID,
  SIZE1_UOM_ID AS SIZE1_UOM_ID,
  SIZE1_VALUE AS SIZE1_VALUE,
  RECEIVED_SIZE1 AS RECEIVED_SIZE1,
  SHIPPED_SIZE1 AS SHIPPED_SIZE1,
  SIZE2_UOM_ID AS SIZE2_UOM_ID,
  SIZE2_VALUE AS SIZE2_VALUE,
  RECEIVED_SIZE2 AS RECEIVED_SIZE2,
  SHIPPED_SIZE2 AS SHIPPED_SIZE2,
  QTY_CONV_FACTOR AS QTY_CONV_FACTOR,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RETAIL_PRICE AS RETAIL_PRICE,
  DSG_SHIP_VIA AS DSG_SHIP_VIA,
  LINE_TOTAL AS LINE_TOTAL,
  IS_GIFT AS IS_GIFT,
  ADDR_VALID AS ADDR_VALID,
  REASON_ID AS REASON_ID,
  HAS_CHILD AS HAS_CHILD,
  NEW_LINE_TYPE AS NEW_LINE_TYPE,
  REQ_CAPACITY_PER_UNIT AS REQ_CAPACITY_PER_UNIT,
  MERGE_FACILITY AS MERGE_FACILITY,
  MERGE_FACILITY_ALIAS_ID AS MERGE_FACILITY_ALIAS_ID,
  ORDER_FULFILLMENT_OPTION AS ORDER_FULFILLMENT_OPTION,
  REF_FIELD_1 AS REF_FIELD_1,
  REF_FIELD_2 AS REF_FIELD_2,
  REF_FIELD_3 AS REF_FIELD_3,
  IS_RETURNABLE AS IS_RETURNABLE,
  BACK_ORD_REASON AS BACK_ORD_REASON,
  FREIGHT_REVENUE_CURRENCY_CODE AS FREIGHT_REVENUE_CURRENCY_CODE,
  FREIGHT_REVENUE AS FREIGHT_REVENUE,
  PRICE_OVERRIDE AS PRICE_OVERRIDE,
  ORIGINAL_PRICE AS ORIGINAL_PRICE,
  EXT_CREATED_DTTM AS EXT_CREATED_DTTM,
  REASON_CODES_GROUP_ID AS REASON_CODES_GROUP_ID,
  OVER_SHIP_PCT AS OVER_SHIP_PCT,
  IS_EXCHANGEABLE AS IS_EXCHANGEABLE,
  ORIGINAL_PO_LINE_ITEM_ID AS ORIGINAL_PO_LINE_ITEM_ID,
  RMA_LINE_STATUS AS RMA_LINE_STATUS,
  OVER_PACK_PCT AS OVER_PACK_PCT,
  ALLOW_RESIDUAL_PACK AS ALLOW_RESIDUAL_PACK,
  HAS_COMP_ITEM AS HAS_COMP_ITEM,
  INV_DISPOSITION AS INV_DISPOSITION,
  EXT_PLAN_ID AS EXT_PLAN_ID,
  TAX_INCLUDED AS TAX_INCLUDED,
  RETURN_ACTION_TYPE AS RETURN_ACTION_TYPE,
  REF_FIELD4 AS REF_FIELD4,
  REF_FIELD5 AS REF_FIELD5,
  REF_FIELD6 AS REF_FIELD6,
  REF_FIELD7 AS REF_FIELD7,
  REF_FIELD8 AS REF_FIELD8,
  REF_FIELD9 AS REF_FIELD9,
  REF_FIELD10 AS REF_FIELD10,
  REF_NUM1 AS REF_NUM1,
  REF_NUM2 AS REF_NUM2,
  REF_NUM3 AS REF_NUM3,
  REF_NUM4 AS REF_NUM4,
  REF_NUM5 AS REF_NUM5,
  REF_BOOLEAN1 AS REF_BOOLEAN1,
  REF_BOOLEAN2 AS REF_BOOLEAN2,
  REF_SYSCODE1 AS REF_SYSCODE1,
  REF_SYSCODE2 AS REF_SYSCODE2,
  REF_SYSCODE3 AS REF_SYSCODE3
FROM
  PURCHASE_ORDERS_LINE_ITEM"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_2


query_2 = f"""SELECT
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.O_FACILITY_ID AS O_FACILITY_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.O_FACILITY_ALIAS_ID AS O_FACILITY_ALIAS_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_NAME AS D_NAME,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_LAST_NAME AS D_LAST_NAME,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_ADDRESS_1 AS D_ADDRESS_1,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_ADDRESS_2 AS D_ADDRESS_2,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_ADDRESS_3 AS D_ADDRESS_3,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_CITY AS D_CITY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_STATE_PROV AS D_STATE_PROV,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_POSTAL_CODE AS D_POSTAL_CODE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_COUNTY AS D_COUNTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_COUNTRY_CODE AS D_COUNTRY_CODE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_PHONE_NUMBER AS D_PHONE_NUMBER,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_EMAIL AS D_EMAIL,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.BILL_FACILITY_ALIAS_ID AS BILL_FACILITY_ALIAS_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.BILL_FACILITY_ID AS BILL_FACILITY_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.STORE_FACILITY_ALIAS_ID AS STORE_FACILITY_ALIAS_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.STORE_FACILITY_ID AS STORE_FACILITY_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.SKU AS SKU,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.TOTAL_MONETARY_VALUE AS TOTAL_MONETARY_VALUE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.UNIT_MONETARY_VALUE AS UNIT_MONETARY_VALUE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.UNIT_TAX_AMOUNT AS UNIT_TAX_AMOUNT,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PRODUCT_CLASS_ID AS PRODUCT_CLASS_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.ALLOCATED_QTY AS ALLOCATED_QTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.RELEASED_QTY AS RELEASED_QTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.CANCELLED_QTY AS CANCELLED_QTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PPACK_QTY AS PPACK_QTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.DELIVERY_TZ AS DELIVERY_TZ,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PICKUP_TZ AS PICKUP_TZ,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PICKUP_START_DTTM AS PICKUP_START_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PICKUP_END_DTTM AS PICKUP_END_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.REQ_DLVR_DTTM AS REQ_DLVR_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.MUST_DLVR_DTTM AS MUST_DLVR_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.SHIP_BY_DATE AS SHIP_BY_DATE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.IS_READY_TO_SHIP AS IS_READY_TO_SHIP,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.IS_CANCELLED AS IS_CANCELLED,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.IS_CLOSED AS IS_CLOSED,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.IS_DELETED AS IS_DELETED,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.SHIPPED_QTY AS SHIPPED_QTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.RECEIVED_QTY AS RECEIVED_QTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.CREATED_SOURCE AS CREATED_SOURCE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.CREATED_DTTM AS CREATED_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.DSG_CARRIER_ID AS DSG_CARRIER_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PURCHASE_ORDERS_LINE_STATUS AS PURCHASE_ORDERS_LINE_STATUS,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.TC_PO_LINE_ID AS TC_PO_LINE_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PARENT_PO_LINE_ITEM_ID AS PARENT_PO_LINE_ITEM_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.ORDER_QTY AS ORDER_QTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.ALLOC_FINALIZER AS ALLOC_FINALIZER,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.CUST_SKU AS CUST_SKU,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.IS_DO_CREATED AS IS_DO_CREATED,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.RETAIL_PRICE AS RETAIL_PRICE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.DSG_SHIP_VIA AS DSG_SHIP_VIA,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.LINE_TOTAL AS LINE_TOTAL,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.ADDR_VALID AS ADDR_VALID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.REASON_ID AS REASON_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.ORDER_FULFILLMENT_OPTION AS ORDER_FULFILLMENT_OPTION,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.IS_RETURNABLE AS IS_RETURNABLE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.BACK_ORD_REASON AS BACK_ORD_REASON,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PRICE_OVERRIDE AS PRICE_OVERRIDE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.ORIGINAL_PRICE AS ORIGINAL_PRICE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.EXT_CREATED_DTTM AS EXT_CREATED_DTTM,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.REASON_CODES_GROUP_ID AS REASON_CODES_GROUP_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.IS_EXCHANGEABLE AS IS_EXCHANGEABLE,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.ORIGINAL_PO_LINE_ITEM_ID AS ORIGINAL_PO_LINE_ITEM_ID,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID1,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_FACILITY_ID AS D_FACILITY_ID,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.D_FACILITY_ALIAS_ID AS D_FACILITY_ALIAS_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0,
  Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1
WHERE
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID = Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_1.PURCHASE_ORDERS_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_2")

# COMMAND ----------
# DBTITLE 1, EXP_TRANS_3


query_3 = f"""SELECT
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
  O_FACILITY_ID AS O_FACILITY_ID,
  O_FACILITY_ALIAS_ID AS O_FACILITY_ALIAS_ID,
  D_NAME AS D_NAME,
  D_LAST_NAME AS D_LAST_NAME,
  D_ADDRESS_1 AS D_ADDRESS_1,
  D_ADDRESS_2 AS D_ADDRESS_2,
  D_ADDRESS_3 AS D_ADDRESS_3,
  D_CITY AS D_CITY,
  D_STATE_PROV AS D_STATE_PROV,
  D_POSTAL_CODE AS D_POSTAL_CODE,
  D_COUNTY AS D_COUNTY,
  D_COUNTRY_CODE AS D_COUNTRY_CODE,
  D_PHONE_NUMBER AS D_PHONE_NUMBER,
  D_EMAIL AS D_EMAIL,
  BILL_FACILITY_ALIAS_ID AS BILL_FACILITY_ALIAS_ID,
  BILL_FACILITY_ID AS BILL_FACILITY_ID,
  STORE_FACILITY_ALIAS_ID AS STORE_FACILITY_ALIAS_ID,
  STORE_FACILITY_ID AS STORE_FACILITY_ID,
  SKU AS SKU,
  MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  TOTAL_MONETARY_VALUE AS TOTAL_MONETARY_VALUE,
  UNIT_MONETARY_VALUE AS UNIT_MONETARY_VALUE,
  UNIT_TAX_AMOUNT AS UNIT_TAX_AMOUNT,
  PRODUCT_CLASS_ID AS PRODUCT_CLASS_ID,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  RELEASED_QTY AS RELEASED_QTY,
  CANCELLED_QTY AS CANCELLED_QTY,
  PPACK_QTY AS PPACK_QTY,
  DELIVERY_TZ AS DELIVERY_TZ,
  PICKUP_TZ AS PICKUP_TZ,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  REQ_DLVR_DTTM AS REQ_DLVR_DTTM,
  MUST_DLVR_DTTM AS MUST_DLVR_DTTM,
  SHIP_BY_DATE AS SHIP_BY_DATE,
  IS_READY_TO_SHIP AS IS_READY_TO_SHIP,
  IS_CANCELLED AS IS_CANCELLED,
  IS_CLOSED AS IS_CLOSED,
  IS_DELETED AS IS_DELETED,
  SHIPPED_QTY AS SHIPPED_QTY,
  RECEIVED_QTY AS RECEIVED_QTY,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DSG_CARRIER_ID AS DSG_CARRIER_ID,
  PURCHASE_ORDERS_LINE_STATUS AS PURCHASE_ORDERS_LINE_STATUS,
  TC_PO_LINE_ID AS TC_PO_LINE_ID,
  PARENT_PO_LINE_ITEM_ID AS PARENT_PO_LINE_ITEM_ID,
  ORDER_QTY AS ORDER_QTY,
  ALLOC_FINALIZER AS ALLOC_FINALIZER,
  CUST_SKU AS CUST_SKU,
  IS_DO_CREATED AS IS_DO_CREATED,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RETAIL_PRICE AS RETAIL_PRICE,
  DSG_SHIP_VIA AS DSG_SHIP_VIA,
  LINE_TOTAL AS LINE_TOTAL,
  ADDR_VALID AS ADDR_VALID,
  REASON_ID AS REASON_ID,
  ORDER_FULFILLMENT_OPTION AS ORDER_FULFILLMENT_OPTION,
  IS_RETURNABLE AS IS_RETURNABLE,
  BACK_ORD_REASON AS BACK_ORD_REASON,
  PRICE_OVERRIDE AS PRICE_OVERRIDE,
  ORIGINAL_PRICE AS ORIGINAL_PRICE,
  EXT_CREATED_DTTM AS EXT_CREATED_DTTM,
  REASON_CODES_GROUP_ID AS REASON_CODES_GROUP_ID,
  IS_EXCHANGEABLE AS IS_EXCHANGEABLE,
  ORIGINAL_PO_LINE_ITEM_ID AS ORIGINAL_PO_LINE_ITEM_ID,
  now() AS LOAD_TSTMP_exp,
  D_FACILITY_ID AS D_FACILITY_ID,
  D_FACILITY_ALIAS_ID AS D_FACILITY_ALIAS_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PURCHASE_ORDERS_LINE_ITEM_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_TRANS_3")

# COMMAND ----------
# DBTITLE 1, OMS_PURCHASE_ORDERS_LINE_ITEM_PRE


spark.sql("""INSERT INTO
  OMS_PURCHASE_ORDERS_LINE_ITEM_PRE
SELECT
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
  O_FACILITY_ID AS O_FACILITY_ID,
  O_FACILITY_ALIAS_ID AS O_FACILITY_ALIAS_ID,
  D_FACILITY_ID AS D_FACILITY_ID,
  D_FACILITY_ALIAS_ID AS D_FACILITY_ALIAS_ID,
  D_NAME AS D_NAME,
  D_LAST_NAME AS D_LAST_NAME,
  D_ADDRESS_1 AS D_ADDRESS_1,
  D_ADDRESS_2 AS D_ADDRESS_2,
  D_ADDRESS_3 AS D_ADDRESS_3,
  D_CITY AS D_CITY,
  D_STATE_PROV AS D_STATE_PROV,
  D_POSTAL_CODE AS D_POSTAL_CODE,
  D_COUNTY AS D_COUNTY,
  D_COUNTRY_CODE AS D_COUNTRY_CODE,
  D_PHONE_NUMBER AS D_PHONE_NUMBER,
  D_EMAIL AS D_EMAIL,
  BILL_FACILITY_ALIAS_ID AS BILL_FACILITY_ALIAS_ID,
  BILL_FACILITY_ID AS BILL_FACILITY_ID,
  STORE_FACILITY_ALIAS_ID AS STORE_FACILITY_ALIAS_ID,
  STORE_FACILITY_ID AS STORE_FACILITY_ID,
  SKU AS SKU,
  MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  TOTAL_MONETARY_VALUE AS TOTAL_MONETARY_VALUE,
  UNIT_MONETARY_VALUE AS UNIT_MONETARY_VALUE,
  UNIT_TAX_AMOUNT AS UNIT_TAX_AMOUNT,
  PRODUCT_CLASS_ID AS PRODUCT_CLASS_ID,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  RELEASED_QTY AS RELEASED_QTY,
  CANCELLED_QTY AS CANCELLED_QTY,
  PPACK_QTY AS PPACK_QTY,
  DELIVERY_TZ AS DELIVERY_TZ,
  PICKUP_TZ AS PICKUP_TZ,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  REQ_DLVR_DTTM AS REQ_DLVR_DTTM,
  MUST_DLVR_DTTM AS MUST_DLVR_DTTM,
  SHIP_BY_DATE AS SHIP_BY_DATE,
  IS_READY_TO_SHIP AS IS_READY_TO_SHIP,
  IS_CANCELLED AS IS_CANCELLED,
  IS_CLOSED AS IS_CLOSED,
  IS_DELETED AS IS_DELETED,
  SHIPPED_QTY AS SHIPPED_QTY,
  RECEIVED_QTY AS RECEIVED_QTY,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DSG_CARRIER_ID AS DSG_CARRIER_ID,
  PURCHASE_ORDERS_LINE_STATUS AS PURCHASE_ORDERS_LINE_STATUS,
  TC_PO_LINE_ID AS TC_PO_LINE_ID,
  PARENT_PO_LINE_ITEM_ID AS PARENT_PO_LINE_ITEM_ID,
  ORDER_QTY AS ORDER_QTY,
  ALLOC_FINALIZER AS ALLOC_FINALIZER,
  CUST_SKU AS CUST_SKU,
  IS_DO_CREATED AS IS_DO_CREATED,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RETAIL_PRICE AS RETAIL_PRICE,
  DSG_SHIP_VIA AS DSG_SHIP_VIA,
  LINE_TOTAL AS LINE_TOTAL,
  ADDR_VALID AS ADDR_VALID,
  REASON_ID AS REASON_ID,
  ORDER_FULFILLMENT_OPTION AS ORDER_FULFILLMENT_OPTION,
  IS_RETURNABLE AS IS_RETURNABLE,
  BACK_ORD_REASON AS BACK_ORD_REASON,
  PRICE_OVERRIDE AS PRICE_OVERRIDE,
  ORIGINAL_PRICE AS ORIGINAL_PRICE,
  EXT_CREATED_DTTM AS EXT_CREATED_DTTM,
  REASON_CODES_GROUP_ID AS REASON_CODES_GROUP_ID,
  IS_EXCHANGEABLE AS IS_EXCHANGEABLE,
  ORIGINAL_PO_LINE_ITEM_ID AS ORIGINAL_PO_LINE_ITEM_ID,
  LOAD_TSTMP_exp AS LOAD_TSTMP
FROM
  EXP_TRANS_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Purchase_Orders_Line_Item_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Purchase_Orders_Line_Item_Pre", mainWorkflowId, parentName)
