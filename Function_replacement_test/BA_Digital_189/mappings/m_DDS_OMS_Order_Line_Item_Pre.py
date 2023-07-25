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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Line_Item_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Order_Line_Item_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_LOAD_CTRL_0


query_0 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID
FROM
  OMS_ORDER_LOAD_CTRL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ORDER_LOAD_CTRL_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ORDER_LINE_ITEM_1


query_1 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  PARENT_LINE_ITEM_ID AS PARENT_LINE_ITEM_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  COMMODITY_CLASS AS COMMODITY_CLASS,
  REF_FIELD1 AS REF_FIELD1,
  REF_FIELD2 AS REF_FIELD2,
  REF_FIELD3 AS REF_FIELD3,
  EXT_SYS_LINE_ITEM_ID AS EXT_SYS_LINE_ITEM_ID,
  MASTER_ORDER_ID AS MASTER_ORDER_ID,
  MO_LINE_ITEM_ID AS MO_LINE_ITEM_ID,
  IS_HAZMAT AS IS_HAZMAT,
  IS_STACKABLE AS IS_STACKABLE,
  HAS_ERRORS AS HAS_ERRORS,
  ORIG_BUDG_COST AS ORIG_BUDG_COST,
  BUDG_COST_CURRENCY_CODE AS BUDG_COST_CURRENCY_CODE,
  BUDG_COST AS BUDG_COST,
  ACTUAL_COST_CURRENCY_CODE AS ACTUAL_COST_CURRENCY_CODE,
  TOTAL_MONETARY_VALUE AS TOTAL_MONETARY_VALUE,
  UNIT_MONETARY_VALUE AS UNIT_MONETARY_VALUE,
  UNIT_TAX_AMOUNT AS UNIT_TAX_AMOUNT,
  RTS_ID AS RTS_ID,
  RTS_LINE_ITEM_ID AS RTS_LINE_ITEM_ID,
  STD_PACK_QTY AS STD_PACK_QTY,
  PRODUCT_CLASS_ID AS PRODUCT_CLASS_ID,
  PROTECTION_LEVEL_ID AS PROTECTION_LEVEL_ID,
  PACKAGE_TYPE_ID AS PACKAGE_TYPE_ID,
  COMMODITY_CODE_ID AS COMMODITY_CODE_ID,
  MV_SIZE_UOM_ID AS MV_SIZE_UOM_ID,
  QTY_UOM_ID_BASE AS QTY_UOM_ID_BASE,
  MV_CURRENCY_CODE AS MV_CURRENCY_CODE,
  SHIPPED_QTY AS SHIPPED_QTY,
  RECEIVED_QTY AS RECEIVED_QTY,
  PRIORITY AS PRIORITY,
  MERCHANDIZING_DEPARTMENT_ID AS MERCHANDIZING_DEPARTMENT_ID,
  UN_NUMBER_ID AS UN_NUMBER_ID,
  PICKUP_REFERENCE_NUMBER AS PICKUP_REFERENCE_NUMBER,
  DELIVERY_REFERENCE_NUMBER AS DELIVERY_REFERENCE_NUMBER,
  CNTRY_OF_ORGN AS CNTRY_OF_ORGN,
  PROD_STAT AS PROD_STAT,
  STACK_RANK AS STACK_RANK,
  STACK_LENGTH_VALUE AS STACK_LENGTH_VALUE,
  STACK_LENGTH_STANDARD_UOM AS STACK_LENGTH_STANDARD_UOM,
  STACK_WIDTH_VALUE AS STACK_WIDTH_VALUE,
  STACK_WIDTH_STANDARD_UOM AS STACK_WIDTH_STANDARD_UOM,
  STACK_HEIGHT_VALUE AS STACK_HEIGHT_VALUE,
  STACK_HEIGHT_STANDARD_UOM AS STACK_HEIGHT_STANDARD_UOM,
  STACK_DIAMETER_VALUE AS STACK_DIAMETER_VALUE,
  STACK_DIAMETER_STANDARD_UOM AS STACK_DIAMETER_STANDARD_UOM,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  CUBE_MULTIPLE_QTY AS CUBE_MULTIPLE_QTY,
  DO_DTL_STATUS AS DO_DTL_STATUS,
  INTERNAL_ORDER_SEQ_NBR AS INTERNAL_ORDER_SEQ_NBR,
  IS_EMERGENCY AS IS_EMERGENCY,
  LPN_SIZE AS LPN_SIZE,
  MANUFACTURING_DTTM AS MANUFACTURING_DTTM,
  ORDER_LINE_ID AS ORDER_LINE_ID,
  PACK_RATE AS PACK_RATE,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  PLANNED_SHIP_DATE AS PLANNED_SHIP_DATE,
  PPACK_QTY AS PPACK_QTY,
  STD_BUNDLE_QTY AS STD_BUNDLE_QTY,
  STD_LPN_QTY AS STD_LPN_QTY,
  STD_LPN_VOL AS STD_LPN_VOL,
  STD_LPN_WT AS STD_LPN_WT,
  UNIT_COST AS UNIT_COST,
  UNIT_PRICE_AMOUNT AS UNIT_PRICE_AMOUNT,
  UNIT_VOL AS UNIT_VOL,
  UNIT_WT AS UNIT_WT,
  USER_CANCELED_QTY AS USER_CANCELED_QTY,
  WAVE_PROC_TYPE AS WAVE_PROC_TYPE,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  EVENT_CODE AS EVENT_CODE,
  REASON_CODE AS REASON_CODE,
  EXP_INFO_CODE AS EXP_INFO_CODE,
  PARTL_FILL AS PARTL_FILL,
  LINE_TYPE AS LINE_TYPE,
  REPL_PROC_TYPE AS REPL_PROC_TYPE,
  ORDER_QTY AS ORDER_QTY,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RTL_TO_BE_DISTROED_QTY AS RTL_TO_BE_DISTROED_QTY,
  PRICE AS PRICE,
  RETAIL_PRICE AS RETAIL_PRICE,
  UNITS_PAKD AS UNITS_PAKD,
  MINOR_ORDER_NBR AS MINOR_ORDER_NBR,
  ITEM_ID AS ITEM_ID,
  TC_ORDER_LINE_ID AS TC_ORDER_LINE_ID,
  ACTUAL_SHIPPED_DTTM AS ACTUAL_SHIPPED_DTTM,
  QTY_UOM_ID AS QTY_UOM_ID,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  PURCHASE_ORDER_LINE_NUMBER AS PURCHASE_ORDER_LINE_NUMBER,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  REFERENCE_LINE_ITEM_ID AS REFERENCE_LINE_ITEM_ID,
  REFERENCE_ORDER_ID AS REFERENCE_ORDER_ID,
  REPL_WAVE_RUN AS REPL_WAVE_RUN,
  STD_PALLET_QTY AS STD_PALLET_QTY,
  STD_SUB_PACK_QTY AS STD_SUB_PACK_QTY,
  STORE_DEPT AS STORE_DEPT,
  ALLOC_TYPE AS ALLOC_TYPE,
  BATCH_NBR AS BATCH_NBR,
  BATCH_REQUIREMENT_TYPE AS BATCH_REQUIREMENT_TYPE,
  CHUTE_ASSIGN_TYPE AS CHUTE_ASSIGN_TYPE,
  CUSTOM_TAG AS CUSTOM_TAG,
  CUSTOMER_ITEM AS CUSTOMER_ITEM,
  INTERNAL_ORDER_ID AS INTERNAL_ORDER_ID,
  INVN_TYPE AS INVN_TYPE,
  ITEM_ATTR_1 AS ITEM_ATTR_1,
  ITEM_ATTR_2 AS ITEM_ATTR_2,
  ITEM_ATTR_3 AS ITEM_ATTR_3,
  ITEM_ATTR_4 AS ITEM_ATTR_4,
  ITEM_ATTR_5 AS ITEM_ATTR_5,
  LPN_BRK_ATTRIB AS LPN_BRK_ATTRIB,
  MERCH_GRP AS MERCH_GRP,
  MERCH_TYPE AS MERCH_TYPE,
  PACK_ZONE AS PACK_ZONE,
  PALLET_TYPE AS PALLET_TYPE,
  PICK_LOCN_ASSIGN_TYPE AS PICK_LOCN_ASSIGN_TYPE,
  PICK_LOCN_ID AS PICK_LOCN_ID,
  PPACK_GRP_CODE AS PPACK_GRP_CODE,
  PRICE_TKT_TYPE AS PRICE_TKT_TYPE,
  SERIAL_NUMBER_REQUIRED_FLAG AS SERIAL_NUMBER_REQUIRED_FLAG,
  SKU_GTIN AS SKU_GTIN,
  SKU_SUB_CODE_ID AS SKU_SUB_CODE_ID,
  SKU_SUB_CODE_VALUE AS SKU_SUB_CODE_VALUE,
  VAS_PROCESS_TYPE AS VAS_PROCESS_TYPE,
  SUBSTITUTED_PARENT_LINE_ID AS SUBSTITUTED_PARENT_LINE_ID,
  IS_CANCELLED AS IS_CANCELLED,
  ITEM_NAME AS ITEM_NAME,
  ALLOC_LINE_ID AS ALLOC_LINE_ID,
  ALLOCATION_SOURCE_ID AS ALLOCATION_SOURCE_ID,
  ALLOCATION_SOURCE_LINE_ID AS ALLOCATION_SOURCE_LINE_ID,
  ALLOCATION_SOURCE AS ALLOCATION_SOURCE,
  SHELF_DAYS AS SHELF_DAYS,
  FULFILLMENT_TYPE AS FULFILLMENT_TYPE,
  WAVE_NBR AS WAVE_NBR,
  SHIP_WAVE_NBR AS SHIP_WAVE_NBR,
  REPL_WAVE_NBR AS REPL_WAVE_NBR,
  SINGLE_UNIT_FLAG AS SINGLE_UNIT_FLAG,
  ACTUAL_COST AS ACTUAL_COST,
  QTY_CONV_FACTOR AS QTY_CONV_FACTOR,
  PLANNED_WEIGHT AS PLANNED_WEIGHT,
  RECEIVED_WEIGHT AS RECEIVED_WEIGHT,
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
  LPN_TYPE AS LPN_TYPE,
  ORDER_CONSOL_ATTR AS ORDER_CONSOL_ATTR,
  SKU_BREAK_ATTR AS SKU_BREAK_ATTR,
  PURCHASE_ORDER_NUMBER AS PURCHASE_ORDER_NUMBER,
  EXT_PURCHASE_ORDER AS EXT_PURCHASE_ORDER,
  CRITCL_DIM_1 AS CRITCL_DIM_1,
  CRITCL_DIM_2 AS CRITCL_DIM_2,
  CRITCL_DIM_3 AS CRITCL_DIM_3,
  ASSORT_NBR AS ASSORT_NBR,
  DESCRIPTION AS DESCRIPTION,
  ORIGINAL_ORDERED_ITEM_ID AS ORIGINAL_ORDERED_ITEM_ID,
  SUBSTITUTION_TYPE AS SUBSTITUTION_TYPE,
  BACK_ORD_REASON AS BACK_ORD_REASON,
  FREIGHT_REVENUE_CURRENCY_CODE AS FREIGHT_REVENUE_CURRENCY_CODE,
  FREIGHT_REVENUE AS FREIGHT_REVENUE,
  IS_CHASE_CREATED_LINE AS IS_CHASE_CREATED_LINE,
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
  IS_GIFT AS IS_GIFT,
  ADJUSTED_ORDER_QTY AS ADJUSTED_ORDER_QTY,
  PRE_RECEIPT_STATUS AS PRE_RECEIPT_STATUS,
  ALLOW_REVIVAL_RECEIPT_ALLOC AS ALLOW_REVIVAL_RECEIPT_ALLOC,
  SEGMENT_NAME AS SEGMENT_NAME,
  EFFECTIVE_RANK AS EFFECTIVE_RANK,
  WM_RECEIPT_ALLOCATED AS WM_RECEIPT_ALLOCATED,
  REF_BOOLEAN1 AS REF_BOOLEAN1,
  REF_BOOLEAN2 AS REF_BOOLEAN2,
  REF_SYSCODE1 AS REF_SYSCODE1,
  REF_SYSCODE2 AS REF_SYSCODE2,
  REF_SYSCODE3 AS REF_SYSCODE3
FROM
  ORDER_LINE_ITEM"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_ORDER_LINE_ITEM_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ORDER_LINE_ITEM_2


query_2 = f"""SELECT
  Shortcut_to_ORDER_LINE_ITEM_1.ORDER_ID AS ORDER_ID,
  Shortcut_to_ORDER_LINE_ITEM_1.LINE_ITEM_ID AS LINE_ITEM_ID,
  Shortcut_to_ORDER_LINE_ITEM_1.TC_COMPANY_ID AS TC_COMPANY_ID,
  Shortcut_to_ORDER_LINE_ITEM_1.MASTER_ORDER_ID AS MASTER_ORDER_ID,
  Shortcut_to_ORDER_LINE_ITEM_1.MO_LINE_ITEM_ID AS MO_LINE_ITEM_ID,
  Shortcut_to_ORDER_LINE_ITEM_1.TOTAL_MONETARY_VALUE AS TOTAL_MONETARY_VALUE,
  Shortcut_to_ORDER_LINE_ITEM_1.UNIT_MONETARY_VALUE AS UNIT_MONETARY_VALUE,
  Shortcut_to_ORDER_LINE_ITEM_1.UNIT_TAX_AMOUNT AS UNIT_TAX_AMOUNT,
  Shortcut_to_ORDER_LINE_ITEM_1.SHIPPED_QTY AS SHIPPED_QTY,
  Shortcut_to_ORDER_LINE_ITEM_1.RECEIVED_QTY AS RECEIVED_QTY,
  Shortcut_to_ORDER_LINE_ITEM_1.CREATED_SOURCE AS CREATED_SOURCE,
  Shortcut_to_ORDER_LINE_ITEM_1.CREATED_DTTM AS CREATED_DTTM,
  Shortcut_to_ORDER_LINE_ITEM_1.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  Shortcut_to_ORDER_LINE_ITEM_1.DO_DTL_STATUS AS DO_DTL_STATUS,
  Shortcut_to_ORDER_LINE_ITEM_1.ALLOCATED_QTY AS ALLOCATED_QTY,
  Shortcut_to_ORDER_LINE_ITEM_1.UNIT_COST AS UNIT_COST,
  Shortcut_to_ORDER_LINE_ITEM_1.UNIT_PRICE_AMOUNT AS UNIT_PRICE_AMOUNT,
  Shortcut_to_ORDER_LINE_ITEM_1.USER_CANCELED_QTY AS USER_CANCELED_QTY,
  Shortcut_to_ORDER_LINE_ITEM_1.DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  Shortcut_to_ORDER_LINE_ITEM_1.DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  Shortcut_to_ORDER_LINE_ITEM_1.EVENT_CODE AS EVENT_CODE,
  Shortcut_to_ORDER_LINE_ITEM_1.REASON_CODE AS REASON_CODE,
  Shortcut_to_ORDER_LINE_ITEM_1.EXP_INFO_CODE AS EXP_INFO_CODE,
  Shortcut_to_ORDER_LINE_ITEM_1.PARTL_FILL AS PARTL_FILL,
  Shortcut_to_ORDER_LINE_ITEM_1.ORDER_QTY AS ORDER_QTY,
  Shortcut_to_ORDER_LINE_ITEM_1.ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  Shortcut_to_ORDER_LINE_ITEM_1.RETAIL_PRICE AS RETAIL_PRICE,
  Shortcut_to_ORDER_LINE_ITEM_1.ITEM_ID AS ITEM_ID,
  Shortcut_to_ORDER_LINE_ITEM_1.TC_ORDER_LINE_ID AS TC_ORDER_LINE_ID,
  Shortcut_to_ORDER_LINE_ITEM_1.ACTUAL_SHIPPED_DTTM AS ACTUAL_SHIPPED_DTTM,
  Shortcut_to_ORDER_LINE_ITEM_1.PICKUP_END_DTTM AS PICKUP_END_DTTM,
  Shortcut_to_ORDER_LINE_ITEM_1.PURCHASE_ORDER_LINE_NUMBER AS PURCHASE_ORDER_LINE_NUMBER,
  Shortcut_to_ORDER_LINE_ITEM_1.PICKUP_START_DTTM AS PICKUP_START_DTTM,
  Shortcut_to_ORDER_LINE_ITEM_1.IS_CANCELLED AS IS_CANCELLED,
  Shortcut_to_ORDER_LINE_ITEM_1.ITEM_NAME AS ITEM_NAME,
  Shortcut_to_ORDER_LINE_ITEM_1.PURCHASE_ORDER_NUMBER AS PURCHASE_ORDER_NUMBER,
  Shortcut_to_ORDER_LINE_ITEM_1.FREIGHT_REVENUE_CURRENCY_CODE AS FREIGHT_REVENUE_CURRENCY_CODE,
  Shortcut_to_ORDER_LINE_ITEM_1.FREIGHT_REVENUE AS FREIGHT_REVENUE,
  Shortcut_to_ORDER_LINE_ITEM_1.ADJUSTED_ORDER_QTY AS ADJUSTED_ORDER_QTY,
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0.ORDER_ID AS ORDER_ID1,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ORDER_LINE_ITEM_1,
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0
WHERE
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0.ORDER_ID = Shortcut_to_ORDER_LINE_ITEM_1.ORDER_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_ORDER_LINE_ITEM_2")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_3


query_3 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  MASTER_ORDER_ID AS MASTER_ORDER_ID,
  MO_LINE_ITEM_ID AS MO_LINE_ITEM_ID,
  TOTAL_MONETARY_VALUE AS TOTAL_MONETARY_VALUE,
  UNIT_MONETARY_VALUE AS UNIT_MONETARY_VALUE,
  UNIT_TAX_AMOUNT AS UNIT_TAX_AMOUNT,
  SHIPPED_QTY AS SHIPPED_QTY,
  RECEIVED_QTY AS RECEIVED_QTY,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DO_DTL_STATUS AS DO_DTL_STATUS,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  UNIT_COST AS UNIT_COST,
  UNIT_PRICE_AMOUNT AS UNIT_PRICE_AMOUNT,
  USER_CANCELED_QTY AS USER_CANCELED_QTY,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  EVENT_CODE AS EVENT_CODE,
  REASON_CODE AS REASON_CODE,
  EXP_INFO_CODE AS EXP_INFO_CODE,
  PARTL_FILL AS PARTL_FILL,
  ORDER_QTY AS ORDER_QTY,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RETAIL_PRICE AS RETAIL_PRICE,
  ITEM_ID AS ITEM_ID,
  TC_ORDER_LINE_ID AS TC_ORDER_LINE_ID,
  ACTUAL_SHIPPED_DTTM AS ACTUAL_SHIPPED_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  PURCHASE_ORDER_LINE_NUMBER AS PURCHASE_ORDER_LINE_NUMBER,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  IS_CANCELLED AS IS_CANCELLED,
  ITEM_NAME AS ITEM_NAME,
  PURCHASE_ORDER_NUMBER AS PURCHASE_ORDER_NUMBER,
  FREIGHT_REVENUE_CURRENCY_CODE AS FREIGHT_REVENUE_CURRENCY_CODE,
  FREIGHT_REVENUE AS FREIGHT_REVENUE,
  ADJUSTED_ORDER_QTY AS ADJUSTED_ORDER_QTY,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ORDER_LINE_ITEM_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_LOAD_TSTMP_3")

# COMMAND ----------
# DBTITLE 1, OMS_ORDER_LINE_ITEM_PRE


spark.sql("""INSERT INTO
  OMS_ORDER_LINE_ITEM_PRE
SELECT
  ORDER_ID AS ORDER_ID,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  MASTER_ORDER_ID AS MASTER_ORDER_ID,
  MO_LINE_ITEM_ID AS MO_LINE_ITEM_ID,
  TOTAL_MONETARY_VALUE AS TOTAL_MONETARY_VALUE,
  UNIT_MONETARY_VALUE AS UNIT_MONETARY_VALUE,
  UNIT_TAX_AMOUNT AS UNIT_TAX_AMOUNT,
  SHIPPED_QTY AS SHIPPED_QTY,
  RECEIVED_QTY AS RECEIVED_QTY,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DO_DTL_STATUS AS DO_DTL_STATUS,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  UNIT_COST AS UNIT_COST,
  UNIT_PRICE_AMOUNT AS UNIT_PRICE_AMOUNT,
  USER_CANCELED_QTY AS USER_CANCELED_QTY,
  DELIVERY_END_DTTM AS DELIVERY_END_DTTM,
  DELIVERY_START_DTTM AS DELIVERY_START_DTTM,
  EVENT_CODE AS EVENT_CODE,
  REASON_CODE AS REASON_CODE,
  EXP_INFO_CODE AS EXP_INFO_CODE,
  PARTL_FILL AS PARTL_FILL,
  ORDER_QTY AS ORDER_QTY,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RETAIL_PRICE AS RETAIL_PRICE,
  ITEM_ID AS ITEM_ID,
  TC_ORDER_LINE_ID AS TC_ORDER_LINE_ID,
  ACTUAL_SHIPPED_DTTM AS ACTUAL_SHIPPED_DTTM,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  PURCHASE_ORDER_LINE_NUMBER AS PURCHASE_ORDER_LINE_NUMBER,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  IS_CANCELLED AS IS_CANCELLED,
  ITEM_NAME AS ITEM_NAME,
  PURCHASE_ORDER_NUMBER AS PURCHASE_ORDER_NUMBER,
  FREIGHT_REVENUE_CURRENCY_CODE AS FREIGHT_REVENUE_CURRENCY_CODE,
  FREIGHT_REVENUE AS FREIGHT_REVENUE,
  ADJUSTED_ORDER_QTY AS ADJUSTED_ORDER_QTY,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Line_Item_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Order_Line_Item_Pre", mainWorkflowId, parentName)