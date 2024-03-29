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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Orders_Abandoned_Overdue_Flag_UPD")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Orders_Abandoned_Overdue_Flag_UPD", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDERS_DDS_0


query_0 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_TC_COMPANY_ID AS OMS_TC_COMPANY_ID,
  OMS_TC_ORDER_ID AS OMS_TC_ORDER_ID,
  OMS_PURCHASE_ORDER_ID AS OMS_PURCHASE_ORDER_ID,
  OMS_PURCHASE_ORDER_NBR AS OMS_PURCHASE_ORDER_NBR,
  OMS_EXT_PURCHASE_ORDER AS OMS_EXT_PURCHASE_ORDER,
  OMS_ORDER_TYPE AS OMS_ORDER_TYPE,
  OMS_ORDER_STATUS_ID AS OMS_ORDER_STATUS_ID,
  OMS_DO_TYPE_ID AS OMS_DO_TYPE_ID,
  OMS_DO_STATUS_ID AS OMS_DO_STATUS_ID,
  OMS_SHIPMENT_ID AS OMS_SHIPMENT_ID,
  OMS_CREATION_TYPE_ID AS OMS_CREATION_TYPE_ID,
  OMS_BILLING_METHOD_ID AS OMS_BILLING_METHOD_ID,
  DROPOFF_PICKUP AS DROPOFF_PICKUP,
  OMS_DELIVERY_OPTIONS_ID AS OMS_DELIVERY_OPTIONS_ID,
  CUSTOMER_PICKUP_FLAG AS CUSTOMER_PICKUP_FLAG,
  PARTIALLY_PLANNED_FLAG AS PARTIALLY_PLANNED_FLAG,
  PARTIAL_SHIP_CONFIRMATION_FLAG AS PARTIAL_SHIP_CONFIRMATION_FLAG,
  PARTIAL_SHIP_CONFIRMATION_STATUS AS PARTIAL_SHIP_CONFIRMATION_STATUS,
  HAS_ALERTS_FLAG AS HAS_ALERTS_FLAG,
  HAS_SPLIT_FLAG AS HAS_SPLIT_FLAG,
  CANCELLED_FLAG AS CANCELLED_FLAG,
  BACK_ORDERED_FLAG AS BACK_ORDERED_FLAG,
  OMS_INBOUND_REGION_ID AS OMS_INBOUND_REGION_ID,
  OMS_OUTBOUND_REGION_ID AS OMS_OUTBOUND_REGION_ID,
  OMS_O_FACILITY_ALIAS_ID AS OMS_O_FACILITY_ALIAS_ID,
  OMS_O_FACILITY_ID AS OMS_O_FACILITY_ID,
  OMS_D_FACILITY_ALIAS_ID AS OMS_D_FACILITY_ALIAS_ID,
  OMS_D_FACILITY_ID AS OMS_D_FACILITY_ID,
  OMS_BILL_FACILITY_ALIAS_ID AS OMS_BILL_FACILITY_ALIAS_ID,
  OMS_BILL_FACILITY_ID AS OMS_BILL_FACILITY_ID,
  OMS_CUSTOMER_ID AS OMS_CUSTOMER_ID,
  ADDR_VALID_FLAG AS ADDR_VALID_FLAG,
  OMS_PICKUP_START_TSTMP AS OMS_PICKUP_START_TSTMP,
  OMS_PICKUP_END_TSTMP AS OMS_PICKUP_END_TSTMP,
  OMS_PICKUP_TZ_ID AS OMS_PICKUP_TZ_ID,
  OMS_DELIVERY_TZ_ID AS OMS_DELIVERY_TZ_ID,
  OMS_DELIVERY_START_TSTMP AS OMS_DELIVERY_START_TSTMP,
  OMS_DELIVERY_END_TSTMP AS OMS_DELIVERY_END_TSTMP,
  OMS_ORDER_DT_TSTMP AS OMS_ORDER_DT_TSTMP,
  OMS_ORDER_RECON_TSTMP AS OMS_ORDER_RECON_TSTMP,
  OMS_SCHED_PICKUP_TSTMP AS OMS_SCHED_PICKUP_TSTMP,
  OMS_SCHED_DELIVERY_TSTMP AS OMS_SCHED_DELIVERY_TSTMP,
  OMS_ACTUAL_SHIPPED_TSTMP AS OMS_ACTUAL_SHIPPED_TSTMP,
  TOTAL_NBR_OF_UNITS AS TOTAL_NBR_OF_UNITS,
  BASELINE_COST AS BASELINE_COST,
  ACTUAL_COST AS ACTUAL_COST,
  MONETARY_VALUE AS MONETARY_VALUE,
  MV_CURRENCY_CD AS MV_CURRENCY_CD,
  REF_NUM1 AS REF_NUM1,
  OMS_CREATED_SOURCE_TYPE_ID AS OMS_CREATED_SOURCE_TYPE_ID,
  OMS_CREATED_SOURCE AS OMS_CREATED_SOURCE,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  ABANDONED_FLAG AS ABANDONED_FLAG,
  ABANDONED_OVERDUE_FLAG AS ABANDONED_OVERDUE_FLAG
FROM
  OMS_ORDERS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ORDERS_DDS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ORDERS_1


query_1 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_TC_COMPANY_ID AS OMS_TC_COMPANY_ID,
  OMS_TC_ORDER_ID AS OMS_TC_ORDER_ID,
  OMS_PURCHASE_ORDER_ID AS OMS_PURCHASE_ORDER_ID,
  OMS_PURCHASE_ORDER_NBR AS OMS_PURCHASE_ORDER_NBR,
  OMS_EXT_PURCHASE_ORDER AS OMS_EXT_PURCHASE_ORDER,
  OMS_ORDER_TYPE AS OMS_ORDER_TYPE,
  OMS_ORDER_STATUS_ID AS OMS_ORDER_STATUS_ID,
  OMS_DO_TYPE_ID AS OMS_DO_TYPE_ID,
  OMS_DO_STATUS_ID AS OMS_DO_STATUS_ID,
  OMS_SHIPMENT_ID AS OMS_SHIPMENT_ID,
  OMS_CREATION_TYPE_ID AS OMS_CREATION_TYPE_ID,
  OMS_BILLING_METHOD_ID AS OMS_BILLING_METHOD_ID,
  DROPOFF_PICKUP AS DROPOFF_PICKUP,
  OMS_DELIVERY_OPTIONS_ID AS OMS_DELIVERY_OPTIONS_ID,
  CUSTOMER_PICKUP_FLAG AS CUSTOMER_PICKUP_FLAG,
  PARTIALLY_PLANNED_FLAG AS PARTIALLY_PLANNED_FLAG,
  PARTIAL_SHIP_CONFIRMATION_FLAG AS PARTIAL_SHIP_CONFIRMATION_FLAG,
  PARTIAL_SHIP_CONFIRMATION_STATUS AS PARTIAL_SHIP_CONFIRMATION_STATUS,
  HAS_ALERTS_FLAG AS HAS_ALERTS_FLAG,
  HAS_SPLIT_FLAG AS HAS_SPLIT_FLAG,
  CANCELLED_FLAG AS CANCELLED_FLAG,
  BACK_ORDERED_FLAG AS BACK_ORDERED_FLAG,
  OMS_INBOUND_REGION_ID AS OMS_INBOUND_REGION_ID,
  OMS_OUTBOUND_REGION_ID AS OMS_OUTBOUND_REGION_ID,
  OMS_O_FACILITY_ALIAS_ID AS OMS_O_FACILITY_ALIAS_ID,
  OMS_O_FACILITY_ID AS OMS_O_FACILITY_ID,
  OMS_D_FACILITY_ALIAS_ID AS OMS_D_FACILITY_ALIAS_ID,
  OMS_D_FACILITY_ID AS OMS_D_FACILITY_ID,
  OMS_BILL_FACILITY_ALIAS_ID AS OMS_BILL_FACILITY_ALIAS_ID,
  OMS_BILL_FACILITY_ID AS OMS_BILL_FACILITY_ID,
  OMS_CUSTOMER_ID AS OMS_CUSTOMER_ID,
  ADDR_VALID_FLAG AS ADDR_VALID_FLAG,
  OMS_PICKUP_START_TSTMP AS OMS_PICKUP_START_TSTMP,
  OMS_PICKUP_END_TSTMP AS OMS_PICKUP_END_TSTMP,
  OMS_PICKUP_TZ_ID AS OMS_PICKUP_TZ_ID,
  OMS_DELIVERY_TZ_ID AS OMS_DELIVERY_TZ_ID,
  OMS_DELIVERY_START_TSTMP AS OMS_DELIVERY_START_TSTMP,
  OMS_DELIVERY_END_TSTMP AS OMS_DELIVERY_END_TSTMP,
  OMS_ORDER_DT_TSTMP AS OMS_ORDER_DT_TSTMP,
  OMS_ORDER_RECON_TSTMP AS OMS_ORDER_RECON_TSTMP,
  OMS_SCHED_PICKUP_TSTMP AS OMS_SCHED_PICKUP_TSTMP,
  OMS_SCHED_DELIVERY_TSTMP AS OMS_SCHED_DELIVERY_TSTMP,
  OMS_ACTUAL_SHIPPED_TSTMP AS OMS_ACTUAL_SHIPPED_TSTMP,
  TOTAL_NBR_OF_UNITS AS TOTAL_NBR_OF_UNITS,
  BASELINE_COST AS BASELINE_COST,
  ACTUAL_COST AS ACTUAL_COST,
  MONETARY_VALUE AS MONETARY_VALUE,
  MV_CURRENCY_CD AS MV_CURRENCY_CD,
  REF_NUM1 AS REF_NUM1,
  OMS_CREATED_SOURCE_TYPE_ID AS OMS_CREATED_SOURCE_TYPE_ID,
  OMS_CREATED_SOURCE AS OMS_CREATED_SOURCE,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  ABANDONED_FLAG AS ABANDONED_FLAG,
  ABANDONED_OVERDUE_FLAG AS ABANDONED_OVERDUE_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ORDERS_DDS_0
WHERE
  CAST(
    Shortcut_to_OMS_ORDERS_DDS_0.OMS_DELIVERY_OPTIONS_ID AS NUMERIC(1)
  ) = 1
  AND OMS_DO_STATUS_ID = 190
  AND CANCELLED_FLAG = 0
  AND ABANDONED_FLAG <> 1
  AND ABANDONED_OVERDUE_FLAG <> 1
  AND DATE(Shortcut_to_OMS_ORDERS_DDS_0.OMS_CREATED_TSTMP) >= ADD_MONTHS(DATE(CURRENT_TIMESTAMP), - 3)"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_ORDERS_1")

# COMMAND ----------
# DBTITLE 1, EXP_OMS_Order_Source1_2


query_2 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_PICKUP_END_TSTMP AS OMS_PICKUP_END_TSTMP,
  REF_NUM1 AS REF_NUM1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_ORDERS_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_OMS_Order_Source1_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_RETURN_ORDER_LN2_3


query_3 = f"""SELECT
  OMS_RETURN_ORDER_ID AS OMS_RETURN_ORDER_ID,
  OMS_RETURN_ORDER_LN_ID AS OMS_RETURN_ORDER_LN_ID,
  OMS_COMPANY_ID AS OMS_COMPANY_ID,
  PRODUCT_ID AS PRODUCT_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RETURN_ORDER_QTY AS RETURN_ORDER_QTY,
  ORIG_LINE_TOTAL AS ORIG_LINE_TOTAL,
  RETURN_LINE_TOTAL AS RETURN_LINE_TOTAL,
  LAST_UPDATED_TSTMP AS LAST_UPDATED_TSTMP,
  OMS_RETURN_ORDER_LN_NBR AS OMS_RETURN_ORDER_LN_NBR,
  OMS_RETURN_ORDER_NBR AS OMS_RETURN_ORDER_NBR,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  ORDER_NBR AS ORDER_NBR,
  OMS_ORDER_CREATED_TSTMP AS OMS_ORDER_CREATED_TSTMP,
  OMS_ORDER_RETURNED_TSTMP AS OMS_ORDER_RETURNED_TSTMP,
  RETURN_REASON_ID AS RETURN_REASON_ID,
  RETURN_DESC AS RETURN_DESC,
  RETURN_SHORT_DESC AS RETURN_SHORT_DESC,
  GOODS_EXPECTED_FLG AS GOODS_EXPECTED_FLG,
  PRODUCT_RETURNED_TSTMP AS PRODUCT_RETURNED_TSTMP,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_RETURN_ORDER_LN"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_OMS_RETURN_ORDER_LN2_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_RETURN_ORDER_LN_4


query_4 = f"""SELECT
  OMS_RETURN_ORDER_ID AS OMS_RETURN_ORDER_ID,
  OMS_RETURN_ORDER_LN_ID AS OMS_RETURN_ORDER_LN_ID,
  OMS_COMPANY_ID AS OMS_COMPANY_ID,
  PRODUCT_ID AS PRODUCT_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RETURN_ORDER_QTY AS RETURN_ORDER_QTY,
  ORIG_LINE_TOTAL AS ORIG_LINE_TOTAL,
  RETURN_LINE_TOTAL AS RETURN_LINE_TOTAL,
  LAST_UPDATED_TSTMP AS LAST_UPDATED_TSTMP,
  OMS_RETURN_ORDER_LN_NBR AS OMS_RETURN_ORDER_LN_NBR,
  OMS_RETURN_ORDER_NBR AS OMS_RETURN_ORDER_NBR,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  ORDER_NBR AS ORDER_NBR,
  OMS_ORDER_CREATED_TSTMP AS OMS_ORDER_CREATED_TSTMP,
  OMS_ORDER_RETURNED_TSTMP AS OMS_ORDER_RETURNED_TSTMP,
  RETURN_REASON_ID AS RETURN_REASON_ID,
  RETURN_DESC AS RETURN_DESC,
  RETURN_SHORT_DESC AS RETURN_SHORT_DESC,
  GOODS_EXPECTED_FLG AS GOODS_EXPECTED_FLG,
  PRODUCT_RETURNED_TSTMP AS PRODUCT_RETURNED_TSTMP,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_RETURN_ORDER_LN2_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_Shortcut_to_OMS_RETURN_ORDER_LN_4")

# COMMAND ----------
# DBTITLE 1, EXP_OMS_Ret_Order_Source_5


query_5 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  RETURN_DESC AS RETURN_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_RETURN_ORDER_LN_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_OMS_Ret_Order_Source_5")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_Order_Return_Order_6


query_6 = f"""SELECT
  MASTER.OMS_ORDER_ID AS OMS_ORDER_ID_RO,
  DETAIL.OMS_ORDER_ID AS OMS_ORDER_ID,
  MASTER.RETURN_DESC AS RETURN_DESC,
  DETAIL.OMS_PICKUP_END_TSTMP AS OMS_PICKUP_END_TSTMP,
  DETAIL.REF_NUM1 AS REF_NUM1,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_OMS_Ret_Order_Source_5 MASTER
  RIGHT JOIN EXP_OMS_Order_Source1_2 DETAIL ON MASTER.OMS_ORDER_ID = DETAIL.OMS_ORDER_ID"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("JNR_OMS_Order_Return_Order_6")

# COMMAND ----------
# DBTITLE 1, EXP_Update_7


query_7 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  IFF (
    TRUNC(now()) > ADD_TO_DATE(TRUNC(OMS_PICKUP_END_TSTMP), 'DD', {DT}),
    IFF(REF_NUM1 = 1, IFF (ISNULL(OMS_ORDER_ID_RO), 1, 0))
  ) AS o_ABANDONED_OVERDUE_FLAG,
  SESSSTARTTIME AS o_UPDATE_TSTMP,
  OMS_ORDER_ID_RO AS OMS_ORDER_ID_RO,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_Order_Return_Order_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("EXP_Update_7")

# COMMAND ----------
# DBTITLE 1, LKP_Target_8


query_8 = f"""SELECT
  OO.OMS_ORDER_ID AS OMS_ORDER_ID,
  OO.OMS_TC_COMPANY_ID AS OMS_TC_COMPANY_ID,
  OO.OMS_TC_ORDER_ID AS OMS_TC_ORDER_ID,
  OO.OMS_PURCHASE_ORDER_ID AS OMS_PURCHASE_ORDER_ID,
  OO.OMS_PURCHASE_ORDER_NBR AS OMS_PURCHASE_ORDER_NBR,
  OO.OMS_EXT_PURCHASE_ORDER AS OMS_EXT_PURCHASE_ORDER,
  OO.OMS_ORDER_TYPE AS OMS_ORDER_TYPE,
  OO.OMS_ORDER_STATUS_ID AS OMS_ORDER_STATUS_ID,
  OO.OMS_DO_TYPE_ID AS OMS_DO_TYPE_ID,
  OO.OMS_DO_STATUS_ID AS OMS_DO_STATUS_ID,
  OO.OMS_SHIPMENT_ID AS OMS_SHIPMENT_ID,
  OO.OMS_CREATION_TYPE_ID AS OMS_CREATION_TYPE_ID,
  OO.OMS_BILLING_METHOD_ID AS OMS_BILLING_METHOD_ID,
  OO.DROPOFF_PICKUP AS DROPOFF_PICKUP,
  OO.OMS_DELIVERY_OPTIONS_ID AS OMS_DELIVERY_OPTIONS_ID,
  OO.CUSTOMER_PICKUP_FLAG AS CUSTOMER_PICKUP_FLAG,
  OO.PARTIALLY_PLANNED_FLAG AS PARTIALLY_PLANNED_FLAG,
  OO.PARTIAL_SHIP_CONFIRMATION_FLAG AS PARTIAL_SHIP_CONFIRMATION_FLAG,
  OO.PARTIAL_SHIP_CONFIRMATION_STATUS AS PARTIAL_SHIP_CONFIRMATION_STATUS,
  OO.HAS_ALERTS_FLAG AS HAS_ALERTS_FLAG,
  OO.HAS_SPLIT_FLAG AS HAS_SPLIT_FLAG,
  OO.CANCELLED_FLAG AS CANCELLED_FLAG,
  OO.BACK_ORDERED_FLAG AS BACK_ORDERED_FLAG,
  OO.OMS_INBOUND_REGION_ID AS OMS_INBOUND_REGION_ID,
  OO.OMS_OUTBOUND_REGION_ID AS OMS_OUTBOUND_REGION_ID,
  OO.OMS_O_FACILITY_ALIAS_ID AS OMS_O_FACILITY_ALIAS_ID,
  OO.OMS_O_FACILITY_ID AS OMS_O_FACILITY_ID,
  OO.OMS_D_FACILITY_ALIAS_ID AS OMS_D_FACILITY_ALIAS_ID,
  OO.OMS_D_FACILITY_ID AS OMS_D_FACILITY_ID,
  OO.OMS_BILL_FACILITY_ALIAS_ID AS OMS_BILL_FACILITY_ALIAS_ID,
  OO.OMS_BILL_FACILITY_ID AS OMS_BILL_FACILITY_ID,
  OO.OMS_CUSTOMER_ID AS OMS_CUSTOMER_ID,
  OO.ADDR_VALID_FLAG AS ADDR_VALID_FLAG,
  OO.OMS_PICKUP_START_TSTMP AS OMS_PICKUP_START_TSTMP,
  OO.OMS_PICKUP_END_TSTMP AS OMS_PICKUP_END_TSTMP,
  OO.OMS_PICKUP_TZ_ID AS OMS_PICKUP_TZ_ID,
  OO.OMS_DELIVERY_TZ_ID AS OMS_DELIVERY_TZ_ID,
  OO.OMS_DELIVERY_START_TSTMP AS OMS_DELIVERY_START_TSTMP,
  OO.OMS_DELIVERY_END_TSTMP AS OMS_DELIVERY_END_TSTMP,
  OO.OMS_ORDER_DT_TSTMP AS OMS_ORDER_DT_TSTMP,
  OO.OMS_ORDER_RECON_TSTMP AS OMS_ORDER_RECON_TSTMP,
  OO.OMS_SCHED_PICKUP_TSTMP AS OMS_SCHED_PICKUP_TSTMP,
  OO.OMS_SCHED_DELIVERY_TSTMP AS OMS_SCHED_DELIVERY_TSTMP,
  OO.OMS_ACTUAL_SHIPPED_TSTMP AS OMS_ACTUAL_SHIPPED_TSTMP,
  OO.TOTAL_NBR_OF_UNITS AS TOTAL_NBR_OF_UNITS,
  OO.BASELINE_COST AS BASELINE_COST,
  OO.ACTUAL_COST AS ACTUAL_COST,
  OO.MONETARY_VALUE AS MONETARY_VALUE,
  OO.MV_CURRENCY_CD AS MV_CURRENCY_CD,
  OO.REF_NUM1 AS REF_NUM1,
  OO.OMS_CREATED_SOURCE_TYPE_ID AS OMS_CREATED_SOURCE_TYPE_ID,
  OO.OMS_CREATED_SOURCE AS OMS_CREATED_SOURCE,
  OO.OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OO.OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  OO.UPDATE_TSTMP AS UPDATE_TSTMP,
  OO.LOAD_TSTMP AS LOAD_TSTMP,
  OO.ABANDONED_FLAG AS ABANDONED_FLAG,
  OO.ABANDONED_OVERDUE_FLAG AS ABANDONED_OVERDUE_FLAG,
  EU7.OMS_ORDER_ID AS in_OMS_ORDER_ID,
  EU7.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Update_7 EU7
  LEFT JOIN OMS_ORDERS OO ON OO.OMS_ORDER_ID = EU7.OMS_ORDER_ID"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("LKP_Target_8")

# COMMAND ----------
# DBTITLE 1, EXP_Update_Chk_9


query_9 = f"""SELECT
  EU7.OMS_ORDER_ID AS OMS_ORDER_ID,
  EU7.o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  LT8.ABANDONED_FLAG AS lkp_ABANDONED_FLAG,
  IFF (
    NOT ISNULL(LT8.OMS_ORDER_ID),
    IFF (
      EU7.o_ABANDONED_OVERDUE_FLAG <> LT8.ABANDONED_OVERDUE_FLAG,
      1,
      0
    ),
    0
  ) AS o_Flag,
  IFF (
    LT8.ABANDONED_OVERDUE_FLAG = 1
    AND LT8.REF_NUM1 = 0
    AND LT8.ABANDONED_FLAG = 1,
    0,
    EU7.o_ABANDONED_OVERDUE_FLAG
  ) AS o_ABANDONED_OVERDUE_FLAG,
  EU7.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Update_7 EU7
  INNER JOIN LKP_Target_8 LT8 ON EU7.Monotonically_Increasing_Id = LT8.Monotonically_Increasing_Id"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("EXP_Update_Chk_9")

# COMMAND ----------
# DBTITLE 1, FIL_Check_10


query_10 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  o_ABANDONED_OVERDUE_FLAG AS o_ABANDONED_OVERDUE_FLAG,
  o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  o_Flag AS o_Flag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Update_Chk_9
WHERE
  o_Flag = 1"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("FIL_Check_10")

# COMMAND ----------
# DBTITLE 1, SRT_Update_11


query_11 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  o_ABANDONED_OVERDUE_FLAG AS o_ABANDONED_OVERDUE_FLAG,
  o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_Check_10
ORDER BY
  OMS_ORDER_ID ASC,
  o_ABANDONED_OVERDUE_FLAG ASC,
  o_UPDATE_TSTMP ASC"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("SRT_Update_11")

# COMMAND ----------
# DBTITLE 1, FIL_Update_12


query_12 = f"""SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  o_ABANDONED_OVERDUE_FLAG AS o_ABANDONED_OVERDUE_FLAG,
  o_UPDATE_TSTMP AS o_UPDATE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SRT_Update_11
WHERE
  o_ABANDONED_OVERDUE_FLAG = 1"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("FIL_Update_12")

# COMMAND ----------
# DBTITLE 1, OMS_ORDERS


spark.sql("""INSERT INTO
  OMS_ORDERS
SELECT
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  NULL AS OMS_TC_COMPANY_ID,
  NULL AS OMS_TC_ORDER_ID,
  NULL AS OMS_PURCHASE_ORDER_ID,
  NULL AS OMS_PURCHASE_ORDER_NBR,
  NULL AS OMS_EXT_PURCHASE_ORDER,
  NULL AS OMS_ORDER_TYPE,
  NULL AS OMS_ORDER_STATUS_ID,
  NULL AS OMS_DO_TYPE_ID,
  NULL AS OMS_DO_STATUS_ID,
  NULL AS OMS_SHIPMENT_ID,
  NULL AS OMS_CREATION_TYPE_ID,
  NULL AS OMS_BILLING_METHOD_ID,
  NULL AS DROPOFF_PICKUP,
  NULL AS OMS_DELIVERY_OPTIONS_ID,
  NULL AS CUSTOMER_PICKUP_FLAG,
  NULL AS PARTIALLY_PLANNED_FLAG,
  NULL AS PARTIAL_SHIP_CONFIRMATION_FLAG,
  NULL AS PARTIAL_SHIP_CONFIRMATION_STATUS,
  NULL AS HAS_ALERTS_FLAG,
  NULL AS HAS_SPLIT_FLAG,
  NULL AS CANCELLED_FLAG,
  NULL AS BACK_ORDERED_FLAG,
  NULL AS OMS_INBOUND_REGION_ID,
  NULL AS OMS_OUTBOUND_REGION_ID,
  NULL AS OMS_O_FACILITY_ALIAS_ID,
  NULL AS OMS_O_FACILITY_ID,
  NULL AS OMS_D_FACILITY_ALIAS_ID,
  NULL AS OMS_D_FACILITY_ID,
  NULL AS OMS_BILL_FACILITY_ALIAS_ID,
  NULL AS OMS_BILL_FACILITY_ID,
  NULL AS OMS_CUSTOMER_ID,
  NULL AS ADDR_VALID_FLAG,
  NULL AS OMS_PICKUP_START_TSTMP,
  NULL AS OMS_PICKUP_END_TSTMP,
  NULL AS OMS_PICKUP_TZ_ID,
  NULL AS OMS_DELIVERY_TZ_ID,
  NULL AS OMS_DELIVERY_START_TSTMP,
  NULL AS OMS_DELIVERY_END_TSTMP,
  NULL AS OMS_ORDER_DT_TSTMP,
  NULL AS OMS_ORDER_RECON_TSTMP,
  NULL AS OMS_SCHED_PICKUP_TSTMP,
  NULL AS OMS_SCHED_DELIVERY_TSTMP,
  NULL AS OMS_ACTUAL_SHIPPED_TSTMP,
  NULL AS TOTAL_NBR_OF_UNITS,
  NULL AS BASELINE_COST,
  NULL AS ACTUAL_COST,
  NULL AS MONETARY_VALUE,
  NULL AS MV_CURRENCY_CD,
  NULL AS REF_NUM1,
  NULL AS OMS_CREATED_SOURCE_TYPE_ID,
  NULL AS OMS_CREATED_SOURCE,
  NULL AS OMS_CREATED_TSTMP,
  NULL AS OMS_LAST_UPDATED_TSTMP,
  o_UPDATE_TSTMP AS UPDATE_TSTMP,
  o_UPDATE_TSTMP AS UPDATE_TSTMP,
  NULL AS LOAD_TSTMP,
  o_ABANDONED_FLAG AS ABANDONED_FLAG,
  o_ABANDONED_OVERDUE_FLAG AS ABANDONED_OVERDUE_FLAG,
  o_ABANDONED_OVERDUE_FLAG AS ABANDONED_OVERDUE_FLAG
FROM
  FIL_Update_12""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Orders_Abandoned_Overdue_Flag_UPD")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Orders_Abandoned_Overdue_Flag_UPD", mainWorkflowId, parentName)
