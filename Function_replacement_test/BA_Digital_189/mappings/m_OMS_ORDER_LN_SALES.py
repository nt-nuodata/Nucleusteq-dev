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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_ORDER_LN_SALES")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_OMS_ORDER_LN_SALES", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_LN_SALES_ETL_INS_VW_0


query_0 = f"""SELECT
  CREATED_TSTMP AS CREATED_TSTMP,
  OMS_ORDER_DT AS OMS_ORDER_DT,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  OMS_ORDER_NBR AS OMS_ORDER_NBR,
  ORDER_NBR AS ORDER_NBR,
  ORDER_CHANNEL AS ORDER_CHANNEL,
  ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
  ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
  PRODUCT_ID AS PRODUCT_ID,
  SCHED_DELIVERY_FLG AS SCHED_DELIVERY_FLG,
  ADD_ON_FLAG AS ADD_ON_FLAG,
  CANCELLED_FLG AS CANCELLED_FLG,
  OMS_DO_TYPE_ID AS OMS_DO_TYPE_ID,
  SHIP_POSTAL_CD AS SHIP_POSTAL_CD,
  SHIP_COUNTRY_CD AS SHIP_COUNTRY_CD,
  SHIP_STATE AS SHIP_STATE,
  BILL_POSTAL_CD AS BILL_POSTAL_CD,
  BILL_COUNTRY_CD AS BILL_COUNTRY_CD,
  OMS_ORDER_STATUS_ID AS OMS_ORDER_STATUS_ID,
  OMS_ORDER_LN_STATUS_ID AS OMS_ORDER_LN_STATUS_ID,
  FULF_LOCATION_ID AS FULF_LOCATION_ID,
  FULF_LOCATION_NBR AS FULF_LOCATION_NBR,
  FULF_STORE_NAME AS FULF_STORE_NAME,
  FULF_LOCATION_TYPE AS FULF_LOCATION_TYPE,
  FULF_LOCATION_TYPE_DESC AS FULF_LOCATION_TYPE_DESC,
  FULF_LOC_GROUP_ID AS FULF_LOC_GROUP_ID,
  FULF_LOC_GROUP_DESC AS FULF_LOC_GROUP_DESC,
  CREATION_DEVICE_TYPE AS CREATION_DEVICE_TYPE,
  CREATION_DEVICE_WIDTH AS CREATION_DEVICE_WIDTH,
  SHIPPED_DT AS SHIPPED_DT,
  AGING_DAYS AS AGING_DAYS,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  CANCELLED_QTY AS CANCELLED_QTY,
  UNIT_MONETARY_VALUE_AMT AS UNIT_MONETARY_VALUE_AMT,
  ORIG_SHIP_CHARGE_AMT AS ORIG_SHIP_CHARGE_AMT,
  SHIP_CHARGE_AMT AS SHIP_CHARGE_AMT,
  HEADER_DISC_AMT AS HEADER_DISC_AMT,
  LN_DISC_AMT AS LN_DISC_AMT,
  UNIT_TAX_AMT AS UNIT_TAX_AMT,
  NET_SALES_QTY AS NET_SALES_QTY,
  NET_SALES_AMT AS NET_SALES_AMT,
  SALES_QTY AS SALES_QTY,
  SALES_AMT AS SALES_AMT,
  NET_SALES_TAX_AMT AS NET_SALES_TAX_AMT,
  DISCOUNT_AMT AS DISCOUNT_AMT,
  RETURN_QTY AS RETURN_QTY,
  RETURN_AMT AS RETURN_AMT,
  NET_MARGIN_AMT AS NET_MARGIN_AMT,
  PURCH_COST_AMT AS PURCH_COST_AMT,
  SALES_COST AS SALES_COST,
  EXCH_RATE_PCT AS EXCH_RATE_PCT,
  NET_ORDER_SALES_DISC_AMT AS NET_ORDER_SALES_DISC_AMT,
  NET_ORDER_MARGIN_DISC_AMT AS NET_ORDER_MARGIN_DISC_AMT,
  NET_SALES_SHIP_AMT AS NET_SALES_SHIP_AMT,
  MARGIN_PRE_AMT AS MARGIN_PRE_AMT,
  MA_VF_AMT AS MA_VF_AMT,
  MA_VF_PRODUCT_AMT AS MA_VF_PRODUCT_AMT,
  MA_SALES_VF_AMT AS MA_SALES_VF_AMT,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  PICK_DECL_QTY AS PICK_DECL_QTY,
  ORDER_AGE AS ORDER_AGE,
  RX_TYPE AS RX_TYPE
FROM
  OMS_ORDER_LN_SALES_ETL_INS_VW"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ORDER_LN_SALES_ETL_INS_VW_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ORDER_LN_SALES_ETL_INS_VW_1


query_1 = f"""SELECT
  CREATED_TSTMP AS CREATED_TSTMP,
  OMS_ORDER_DT AS OMS_ORDER_DT,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_NBR AS OMS_ORDER_NBR,
  ORDER_NBR AS ORDER_NBR,
  ORDER_CHANNEL AS ORDER_CHANNEL,
  ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
  ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
  PRODUCT_ID AS PRODUCT_ID,
  SCHED_DELIVERY_FLG AS SCHED_DELIVERY_FLG,
  ADD_ON_FLAG AS ADD_ON_FLAG,
  CANCELLED_FLG AS CANCELLED_FLG,
  OMS_DO_TYPE_ID AS OMS_DO_TYPE_ID,
  SHIP_POSTAL_CD AS SHIP_POSTAL_CD,
  SHIP_COUNTRY_CD AS SHIP_COUNTRY_CD,
  SHIP_STATE AS SHIP_STATE,
  BILL_POSTAL_CD AS BILL_POSTAL_CD,
  BILL_COUNTRY_CD AS BILL_COUNTRY_CD,
  OMS_ORDER_STATUS_ID AS OMS_ORDER_STATUS_ID,
  OMS_ORDER_LN_STATUS_ID AS OMS_ORDER_LN_STATUS_ID,
  FULF_LOCATION_ID AS FULF_LOCATION_ID,
  FULF_LOCATION_NBR AS FULF_LOCATION_NBR,
  FULF_STORE_NAME AS FULF_STORE_NAME,
  FULF_LOCATION_TYPE AS FULF_LOCATION_TYPE,
  FULF_LOCATION_TYPE_DESC AS FULF_LOCATION_TYPE_DESC,
  FULF_LOC_GROUP_ID AS FULF_LOC_GROUP_ID,
  FULF_LOC_GROUP_DESC AS FULF_LOC_GROUP_DESC,
  CREATION_DEVICE_TYPE AS CREATION_DEVICE_TYPE,
  CREATION_DEVICE_WIDTH AS CREATION_DEVICE_WIDTH,
  SHIPPED_DT AS SHIPPED_DT,
  AGING_DAYS AS AGING_DAYS,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  CANCELLED_QTY AS CANCELLED_QTY,
  UNIT_MONETARY_VALUE_AMT AS UNIT_MONETARY_VALUE_AMT,
  ORIG_SHIP_CHARGE_AMT AS ORIG_SHIP_CHARGE_AMT,
  SHIP_CHARGE_AMT AS SHIP_CHARGE_AMT,
  HEADER_DISC_AMT AS HEADER_DISC_AMT,
  LN_DISC_AMT AS LN_DISC_AMT,
  UNIT_TAX_AMT AS UNIT_TAX_AMT,
  NET_SALES_QTY AS NET_SALES_QTY,
  NET_SALES_AMT AS NET_SALES_AMT,
  SALES_QTY AS SALES_QTY,
  SALES_AMT AS SALES_AMT,
  NET_SALES_TAX_AMT AS NET_SALES_TAX_AMT,
  DISCOUNT_AMT AS DISCOUNT_AMT,
  RETURN_QTY AS RETURN_QTY,
  RETURN_AMT AS RETURN_AMT,
  NET_MARGIN_AMT AS NET_MARGIN_AMT,
  PURCH_COST_AMT AS PURCH_COST_AMT,
  SALES_COST AS SALES_COST,
  EXCH_RATE_PCT AS EXCH_RATE_PCT,
  NET_ORDER_SALES_DISC_AMT AS NET_ORDER_SALES_DISC_AMT,
  NET_ORDER_MARGIN_DISC_AMT AS NET_ORDER_MARGIN_DISC_AMT,
  NET_SALES_SHIP_AMT AS NET_SALES_SHIP_AMT,
  MARGIN_PRE_AMT AS MARGIN_PRE_AMT,
  MA_VF_AMT AS MA_VF_AMT,
  MA_VF_PRODUCT_AMT AS MA_VF_PRODUCT_AMT,
  MA_SALES_VF_AMT AS MA_SALES_VF_AMT,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  PICK_DECL_QTY AS PICK_DECL_QTY,
  ORDER_AGE AS ORDER_AGE,
  RX_TYPE AS RX_TYPE,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ORDER_LN_SALES_ETL_INS_VW_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_ORDER_LN_SALES_ETL_INS_VW_1")

# COMMAND ----------
# DBTITLE 1, exp_Timestamp_2


query_2 = f"""SELECT
  CREATED_TSTMP AS CREATED_TSTMP,
  OMS_ORDER_DT AS OMS_ORDER_DT,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_NBR AS OMS_ORDER_NBR,
  ORDER_NBR AS ORDER_NBR,
  ORDER_CHANNEL AS ORDER_CHANNEL,
  ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
  ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
  PRODUCT_ID AS PRODUCT_ID,
  SCHED_DELIVERY_FLG AS SCHED_DELIVERY_FLG,
  ADD_ON_FLAG AS ADD_ON_FLAG,
  CANCELLED_FLG AS CANCELLED_FLG,
  OMS_DO_TYPE_ID AS OMS_DO_TYPE_ID,
  SHIP_POSTAL_CD AS SHIP_POSTAL_CD,
  SHIP_COUNTRY_CD AS SHIP_COUNTRY_CD,
  SHIP_STATE AS SHIP_STATE,
  BILL_POSTAL_CD AS BILL_POSTAL_CD,
  BILL_COUNTRY_CD AS BILL_COUNTRY_CD,
  OMS_ORDER_STATUS_ID AS OMS_ORDER_STATUS_ID,
  OMS_ORDER_LN_STATUS_ID AS OMS_ORDER_LN_STATUS_ID,
  FULF_LOCATION_ID AS FULF_LOCATION_ID,
  FULF_LOCATION_NBR AS FULF_LOCATION_NBR,
  FULF_STORE_NAME AS FULF_STORE_NAME,
  FULF_LOCATION_TYPE AS FULF_LOCATION_TYPE,
  FULF_LOCATION_TYPE_DESC AS FULF_LOCATION_TYPE_DESC,
  FULF_LOC_GROUP_ID AS FULF_LOC_GROUP_ID,
  FULF_LOC_GROUP_DESC AS FULF_LOC_GROUP_DESC,
  CREATION_DEVICE_TYPE AS CREATION_DEVICE_TYPE,
  CREATION_DEVICE_WIDTH AS CREATION_DEVICE_WIDTH,
  SHIPPED_DT AS SHIPPED_DT,
  AGING_DAYS AS AGING_DAYS,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  CANCELLED_QTY AS CANCELLED_QTY,
  UNIT_MONETARY_VALUE_AMT AS UNIT_MONETARY_VALUE_AMT,
  ORIG_SHIP_CHARGE_AMT AS ORIG_SHIP_CHARGE_AMT,
  SHIP_CHARGE_AMT AS SHIP_CHARGE_AMT,
  HEADER_DISC_AMT AS HEADER_DISC_AMT,
  LN_DISC_AMT AS LN_DISC_AMT,
  UNIT_TAX_AMT AS UNIT_TAX_AMT,
  NET_SALES_QTY AS NET_SALES_QTY,
  NET_SALES_AMT AS NET_SALES_AMT,
  SALES_QTY AS SALES_QTY,
  SALES_AMT AS SALES_AMT,
  NET_SALES_TAX_AMT AS NET_SALES_TAX_AMT,
  DISCOUNT_AMT AS DISCOUNT_AMT,
  RETURN_QTY AS RETURN_QTY,
  RETURN_AMT AS RETURN_AMT,
  NET_MARGIN_AMT AS NET_MARGIN_AMT,
  PURCH_COST_AMT AS PURCH_COST_AMT,
  SALES_COST AS SALES_COST,
  EXCH_RATE_PCT AS EXCH_RATE_PCT,
  NET_ORDER_SALES_DISC_AMT AS NET_ORDER_SALES_DISC_AMT,
  NET_ORDER_MARGIN_DISC_AMT AS NET_ORDER_MARGIN_DISC_AMT,
  NET_SALES_SHIP_AMT AS NET_SALES_SHIP_AMT,
  MARGIN_PRE_AMT AS MARGIN_PRE_AMT,
  MA_VF_AMT AS MA_VF_AMT,
  MA_VF_PRODUCT_AMT AS MA_VF_PRODUCT_AMT,
  MA_SALES_VF_AMT AS MA_SALES_VF_AMT,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  PICK_DECL_QTY AS PICK_DECL_QTY,
  ORDER_AGE AS ORDER_AGE,
  RX_TYPE AS RX_TYPE,
  now() AS LOAD_TSTMP,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_ORDER_LN_SALES_ETL_INS_VW_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("exp_Timestamp_2")

# COMMAND ----------
# DBTITLE 1, OMS_ORDER_LN_SALES


spark.sql("""INSERT INTO
  OMS_ORDER_LN_SALES
SELECT
  CREATED_TSTMP AS CREATED_TSTMP,
  OMS_ORDER_DT AS OMS_ORDER_DT,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_NBR AS OMS_ORDER_NBR,
  ORDER_NBR AS ORDER_NBR,
  ORDER_CHANNEL AS ORDER_CHANNEL,
  ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
  ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
  PRODUCT_ID AS PRODUCT_ID,
  SCHED_DELIVERY_FLG AS SCHED_DELIVERY_FLG,
  ADD_ON_FLAG AS ADD_ON_FLAG,
  CANCELLED_FLG AS CANCELLED_FLG,
  OMS_DO_TYPE_ID AS OMS_DO_TYPE_ID,
  SHIP_POSTAL_CD AS SHIP_POSTAL_CD,
  SHIP_COUNTRY_CD AS SHIP_COUNTRY_CD,
  SHIP_STATE AS SHIP_STATE,
  BILL_POSTAL_CD AS BILL_POSTAL_CD,
  BILL_COUNTRY_CD AS BILL_COUNTRY_CD,
  OMS_ORDER_STATUS_ID AS OMS_ORDER_STATUS_ID,
  OMS_ORDER_LN_STATUS_ID AS OMS_ORDER_LN_STATUS_ID,
  FULF_LOCATION_ID AS FULF_LOCATION_ID,
  FULF_LOCATION_NBR AS FULF_LOCATION_NBR,
  FULF_STORE_NAME AS FULF_STORE_NAME,
  FULF_LOCATION_TYPE AS FULF_LOCATION_TYPE,
  FULF_LOCATION_TYPE_DESC AS FULF_LOCATION_TYPE_DESC,
  FULF_LOC_GROUP_ID AS FULF_LOC_GROUP_ID,
  FULF_LOC_GROUP_DESC AS FULF_LOC_GROUP_DESC,
  CREATION_DEVICE_TYPE AS CREATION_DEVICE_TYPE,
  CREATION_DEVICE_WIDTH AS CREATION_DEVICE_WIDTH,
  SHIPPED_DT AS SHIPPED_DT,
  AGING_DAYS AS AGING_DAYS,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  CANCELLED_QTY AS CANCELLED_QTY,
  UNIT_MONETARY_VALUE_AMT AS UNIT_MONETARY_VALUE_AMT,
  ORIG_SHIP_CHARGE_AMT AS ORIG_SHIP_CHARGE_AMT,
  SHIP_CHARGE_AMT AS SHIP_CHARGE_AMT,
  HEADER_DISC_AMT AS HEADER_DISC_AMT,
  LN_DISC_AMT AS LN_DISC_AMT,
  UNIT_TAX_AMT AS UNIT_TAX_AMT,
  NET_SALES_QTY AS NET_SALES_QTY,
  NET_SALES_AMT AS NET_SALES_AMT,
  SALES_QTY AS SALES_QTY,
  SALES_AMT AS SALES_AMT,
  NET_SALES_TAX_AMT AS NET_SALES_TAX_AMT,
  DISCOUNT_AMT AS DISCOUNT_AMT,
  RETURN_QTY AS RETURN_QTY,
  RETURN_AMT AS RETURN_AMT,
  NET_MARGIN_AMT AS NET_MARGIN_AMT,
  PURCH_COST_AMT AS PURCH_COST_AMT,
  SALES_COST AS SALES_COST,
  EXCH_RATE_PCT AS EXCH_RATE_PCT,
  NET_ORDER_SALES_DISC_AMT AS NET_ORDER_SALES_DISC_AMT,
  NET_ORDER_MARGIN_DISC_AMT AS NET_ORDER_MARGIN_DISC_AMT,
  NET_SALES_SHIP_AMT AS NET_SALES_SHIP_AMT,
  MARGIN_PRE_AMT AS MARGIN_PRE_AMT,
  MA_VF_AMT AS MA_VF_AMT,
  MA_VF_PRODUCT_AMT AS MA_VF_PRODUCT_AMT,
  MA_SALES_VF_AMT AS MA_SALES_VF_AMT,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  PICK_DECL_QTY AS PICK_DECL_QTY,
  ORDER_AGE AS ORDER_AGE,
  RX_TYPE AS RX_TYPE,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  exp_Timestamp_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_ORDER_LN_SALES")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_OMS_ORDER_LN_SALES", mainWorkflowId, parentName)
