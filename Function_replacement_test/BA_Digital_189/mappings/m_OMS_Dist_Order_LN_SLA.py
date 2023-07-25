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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_Dist_Order_LN_SLA")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_OMS_Dist_Order_LN_SLA", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_DIST_ORDER_LN_SLA1_0


query_0 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  OMS_DO_SLA_TSTMP AS OMS_DO_SLA_TSTMP,
  OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG,
  OMS_DO_AGE_1_TSTMP AS OMS_DO_AGE_1_TSTMP,
  OMS_DO_AGE_2_TSTMP AS OMS_DO_AGE_2_TSTMP,
  OMS_DO_AGE_3_TSTMP AS OMS_DO_AGE_3_TSTMP,
  OMS_DO_AGE_4_TSTMP AS OMS_DO_AGE_4_TSTMP,
  OMS_DO_AGE_5_TSTMP AS OMS_DO_AGE_5_TSTMP,
  TIME_ZONE AS TIME_ZONE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_DIST_ORDER_LN_SLA"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_DIST_ORDER_LN_SLA1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_DIST_ORDER_LN_SLA_1


query_1 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  OMS_DO_SLA_TSTMP AS OMS_DO_SLA_TSTMP,
  OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG,
  OMS_DO_AGE_1_TSTMP AS OMS_DO_AGE_1_TSTMP,
  OMS_DO_AGE_2_TSTMP AS OMS_DO_AGE_2_TSTMP,
  OMS_DO_AGE_3_TSTMP AS OMS_DO_AGE_3_TSTMP,
  OMS_DO_AGE_4_TSTMP AS OMS_DO_AGE_4_TSTMP,
  OMS_DO_AGE_5_TSTMP AS OMS_DO_AGE_5_TSTMP,
  TIME_ZONE AS TIME_ZONE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_DIST_ORDER_LN_SLA1_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_DIST_ORDER_LN_SLA_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_ORDER_SLA_DAY_2


query_2 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SITE_ORDER_SLA_DAY"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SITE_ORDER_SLA_DAY_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_ORDER_SLA_DAY_3


query_3 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_ORDER_SLA_DAY_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SITE_ORDER_SLA_DAY_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_HOURS_DAY_4


query_4 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  STORE_NBR AS STORE_NBR,
  CLOSE_FLAG AS CLOSE_FLAG,
  TIME_ZONE AS TIME_ZONE,
  OPEN_TSTMP AS OPEN_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SITE_HOURS_DAY"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_SITE_HOURS_DAY_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_HOURS_DAY_5


query_5 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  STORE_NBR AS STORE_NBR,
  CLOSE_FLAG AS CLOSE_FLAG,
  TIME_ZONE AS TIME_ZONE,
  OPEN_TSTMP AS OPEN_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_HOURS_DAY_4
ORDER BY
  DAY_DT"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("SQ_Shortcut_to_SITE_HOURS_DAY_5")

# COMMAND ----------
# DBTITLE 1, Fil_Site_Hours_Day_6


query_6 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  STORE_NBR AS STORE_NBR,
  CLOSE_FLAG AS CLOSE_FLAG,
  TIME_ZONE AS TIME_ZONE,
  OPEN_TSTMP AS OPEN_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SITE_HOURS_DAY_5
WHERE
  (
    BUSINESS_AREA = 'Store'
    OR BUSINESS_AREA = 'DC'
    OR BUSINESS_AREA = 'Vendor'
  )
  AND CLOSE_FLAG = 0"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Fil_Site_Hours_Day_6")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_DIST_ORDER_LN_VW_7


query_7 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  TOTAL_MONETARY_VALUE_AMT AS TOTAL_MONETARY_VALUE_AMT,
  UNIT_MONETARY_VALUE_AMT AS UNIT_MONETARY_VALUE_AMT,
  UNIT_TAX_AMT AS UNIT_TAX_AMT,
  MV_CURRENCY_CD AS MV_CURRENCY_CD,
  SHIPPED_QTY AS SHIPPED_QTY,
  RECEIVED_QTY AS RECEIVED_QTY,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_TSTMP AS CREATED_TSTMP,
  CREATED_ORIG_TSTMP AS CREATED_ORIG_TSTMP,
  LAST_UPDATED_TSTMP AS LAST_UPDATED_TSTMP,
  OMS_DIST_ORDER_LN_STATUS_ID AS OMS_DIST_ORDER_LN_STATUS_ID,
  ALLOCATED_QTY AS ALLOCATED_QTY,
  UNIT_COST_AMT AS UNIT_COST_AMT,
  UNIT_PRICE_AMT AS UNIT_PRICE_AMT,
  USER_CANCELED_QTY AS USER_CANCELED_QTY,
  DELIVERY_END_DT AS DELIVERY_END_DT,
  DELIVERY_START_DT AS DELIVERY_START_DT,
  EVENT_CD AS EVENT_CD,
  REASON_CODE AS REASON_CODE,
  PARTL_FILL_FLG AS PARTL_FILL_FLG,
  ORDER_QTY AS ORDER_QTY,
  ORIG_ORDER_QTY AS ORIG_ORDER_QTY,
  RETAIL_PRICE AS RETAIL_PRICE,
  OMS_DIST_ORDER_LN_NBR AS OMS_DIST_ORDER_LN_NBR,
  PICKUP_END_DTTM AS PICKUP_END_DTTM,
  OMS_ORDER_LN_NBR AS OMS_ORDER_LN_NBR,
  PICKUP_START_DTTM AS PICKUP_START_DTTM,
  CANCELLED_FLG AS CANCELLED_FLG,
  PRODUCT_ID AS PRODUCT_ID,
  OMS_ORDER_NBR AS OMS_ORDER_NBR,
  FREIGHT_REVENUE_CURRENCY_CD AS FREIGHT_REVENUE_CURRENCY_CD,
  FREIGHT_REVENUE AS FREIGHT_REVENUE,
  ADJUSTED_ORDER_QTY AS ADJUSTED_ORDER_QTY,
  EV_RELEASED_TSTMP AS EV_RELEASED_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_ALLOCATED_TSTMP AS EV_ALLOCATED_TSTMP,
  EV_ALLOCATED_ORIG_TSTMP AS EV_ALLOCATED_ORIG_TSTMP,
  EV_SHIPPED_TSTMP AS EV_SHIPPED_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  EV_PICKEDUP_TSTMP AS EV_PICKEDUP_TSTMP,
  EV_PICKEDUP_ORIG_TSTMP AS EV_PICKEDUP_ORIG_TSTMP,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_TSTMP AS OMS_DO_CREATED_TSTMP,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  ORDER_NBR AS ORDER_NBR,
  OMS_ORDER_CREATED_TSTMP AS OMS_ORDER_CREATED_TSTMP,
  OMS_ORDER_CREATED_ORIG_TSTMP AS OMS_ORDER_CREATED_ORIG_TSTMP,
  ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  ORDER_CREATION_CHANNEL AS ORDER_CREATION_CHANNEL,
  ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
  ORDER_CHANNEL AS ORDER_CHANNEL,
  SCHED_DELIVERY_FLG AS SCHED_DELIVERY_FLG,
  SUBSCRIPTION_ORDER_FLG AS SUBSCRIPTION_ORDER_FLG,
  ADD_ON_FLAG AS ADD_ON_FLAG,
  ISPU_PXY_FIRST_NAME AS ISPU_PXY_FIRST_NAME,
  ISPU_PXY_LAST_NAME AS ISPU_PXY_LAST_NAME,
  ISPU_PXY_ADD_LINE1 AS ISPU_PXY_ADD_LINE1,
  ISPU_PXY_ADD_LINE2 AS ISPU_PXY_ADD_LINE2,
  ISPU_PXY_ADD_LINE3 AS ISPU_PXY_ADD_LINE3,
  ISPU_PXY_CITY AS ISPU_PXY_CITY,
  ISPU_PXY_STATE AS ISPU_PXY_STATE,
  ISPU_PXY_POSTAL_CD AS ISPU_PXY_POSTAL_CD,
  ISPU_PXY_COUNTRY AS ISPU_PXY_COUNTRY,
  ISPU_PXY_EMAIL AS ISPU_PXY_EMAIL,
  ISUP_PXY_PHONE AS ISUP_PXY_PHONE,
  OMS_COMPANY_ID AS OMS_COMPANY_ID,
  EXCHANGE_RATE_PCNT AS EXCHANGE_RATE_PCNT,
  CANCEL_TSTMP AS CANCEL_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_DIST_ORDER_LN_VW"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Shortcut_to_OMS_DIST_ORDER_LN_VW_7")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_DIST_ORDER_LN_VW_8


query_8 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  OMS_ORDER_CREATED_ORIG_TSTMP AS OMS_ORDER_CREATED_ORIG_TSTMP,
  ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_DIST_ORDER_LN_VW_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("SQ_Shortcut_to_OMS_DIST_ORDER_LN_VW_8")

# COMMAND ----------
# DBTITLE 1, JNR_SITE_ORD_SLA_DY_OMS_DIST_VW_9


query_9 = f"""SELECT
  DETAIL.OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  DETAIL.OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  DETAIL.OMS_ORDER_ID AS OMS_ORDER_ID,
  DETAIL.OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  DETAIL.EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  DETAIL.EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  DETAIL.OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  DETAIL.OMS_ORDER_CREATED_ORIG_TSTMP AS OMS_ORDER_CREATED_ORIG_TSTMP,
  DETAIL.ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  DETAIL.UPDATE_TSTMP AS UPDATE_TSTMP,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  DETAIL.ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
  MASTER.LOCATION_ID AS LOCATION_ID,
  MASTER.START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  MASTER.END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  MASTER.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  MASTER.SLA_DAY_DT AS SLA_DAY_DT,
  MASTER.SLA_TSTMP AS SLA_TSTMP,
  MASTER.SLA_TIME_HOUR AS SLA_TIME_HOUR,
  MASTER.UPDATE_TSTMP AS UPDATE_TSTMP1,
  MASTER.LOAD_TSTMP AS LOAD_TSTMP1,
  MASTER.DAY_DT AS DAY_DT,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SITE_ORDER_SLA_DAY_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_DIST_ORDER_LN_VW_8 DETAIL ON MASTER.LOCATION_ID = DETAIL.ORIG_LOCATION_ID"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("JNR_SITE_ORD_SLA_DY_OMS_DIST_VW_9")

# COMMAND ----------
# DBTITLE 1, FIL_OMS_DIST_SIT_ORD_SLA_DY_10


query_10 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  OMS_ORDER_CREATED_ORIG_TSTMP AS OMS_ORDER_CREATED_ORIG_TSTMP,
  ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  ORDER_FULFILLMENT_CHANNEL AS ORDER_FULFILLMENT_CHANNEL,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP1 AS UPDATE_TSTMP1,
  LOAD_TSTMP1 AS LOAD_TSTMP1,
  DAY_DT AS DAY_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_SITE_ORD_SLA_DY_OMS_DIST_VW_9
WHERE
  (
    UPDATE_TSTMP >= TRUNC(now())
    OR UPDATE_TSTMP1 >= TRUNC(now())
  )
  AND IFF(
    LOCATION_TYPE_ID = 8,
    OMS_DO_CREATED_ORIG_TSTMP,
    IFF(
      ISNULL(EV_RELEASED_ORIG_TSTMP)
      AND NOT ISNULL(EV_SHIPPED_ORIG_TSTMP),
      OMS_DO_CREATED_ORIG_TSTMP,
      EV_RELEASED_ORIG_TSTMP
    )
  ) >= START_ORDER_CREATE_TSTMP
  AND IFF(
    LOCATION_TYPE_ID = 8,
    OMS_DO_CREATED_ORIG_TSTMP,
    IFF(
      ISNULL(EV_RELEASED_ORIG_TSTMP)
      AND NOT ISNULL(EV_SHIPPED_ORIG_TSTMP),
      OMS_DO_CREATED_ORIG_TSTMP,
      EV_RELEASED_ORIG_TSTMP
    )
  ) <= END_ORDER_CREATE_TSTMP
  AND ORDER_FULFILLMENT_CHANNEL <> 'SFS'"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("FIL_OMS_DIST_SIT_ORD_SLA_DY_10")

# COMMAND ----------
# DBTITLE 1, EXP_SLA_LOGIC_11


query_11 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  IFF(
    LOCATION_TYPE_ID = 8,
    IFF(
      NOT ISNULL(SLA_TIME_HOUR),
      ADD_TO_DATE(OMS_DO_CREATED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR),
      SLA_TSTMP
    ),
    IFF(
      NOT ISNULL(SLA_TIME_HOUR),
      IFF (
        ISNULL(EV_RELEASED_ORIG_TSTMP)
        AND NOT ISNULL (EV_SHIPPED_ORIG_TSTMP),
        ADD_TO_DATE(OMS_DO_CREATED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR),
        ADD_TO_DATE(EV_RELEASED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR)
      ),
      SLA_TSTMP
    )
  ) AS v_OMS_DO_SLA_TSTMP,
  IFF(
    LOCATION_TYPE_ID = 8,
    IFF(
      NOT ISNULL(SLA_TIME_HOUR),
      ADD_TO_DATE(OMS_DO_CREATED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR),
      SLA_TSTMP
    ),
    IFF(
      NOT ISNULL(SLA_TIME_HOUR),
      IFF (
        ISNULL(EV_RELEASED_ORIG_TSTMP)
        AND NOT ISNULL (EV_SHIPPED_ORIG_TSTMP),
        ADD_TO_DATE(OMS_DO_CREATED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR),
        ADD_TO_DATE(EV_RELEASED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR)
      ),
      SLA_TSTMP
    )
  ) AS o_OMS_DO_SLA_TSTMP,
  DECODE(
    TRUE,
    ISNULL(EV_SHIPPED_ORIG_TSTMP),
    NULL,
    (EV_SHIPPED_ORIG_TSTMP > v_OMS_DO_SLA_TSTMP),
    0,
    (EV_SHIPPED_ORIG_TSTMP <= v_OMS_DO_SLA_TSTMP),
    1
  ) AS v_OMS_DO_SLA_FLAG,
  DECODE(
    TRUE,
    ISNULL(EV_SHIPPED_ORIG_TSTMP),
    NULL,
    (
      EV_SHIPPED_ORIG_TSTMP > IFF(
        LOCATION_TYPE_ID = 8,
        IFF(
          NOT ISNULL(SLA_TIME_HOUR),
          ADD_TO_DATE(OMS_DO_CREATED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR),
          SLA_TSTMP
        ),
        IFF(
          NOT ISNULL(SLA_TIME_HOUR),
          IFF (
            ISNULL(EV_RELEASED_ORIG_TSTMP)
            AND NOT ISNULL (EV_SHIPPED_ORIG_TSTMP),
            ADD_TO_DATE(OMS_DO_CREATED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR),
            ADD_TO_DATE(EV_RELEASED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR)
          ),
          SLA_TSTMP
        )
      )
    ),
    0,
    (
      EV_SHIPPED_ORIG_TSTMP <= IFF(
        LOCATION_TYPE_ID = 8,
        IFF(
          NOT ISNULL(SLA_TIME_HOUR),
          ADD_TO_DATE(OMS_DO_CREATED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR),
          SLA_TSTMP
        ),
        IFF(
          NOT ISNULL(SLA_TIME_HOUR),
          IFF (
            ISNULL(EV_RELEASED_ORIG_TSTMP)
            AND NOT ISNULL (EV_SHIPPED_ORIG_TSTMP),
            ADD_TO_DATE(OMS_DO_CREATED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR),
            ADD_TO_DATE(EV_RELEASED_ORIG_TSTMP, 'HH', SLA_TIME_HOUR)
          ),
          SLA_TSTMP
        )
      )
    ),
    1
  ) AS o_OMS_DO_SLA_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_OMS_DIST_SIT_ORD_SLA_DY_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("EXP_SLA_LOGIC_11")

# COMMAND ----------
# DBTITLE 1, jnr_SITE_HOURS_DAY_12


query_12 = f"""SELECT
  DETAIL.OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  DETAIL.OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  DETAIL.OMS_ORDER_ID AS OMS_ORDER_ID,
  DETAIL.OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  DETAIL.OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  DETAIL.EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  DETAIL.EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  DETAIL.ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  DETAIL.START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  DETAIL.END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  DETAIL.SLA_TIME_HOUR AS SLA_TIME_HOUR,
  DETAIL.SLA_TSTMP AS SLA_TSTMP,
  DETAIL.o_OMS_DO_SLA_TSTMP AS o_OMS_DO_SLA_TSTMP,
  DETAIL.o_OMS_DO_SLA_FLAG AS o_OMS_DO_SLA_FLAG,
  MASTER.DAY_DT AS DAY_DT,
  MASTER.LOCATION_ID AS LOCATION_ID,
  MASTER.BUSINESS_AREA AS BUSINESS_AREA,
  MASTER.LOCATION_TYPE_ID AS LOCATION_TYPE_ID1,
  MASTER.STORE_NBR AS STORE_NBR,
  MASTER.CLOSE_FLAG AS CLOSE_FLAG,
  MASTER.TIME_ZONE AS TIME_ZONE,
  MASTER.OPEN_TSTMP AS OPEN_TSTMP,
  MASTER.CLOSE_TSTMP AS CLOSE_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_Site_Hours_Day_6 MASTER
  RIGHT JOIN EXP_SLA_LOGIC_11 DETAIL ON MASTER.LOCATION_ID = DETAIL.ORIG_LOCATION_ID"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("jnr_SITE_HOURS_DAY_12")

# COMMAND ----------
# DBTITLE 1, Fil_Day_Dt_13


query_13 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  SLA_TSTMP AS SLA_TSTMP,
  o_OMS_DO_SLA_TSTMP AS o_OMS_DO_SLA_TSTMP,
  o_OMS_DO_SLA_FLAG AS o_OMS_DO_SLA_FLAG,
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  LOCATION_TYPE_ID1 AS LOCATION_TYPE_ID1,
  STORE_NBR AS STORE_NBR,
  CLOSE_FLAG AS CLOSE_FLAG,
  TIME_ZONE AS TIME_ZONE,
  OPEN_TSTMP AS OPEN_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  NULL AS UPDATE_TSTMP,
  NULL AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  jnr_SITE_HOURS_DAY_12
WHERE
  DAY_DT > o_OMS_DO_SLA_TSTMP"""

df_13 = spark.sql(query_13)

df_13.createOrReplaceTempView("Fil_Day_Dt_13")

# COMMAND ----------
# DBTITLE 1, exp_CALC_14


query_14 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  SLA_TSTMP AS SLA_TSTMP,
  o_OMS_DO_SLA_TSTMP AS o_OMS_DO_SLA_TSTMP,
  o_OMS_DO_SLA_FLAG AS o_OMS_DO_SLA_FLAG,
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  LOCATION_TYPE_ID1 AS LOCATION_TYPE_ID1,
  STORE_NBR AS STORE_NBR,
  CLOSE_FLAG AS CLOSE_FLAG,
  TIME_ZONE AS TIME_ZONE,
  OPEN_TSTMP AS OPEN_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  To_date(
    to_char(DAY_DT, 'yyyy-mm-dd') || ' ' || to_char(get_date_part(o_OMS_DO_SLA_TSTMP, 'HH24')) || ':' || to_char(get_date_part(o_OMS_DO_SLA_TSTMP, 'MI')) || ':' || to_char(get_date_part(o_OMS_DO_SLA_TSTMP, 'SS')),
    'yyyy-mm-dd hh24:mi:ss'
  ) AS DAY_DT1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_Day_Dt_13"""

df_14 = spark.sql(query_14)

df_14.createOrReplaceTempView("exp_CALC_14")

# COMMAND ----------
# DBTITLE 1, Srt_Oms_Dist_Order_Ln_15


query_15 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  ORIG_LOCATION_ID AS ORIG_LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  SLA_TSTMP AS SLA_TSTMP,
  o_OMS_DO_SLA_TSTMP AS o_OMS_DO_SLA_TSTMP,
  o_OMS_DO_SLA_FLAG AS o_OMS_DO_SLA_FLAG,
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  LOCATION_TYPE_ID1 AS LOCATION_TYPE_ID1,
  STORE_NBR AS STORE_NBR,
  CLOSE_FLAG AS CLOSE_FLAG,
  TIME_ZONE AS TIME_ZONE,
  OPEN_TSTMP AS OPEN_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  DAY_DT1 AS DAY_DT1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  exp_CALC_14
ORDER BY
  OMS_DIST_ORDER_ID ASC,
  OMS_DIST_ORDER_LN_ID ASC"""

df_15 = spark.sql(query_15)

df_15.createOrReplaceTempView("Srt_Oms_Dist_Order_Ln_15")

# COMMAND ----------
# DBTITLE 1, rnk_NEXT_FIVE_OPEN_DAY_16


query_16 = f"""SELECT
  *
FROM
  (
    SELECT
      RANK()() OVER(
        PARTITION BY
          OMS_DIST_ORDER_ID,
          OMS_DIST_ORDER_LN_ID
        ORDER BY
          DAY_DT1 DESC
      ) AS RANKINDEX,
      OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
      OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
      OMS_ORDER_ID AS OMS_ORDER_ID,
      OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
      OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
      EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
      EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
      o_OMS_DO_SLA_TSTMP AS OMS_DO_SLA_TSTMP,
      o_OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG,
      TIME_ZONE AS TIME_ZONE,
      DAY_DT1 AS DAY_DT1,
      Monotonically_Increasing_Id AS Monotonically_Increasing_IdFROM Srt_Oms_Dist_Order_Ln_15
  ) AS rnk_NEXT_FIVE_OPEN_DAY
WHERE
  RANKINDEX <= 5"""

df_16 = spark.sql(query_16)

df_16.createOrReplaceTempView("rnk_NEXT_FIVE_OPEN_DAY_16")

# COMMAND ----------
# DBTITLE 1, agg_OPEN_DAY_TRANSPOSE_17


query_17 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  OMS_DO_SLA_TSTMP AS OMS_DO_SLA_TSTMP,
  OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG,
  TIME_ZONE AS TIME_ZONE,
  MAX(IFF(RANKINDEX = 1, DAY_DT1)) AS Aging_1,
  MAX(IFF(RANKINDEX = 2, DAY_DT1)) AS Aging_2,
  MAX(IFF(RANKINDEX = 3, DAY_DT1)) AS Aging_3,
  MAX(IFF(RANKINDEX = 4, DAY_DT1)) AS Aging_4,
  MAX(IFF(RANKINDEX = 5, DAY_DT1)) AS Aging_5,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  rnk_NEXT_FIVE_OPEN_DAY_16
GROUP BY
  OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID"""

df_17 = spark.sql(query_17)

df_17.createOrReplaceTempView("agg_OPEN_DAY_TRANSPOSE_17")

# COMMAND ----------
# DBTITLE 1, jnr_OMS_DIST_ORDER_LN_SLA_18


query_18 = f"""SELECT
  MASTER.OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  MASTER.OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  MASTER.OMS_ORDER_ID AS OMS_ORDER_ID,
  MASTER.OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  MASTER.OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  MASTER.EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  MASTER.EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  MASTER.OMS_DO_SLA_TSTMP AS OMS_DO_SLA_TSTMP,
  MASTER.OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG,
  MASTER.OMS_DO_AGE_1_TSTMP AS OMS_DO_AGE_1_TSTMP,
  MASTER.OMS_DO_AGE_2_TSTMP AS OMS_DO_AGE_2_TSTMP,
  MASTER.OMS_DO_AGE_3_TSTMP AS OMS_DO_AGE_3_TSTMP,
  MASTER.OMS_DO_AGE_4_TSTMP AS OMS_DO_AGE_4_TSTMP,
  MASTER.OMS_DO_AGE_5_TSTMP AS OMS_DO_AGE_5_TSTMP,
  MASTER.TIME_ZONE AS TIME_ZONE,
  MASTER.LOAD_TSTMP AS LOAD_TSTMP,
  DETAIL.OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID1,
  DETAIL.OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID1,
  DETAIL.OMS_ORDER_ID AS OMS_ORDER_ID1,
  DETAIL.OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID1,
  DETAIL.OMS_DO_SLA_TSTMP AS OMS_DO_SLA_TSTMP1,
  DETAIL.OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG1,
  DETAIL.EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP1,
  DETAIL.EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP1,
  DETAIL.OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP1,
  DETAIL.TIME_ZONE AS TIME_ZONE1,
  DETAIL.Aging_1 AS Aging_1,
  DETAIL.Aging_2 AS Aging_2,
  DETAIL.Aging_3 AS Aging_3,
  DETAIL.Aging_4 AS Aging_4,
  DETAIL.Aging_5 AS Aging_5,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_DIST_ORDER_LN_SLA_1 MASTER
  RIGHT JOIN agg_OPEN_DAY_TRANSPOSE_17 DETAIL ON MASTER.OMS_DIST_ORDER_ID = DETAIL.OMS_DIST_ORDER_ID
  AND MASTER.OMS_DIST_ORDER_LN_ID = DETAIL.OMS_DIST_ORDER_LN_ID"""

df_18 = spark.sql(query_18)

df_18.createOrReplaceTempView("jnr_OMS_DIST_ORDER_LN_SLA_18")

# COMMAND ----------
# DBTITLE 1, exp_UPD_FLAG_19


query_19 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  OMS_DO_SLA_TSTMP AS OMS_DO_SLA_TSTMP,
  OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG,
  OMS_DO_AGE_1_TSTMP AS OMS_DO_AGE_1_TSTMP,
  OMS_DO_AGE_2_TSTMP AS OMS_DO_AGE_2_TSTMP,
  OMS_DO_AGE_3_TSTMP AS OMS_DO_AGE_3_TSTMP,
  OMS_DO_AGE_4_TSTMP AS OMS_DO_AGE_4_TSTMP,
  OMS_DO_AGE_5_TSTMP AS OMS_DO_AGE_5_TSTMP,
  TIME_ZONE AS TIME_ZONE,
  LOAD_TSTMP AS LOAD_TSTMP1,
  OMS_DIST_ORDER_ID1 AS OMS_DIST_ORDER_ID1,
  OMS_DIST_ORDER_LN_ID1 AS OMS_DIST_ORDER_LN_ID1,
  OMS_ORDER_ID1 AS OMS_ORDER_ID1,
  OMS_ORDER_LN_ID1 AS OMS_ORDER_LN_ID1,
  OMS_DO_SLA_TSTMP1 AS o_OMS_DO_SLA_TSTMP,
  OMS_DO_SLA_FLAG1 AS o_OMS_DO_SLA_FLAG,
  EV_RELEASED_ORIG_TSTMP1 AS EV_RELEASED_ORIG_TSTMP1,
  EV_SHIPPED_ORIG_TSTMP1 AS EV_SHIPPED_ORIG_TSTMP1,
  OMS_DO_CREATED_ORIG_TSTMP1 AS OMS_DO_CREATED_ORIG_TSTMP1,
  TIME_ZONE1 AS TIME_ZONE1,
  Aging_1 AS Aging_1,
  Aging_2 AS Aging_2,
  Aging_3 AS Aging_3,
  Aging_4 AS Aging_4,
  Aging_5 AS Aging_5,
  SYSTIMESTAMP() AS UPDATE_TSTMP,
  IFF(ISNULL(LOAD_TSTMP), SYSTIMESTAMP(), LOAD_TSTMP) AS LOAD_TSTMP,
  IFF(
    ISNULL(OMS_DIST_ORDER_ID),
    'I',
    IFF(
      IFF(ISNULL(OMS_ORDER_ID), -1, OMS_ORDER_ID) <> IFF(ISNULL(OMS_ORDER_ID1), -1, OMS_ORDER_ID1)
      OR IFF(ISNULL(OMS_ORDER_LN_ID), -1, OMS_ORDER_LN_ID) <> IFF(ISNULL(OMS_ORDER_LN_ID1), -1, OMS_ORDER_LN_ID1)
      OR IFF(ISNULL(TIME_ZONE), ' ', TIME_ZONE) <> IFF(ISNULL(TIME_ZONE1), ' ', TIME_ZONE1)
      OR IFF(
        ISNULL(OMS_DO_CREATED_ORIG_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_CREATED_ORIG_TSTMP
      ) <> IFF(
        ISNULL(OMS_DO_CREATED_ORIG_TSTMP1),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_CREATED_ORIG_TSTMP1
      )
      OR IFF(
        ISNULL(EV_RELEASED_ORIG_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        EV_RELEASED_ORIG_TSTMP
      ) <> IFF(
        ISNULL(EV_RELEASED_ORIG_TSTMP1),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        EV_RELEASED_ORIG_TSTMP1
      )
      OR IFF(
        ISNULL(EV_SHIPPED_ORIG_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        EV_SHIPPED_ORIG_TSTMP
      ) <> IFF(
        ISNULL(EV_SHIPPED_ORIG_TSTMP1),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        EV_SHIPPED_ORIG_TSTMP1
      )
      OR IFF(
        ISNULL(OMS_DO_SLA_TSTMP1),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_SLA_TSTMP1
      ) <> IFF(
        ISNULL(OMS_DO_SLA_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_SLA_TSTMP
      )
      OR IFF(ISNULL(OMS_DO_SLA_FLAG1), 1, OMS_DO_SLA_FLAG1) <> IFF(ISNULL(OMS_DO_SLA_FLAG), 1, OMS_DO_SLA_FLAG)
      OR IFF(
        ISNULL(Aging_1),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        Aging_1
      ) <> IFF(
        ISNULL(OMS_DO_AGE_1_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_AGE_1_TSTMP
      )
      OR IFF(
        ISNULL(Aging_2),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        Aging_2
      ) <> IFF(
        ISNULL(OMS_DO_AGE_2_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_AGE_2_TSTMP
      )
      OR IFF(
        ISNULL(Aging_3),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        Aging_3
      ) <> IFF(
        ISNULL(OMS_DO_AGE_3_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_AGE_3_TSTMP
      )
      OR IFF(
        ISNULL(Aging_4),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        Aging_4
      ) <> IFF(
        ISNULL(OMS_DO_AGE_4_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_AGE_4_TSTMP
      )
      OR IFF(
        ISNULL(Aging_5),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        Aging_5
      ) <> IFF(
        ISNULL(OMS_DO_AGE_5_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        OMS_DO_AGE_5_TSTMP
      ),
      'U',
      'R'
    )
  ) AS UPD_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  jnr_OMS_DIST_ORDER_LN_SLA_18"""

df_19 = spark.sql(query_19)

df_19.createOrReplaceTempView("exp_UPD_FLAG_19")

# COMMAND ----------
# DBTITLE 1, FILTRANS_20


query_20 = f"""SELECT
  OMS_DIST_ORDER_ID1 AS OMS_DIST_ORDER_ID1,
  OMS_DIST_ORDER_LN_ID1 AS OMS_DIST_ORDER_LN_ID1,
  OMS_ORDER_ID1 AS OMS_ORDER_ID1,
  OMS_ORDER_LN_ID1 AS OMS_ORDER_LN_ID1,
  o_OMS_DO_SLA_TSTMP AS o_OMS_DO_SLA_TSTMP,
  o_OMS_DO_SLA_FLAG AS o_OMS_DO_SLA_FLAG,
  EV_RELEASED_ORIG_TSTMP1 AS EV_RELEASED_ORIG_TSTMP1,
  EV_SHIPPED_ORIG_TSTMP1 AS EV_SHIPPED_ORIG_TSTMP1,
  OMS_DO_CREATED_ORIG_TSTMP1 AS OMS_DO_CREATED_ORIG_TSTMP1,
  TIME_ZONE1 AS TIME_ZONE1,
  Aging_1 AS Aging_1,
  Aging_2 AS Aging_2,
  Aging_3 AS Aging_3,
  Aging_4 AS Aging_4,
  Aging_5 AS Aging_5,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  UPD_FLAG AS UPD_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  exp_UPD_FLAG_19
WHERE
  UPD_FLAG = 'I'
  OR UPD_FLAG = 'U'"""

df_20 = spark.sql(query_20)

df_20.createOrReplaceTempView("FILTRANS_20")

# COMMAND ----------
# DBTITLE 1, upd_UPDATE_21


query_21 = f"""SELECT
  OMS_DIST_ORDER_ID1 AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID1 AS OMS_DIST_ORDER_LN_ID1,
  OMS_ORDER_ID1 AS OMS_ORDER_ID1,
  OMS_ORDER_LN_ID1 AS OMS_ORDER_LN_ID1,
  o_OMS_DO_SLA_TSTMP AS o_OMS_DO_SLA_TSTMP,
  o_OMS_DO_SLA_FLAG AS o_OMS_DO_SLA_FLAG,
  EV_RELEASED_ORIG_TSTMP1 AS EV_RELEASED_ORIG_TSTMP1,
  EV_SHIPPED_ORIG_TSTMP1 AS EV_SHIPPED_ORIG_TSTMP1,
  OMS_DO_CREATED_ORIG_TSTMP1 AS OMS_DO_CREATED_ORIG_TSTMP1,
  TIME_ZONE1 AS TIME_ZONE1,
  Aging_1 AS Aging_1,
  Aging_2 AS Aging_2,
  Aging_3 AS Aging_3,
  Aging_4 AS Aging_4,
  Aging_5 AS Aging_5,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  UPD_FLAG AS UPD_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    UPD_FLAG = 'I',
    'DD_INSERT',
    IFF(UPD_FLAG = 'U', 'DD_UPDATE')
  ) AS UPDATE_STRATEGY_FLAG
FROM
  FILTRANS_20"""

df_21 = spark.sql(query_21)

df_21.createOrReplaceTempView("upd_UPDATE_21")

# COMMAND ----------
# DBTITLE 1, OMS_DIST_ORDER_LN_SLA


spark.sql("""MERGE INTO OMS_DIST_ORDER_LN_SLA AS TARGET
USING
  upd_UPDATE_21 AS SOURCE ON TARGET.OMS_DIST_ORDER_LN_ID = SOURCE.OMS_DIST_ORDER_LN_ID1
  AND TARGET.OMS_DIST_ORDER_ID = SOURCE.OMS_DIST_ORDER_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_DIST_ORDER_ID = SOURCE.OMS_DIST_ORDER_ID,
  TARGET.OMS_DIST_ORDER_LN_ID = SOURCE.OMS_DIST_ORDER_LN_ID1,
  TARGET.OMS_ORDER_ID = SOURCE.OMS_ORDER_ID1,
  TARGET.OMS_ORDER_LN_ID = SOURCE.OMS_ORDER_LN_ID1,
  TARGET.OMS_DO_CREATED_ORIG_TSTMP = SOURCE.OMS_DO_CREATED_ORIG_TSTMP1,
  TARGET.EV_RELEASED_ORIG_TSTMP = SOURCE.EV_RELEASED_ORIG_TSTMP1,
  TARGET.EV_SHIPPED_ORIG_TSTMP = SOURCE.EV_SHIPPED_ORIG_TSTMP1,
  TARGET.OMS_DO_SLA_TSTMP = SOURCE.o_OMS_DO_SLA_TSTMP,
  TARGET.OMS_DO_SLA_FLAG = SOURCE.o_OMS_DO_SLA_FLAG,
  TARGET.OMS_DO_AGE_1_TSTMP = SOURCE.Aging_1,
  TARGET.OMS_DO_AGE_2_TSTMP = SOURCE.Aging_2,
  TARGET.OMS_DO_AGE_3_TSTMP = SOURCE.Aging_3,
  TARGET.OMS_DO_AGE_4_TSTMP = SOURCE.Aging_4,
  TARGET.OMS_DO_AGE_5_TSTMP = SOURCE.Aging_5,
  TARGET.TIME_ZONE = SOURCE.TIME_ZONE1,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_ORDER_ID = SOURCE.OMS_ORDER_ID1
  AND TARGET.OMS_ORDER_LN_ID = SOURCE.OMS_ORDER_LN_ID1
  AND TARGET.OMS_DO_CREATED_ORIG_TSTMP = SOURCE.OMS_DO_CREATED_ORIG_TSTMP1
  AND TARGET.EV_RELEASED_ORIG_TSTMP = SOURCE.EV_RELEASED_ORIG_TSTMP1
  AND TARGET.EV_SHIPPED_ORIG_TSTMP = SOURCE.EV_SHIPPED_ORIG_TSTMP1
  AND TARGET.OMS_DO_SLA_TSTMP = SOURCE.o_OMS_DO_SLA_TSTMP
  AND TARGET.OMS_DO_SLA_FLAG = SOURCE.o_OMS_DO_SLA_FLAG
  AND TARGET.OMS_DO_AGE_1_TSTMP = SOURCE.Aging_1
  AND TARGET.OMS_DO_AGE_2_TSTMP = SOURCE.Aging_2
  AND TARGET.OMS_DO_AGE_3_TSTMP = SOURCE.Aging_3
  AND TARGET.OMS_DO_AGE_4_TSTMP = SOURCE.Aging_4
  AND TARGET.OMS_DO_AGE_5_TSTMP = SOURCE.Aging_5
  AND TARGET.TIME_ZONE = SOURCE.TIME_ZONE1
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_DIST_ORDER_ID,
    TARGET.OMS_DIST_ORDER_LN_ID,
    TARGET.OMS_ORDER_ID,
    TARGET.OMS_ORDER_LN_ID,
    TARGET.OMS_DO_CREATED_ORIG_TSTMP,
    TARGET.EV_RELEASED_ORIG_TSTMP,
    TARGET.EV_SHIPPED_ORIG_TSTMP,
    TARGET.OMS_DO_SLA_TSTMP,
    TARGET.OMS_DO_SLA_FLAG,
    TARGET.OMS_DO_AGE_1_TSTMP,
    TARGET.OMS_DO_AGE_2_TSTMP,
    TARGET.OMS_DO_AGE_3_TSTMP,
    TARGET.OMS_DO_AGE_4_TSTMP,
    TARGET.OMS_DO_AGE_5_TSTMP,
    TARGET.TIME_ZONE,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.OMS_DIST_ORDER_ID,
    SOURCE.OMS_DIST_ORDER_LN_ID1,
    SOURCE.OMS_ORDER_ID1,
    SOURCE.OMS_ORDER_LN_ID1,
    SOURCE.OMS_DO_CREATED_ORIG_TSTMP1,
    SOURCE.EV_RELEASED_ORIG_TSTMP1,
    SOURCE.EV_SHIPPED_ORIG_TSTMP1,
    SOURCE.o_OMS_DO_SLA_TSTMP,
    SOURCE.o_OMS_DO_SLA_FLAG,
    SOURCE.Aging_1,
    SOURCE.Aging_2,
    SOURCE.Aging_3,
    SOURCE.Aging_4,
    SOURCE.Aging_5,
    SOURCE.TIME_ZONE1,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_Dist_Order_LN_SLA")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_OMS_Dist_Order_LN_SLA", mainWorkflowId, parentName)