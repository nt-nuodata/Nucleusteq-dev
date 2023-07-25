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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Payment_Detail")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_A_Payment_Detail", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_PAYMENT_DETAIL_PRE_0


query_0 = f"""SELECT
  PAYMENT_DETAIL_ID AS PAYMENT_DETAIL_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_NUMBER AS ENTITY_NUMBER,
  CARD_NUMBER AS CARD_NUMBER,
  BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
  BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
  BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
  BILL_TO_ADDRESS_LINE1 AS BILL_TO_ADDRESS_LINE1,
  BILL_TO_ADDRESS_LINE2 AS BILL_TO_ADDRESS_LINE2,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILLTO_STATE_PROV AS BILLTO_STATE_PROV,
  BILL_TO_POSTAL_CODE AS BILL_TO_POSTAL_CODE,
  BILL_TO_COUNTRY_CODE AS BILL_TO_COUNTRY_CODE,
  BILL_TO_PHONE_NUMBER AS BILL_TO_PHONE_NUMBER,
  BILL_TO_EMAIL AS BILL_TO_EMAIL,
  REQ_AUTH_AMOUNT AS REQ_AUTH_AMOUNT,
  REQ_SETTLEMENT_AMOUNT AS REQ_SETTLEMENT_AMOUNT,
  REQ_REFUND_AMOUNT AS REQ_REFUND_AMOUNT,
  CURRENCY_CODE AS CURRENCY_CODE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  PAYMENT_METHOD AS PAYMENT_METHOD,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_PAYMENT_DETAIL_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_A_PAYMENT_DETAIL_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_PAYMENT_DETAIL_PRE_1


query_1 = f"""SELECT
  PAYMENT_DETAIL_ID AS PAYMENT_DETAIL_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_NUMBER AS ENTITY_NUMBER,
  CARD_NUMBER AS CARD_NUMBER,
  BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
  BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
  BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
  BILL_TO_ADDRESS_LINE1 AS BILL_TO_ADDRESS_LINE1,
  BILL_TO_ADDRESS_LINE2 AS BILL_TO_ADDRESS_LINE2,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILLTO_STATE_PROV AS BILLTO_STATE_PROV,
  BILL_TO_POSTAL_CODE AS BILL_TO_POSTAL_CODE,
  BILL_TO_COUNTRY_CODE AS BILL_TO_COUNTRY_CODE,
  BILL_TO_PHONE_NUMBER AS BILL_TO_PHONE_NUMBER,
  BILL_TO_EMAIL AS BILL_TO_EMAIL,
  REQ_AUTH_AMOUNT AS REQ_AUTH_AMOUNT,
  REQ_SETTLEMENT_AMOUNT AS REQ_SETTLEMENT_AMOUNT,
  REQ_REFUND_AMOUNT AS REQ_REFUND_AMOUNT,
  CURRENCY_CODE AS CURRENCY_CODE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  PAYMENT_METHOD AS PAYMENT_METHOD,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_PAYMENT_DETAIL_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_PAYMENT_DETAIL_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_PAYMENT_DETAIL_2


query_2 = f"""SELECT
  OMS_PAYMENT_DETAIL_ID AS OMS_PAYMENT_DETAIL_ID,
  OMS_ENTITY_TYPE_ID AS OMS_ENTITY_TYPE_ID,
  OMS_ENTITY_ID AS OMS_ENTITY_ID,
  OMS_ENTITY_NBR AS OMS_ENTITY_NBR,
  CARD_NUMBER AS CARD_NUMBER,
  BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
  BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
  BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
  BILL_TO_ADDR_LINE1 AS BILL_TO_ADDR_LINE1,
  BILL_TO_ADDR_LINE2 AS BILL_TO_ADDR_LINE2,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILL_TO_STATE_PROV AS BILL_TO_STATE_PROV,
  BILL_TO_POSTAL_CD AS BILL_TO_POSTAL_CD,
  BILL_TO_COUNTRY_CD AS BILL_TO_COUNTRY_CD,
  BILL_TO_PHONE_NBR AS BILL_TO_PHONE_NBR,
  BILL_TO_EMAIL AS BILL_TO_EMAIL,
  OMS_PAYMENT_METHOD_ID AS OMS_PAYMENT_METHOD_ID,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  REQ_AUTH_AMT AS REQ_AUTH_AMT,
  REQ_SETTLEMENT_AMT AS REQ_SETTLEMENT_AMT,
  REQ_REFUND_AMT AS REQ_REFUND_AMT,
  CURRENCY_CD AS CURRENCY_CD,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_PAYMENT_DETAIL"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_A_PAYMENT_DETAIL_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_PAYMENT_DETAIL_3


query_3 = f"""SELECT
  OMS_PAYMENT_DETAIL_ID AS OMS_PAYMENT_DETAIL_ID,
  OMS_ENTITY_TYPE_ID AS OMS_ENTITY_TYPE_ID,
  OMS_ENTITY_ID AS OMS_ENTITY_ID,
  OMS_ENTITY_NBR AS OMS_ENTITY_NBR,
  CARD_NUMBER AS CARD_NUMBER,
  BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
  BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
  BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
  BILL_TO_ADDR_LINE1 AS BILL_TO_ADDR_LINE1,
  BILL_TO_ADDR_LINE2 AS BILL_TO_ADDR_LINE2,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILL_TO_STATE_PROV AS BILL_TO_STATE_PROV,
  BILL_TO_POSTAL_CD AS BILL_TO_POSTAL_CD,
  BILL_TO_COUNTRY_CD AS BILL_TO_COUNTRY_CD,
  BILL_TO_PHONE_NBR AS BILL_TO_PHONE_NBR,
  BILL_TO_EMAIL AS BILL_TO_EMAIL,
  OMS_PAYMENT_METHOD_ID AS OMS_PAYMENT_METHOD_ID,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  REQ_AUTH_AMT AS REQ_AUTH_AMT,
  REQ_SETTLEMENT_AMT AS REQ_SETTLEMENT_AMT,
  REQ_REFUND_AMT AS REQ_REFUND_AMT,
  CURRENCY_CD AS CURRENCY_CD,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_PAYMENT_DETAIL_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_PAYMENT_DETAIL_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_A_PAYMENT_DETAIL_4


query_4 = f"""SELECT
  DETAIL.PAYMENT_DETAIL_ID AS PAYMENT_DETAIL_ID,
  DETAIL.ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  DETAIL.ENTITY_ID AS ENTITY_ID,
  DETAIL.ENTITY_NUMBER AS ENTITY_NUMBER,
  DETAIL.CARD_NUMBER AS CARD_NUMBER,
  DETAIL.BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
  DETAIL.BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
  DETAIL.BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
  DETAIL.BILL_TO_ADDRESS_LINE1 AS BILL_TO_ADDRESS_LINE1,
  DETAIL.BILL_TO_ADDRESS_LINE2 AS BILL_TO_ADDRESS_LINE2,
  DETAIL.BILL_TO_CITY AS BILL_TO_CITY,
  DETAIL.BILLTO_STATE_PROV AS BILLTO_STATE_PROV,
  DETAIL.BILL_TO_POSTAL_CODE AS BILL_TO_POSTAL_CODE,
  DETAIL.BILL_TO_COUNTRY_CODE AS BILL_TO_COUNTRY_CODE,
  DETAIL.BILL_TO_PHONE_NUMBER AS BILL_TO_PHONE_NUMBER,
  DETAIL.BILL_TO_EMAIL AS BILL_TO_EMAIL,
  DETAIL.REQ_AUTH_AMOUNT AS REQ_AUTH_AMOUNT,
  DETAIL.REQ_SETTLEMENT_AMOUNT AS REQ_SETTLEMENT_AMOUNT,
  DETAIL.REQ_REFUND_AMOUNT AS REQ_REFUND_AMOUNT,
  DETAIL.CURRENCY_CODE AS CURRENCY_CODE,
  DETAIL.CREATED_DTTM AS CREATED_DTTM,
  DETAIL.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  DETAIL.PAYMENT_METHOD AS PAYMENT_METHOD,
  DETAIL.AFTERPAY_FLG AS AFTERPAY_FLG,
  MASTER.OMS_PAYMENT_DETAIL_ID AS lkp_OMS_PAYMENT_DETAIL_ID,
  MASTER.OMS_ENTITY_TYPE_ID AS lkp_OMS_ENTITY_TYPE_ID,
  MASTER.OMS_ENTITY_ID AS lkp_OMS_ENTITY_ID,
  MASTER.OMS_ENTITY_NBR AS lkp_OMS_ENTITY_NBR,
  MASTER.CARD_NUMBER AS lkp_CARD_NUMBER,
  MASTER.BILL_TO_FIRST_NAME AS lkp_BILL_TO_FIRST_NAME,
  MASTER.BILL_TO_MIDDLE_NAME AS lkp_BILL_TO_MIDDLE_NAME,
  MASTER.BILL_TO_LAST_NAME AS lkp_BILL_TO_LAST_NAME,
  MASTER.BILL_TO_ADDR_LINE1 AS lkp_BILL_TO_ADDR_LINE1,
  MASTER.BILL_TO_ADDR_LINE2 AS lkp_BILL_TO_ADDR_LINE2,
  MASTER.BILL_TO_CITY AS lkp_BILL_TO_CITY,
  MASTER.BILL_TO_STATE_PROV AS lkp_BILL_TO_STATE_PROV,
  MASTER.BILL_TO_POSTAL_CD AS lkp_BILL_TO_POSTAL_CD,
  MASTER.BILL_TO_COUNTRY_CD AS lkp_BILL_TO_COUNTRY_CD,
  MASTER.BILL_TO_PHONE_NBR AS lkp_BILL_TO_PHONE_NBR,
  MASTER.BILL_TO_EMAIL AS lkp_BILL_TO_EMAIL,
  MASTER.OMS_PAYMENT_METHOD_ID AS lkp_OMS_PAYMENT_METHOD_ID,
  MASTER.AFTERPAY_FLG AS lkp_AFTERPAY_FLG,
  MASTER.REQ_AUTH_AMT AS lkp_REQ_AUTH_AMT,
  MASTER.REQ_SETTLEMENT_AMT AS lkp_REQ_SETTLEMENT_AMT,
  MASTER.REQ_REFUND_AMT AS lkp_REQ_REFUND_AMT,
  MASTER.CURRENCY_CD AS lkp_CURRENCY_CD,
  MASTER.OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  MASTER.OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_A_PAYMENT_DETAIL_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_A_PAYMENT_DETAIL_PRE_1 DETAIL ON MASTER.OMS_PAYMENT_DETAIL_ID = DETAIL.PAYMENT_DETAIL_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_A_PAYMENT_DETAIL_4")

# COMMAND ----------
# DBTITLE 1, FTR_UNCHANGED_REC_5


query_5 = f"""SELECT
  PAYMENT_DETAIL_ID AS PAYMENT_DETAIL_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_NUMBER AS ENTITY_NUMBER,
  CARD_NUMBER AS CARD_NUMBER,
  BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
  BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
  BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
  BILL_TO_ADDRESS_LINE1 AS BILL_TO_ADDRESS_LINE1,
  BILL_TO_ADDRESS_LINE2 AS BILL_TO_ADDRESS_LINE2,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILLTO_STATE_PROV AS BILLTO_STATE_PROV,
  BILL_TO_POSTAL_CODE AS BILL_TO_POSTAL_CODE,
  BILL_TO_COUNTRY_CODE AS BILL_TO_COUNTRY_CODE,
  BILL_TO_PHONE_NUMBER AS BILL_TO_PHONE_NUMBER,
  BILL_TO_EMAIL AS BILL_TO_EMAIL,
  REQ_AUTH_AMOUNT AS REQ_AUTH_AMOUNT,
  REQ_SETTLEMENT_AMOUNT AS REQ_SETTLEMENT_AMOUNT,
  REQ_REFUND_AMOUNT AS REQ_REFUND_AMOUNT,
  CURRENCY_CODE AS CURRENCY_CODE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  PAYMENT_METHOD AS PAYMENT_METHOD,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  lkp_OMS_PAYMENT_DETAIL_ID AS lkp_OMS_PAYMENT_DETAIL_ID,
  lkp_OMS_ENTITY_TYPE_ID AS lkp_OMS_ENTITY_TYPE_ID,
  lkp_OMS_ENTITY_ID AS lkp_OMS_ENTITY_ID,
  lkp_OMS_ENTITY_NBR AS lkp_OMS_ENTITY_NBR,
  lkp_CARD_NUMBER AS lkp_CARD_NUMBER,
  lkp_BILL_TO_FIRST_NAME AS lkp_BILL_TO_FIRST_NAME,
  lkp_BILL_TO_MIDDLE_NAME AS lkp_BILL_TO_MIDDLE_NAME,
  lkp_BILL_TO_LAST_NAME AS lkp_BILL_TO_LAST_NAME,
  lkp_BILL_TO_ADDR_LINE1 AS lkp_BILL_TO_ADDR_LINE1,
  lkp_BILL_TO_ADDR_LINE2 AS lkp_BILL_TO_ADDR_LINE2,
  lkp_BILL_TO_CITY AS lkp_BILL_TO_CITY,
  lkp_BILL_TO_STATE_PROV AS lkp_BILL_TO_STATE_PROV,
  lkp_BILL_TO_POSTAL_CD AS lkp_BILL_TO_POSTAL_CD,
  lkp_BILL_TO_COUNTRY_CD AS lkp_BILL_TO_COUNTRY_CD,
  lkp_BILL_TO_PHONE_NBR AS lkp_BILL_TO_PHONE_NBR,
  lkp_BILL_TO_EMAIL AS lkp_BILL_TO_EMAIL,
  lkp_OMS_PAYMENT_METHOD_ID AS lkp_OMS_PAYMENT_METHOD_ID,
  lkp_AFTERPAY_FLG AS lkp_AFTERPAY_FLG,
  lkp_REQ_AUTH_AMT AS lkp_REQ_AUTH_AMT,
  lkp_REQ_SETTLEMENT_AMT AS lkp_REQ_SETTLEMENT_AMT,
  lkp_REQ_REFUND_AMT AS lkp_REQ_REFUND_AMT,
  lkp_CURRENCY_CD AS lkp_CURRENCY_CD,
  lkp_OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  lkp_OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_A_PAYMENT_DETAIL_4
WHERE
  ISNULL(lkp_OMS_PAYMENT_DETAIL_ID)
  OR (
    NOT ISNULL(lkp_OMS_PAYMENT_DETAIL_ID)
    AND (
      IFF(
        ISNULL(ENTITY_TYPE_ID),
        TO_INTEGER(999999999),
        ENTITY_TYPE_ID
      ) <> IFF(
        ISNULL(lkp_OMS_ENTITY_TYPE_ID),
        TO_INTEGER(999999999),
        lkp_OMS_ENTITY_TYPE_ID
      )
      OR IFF(
        ISNULL(ENTITY_ID),
        TO_INTEGER(999999999),
        ENTITY_ID
      ) <> IFF(
        ISNULL(lkp_OMS_ENTITY_ID),
        TO_INTEGER(999999999),
        lkp_OMS_ENTITY_ID
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(ENTITY_NUMBER))),
        ' ',
        LTRIM(RTRIM(ENTITY_NUMBER))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_OMS_ENTITY_NBR))),
        ' ',
        LTRIM(RTRIM(lkp_OMS_ENTITY_NBR))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(CARD_NUMBER))),
        ' ',
        LTRIM(RTRIM(CARD_NUMBER))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_CARD_NUMBER))),
        ' ',
        LTRIM(RTRIM(lkp_CARD_NUMBER))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_FIRST_NAME))),
        ' ',
        LTRIM(RTRIM(BILL_TO_FIRST_NAME))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_FIRST_NAME))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_FIRST_NAME))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_MIDDLE_NAME))),
        ' ',
        LTRIM(RTRIM(BILL_TO_MIDDLE_NAME))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_MIDDLE_NAME))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_MIDDLE_NAME))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_LAST_NAME))),
        ' ',
        LTRIM(RTRIM(BILL_TO_LAST_NAME))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_LAST_NAME))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_LAST_NAME))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_ADDRESS_LINE1))),
        ' ',
        LTRIM(RTRIM(BILL_TO_ADDRESS_LINE1))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_ADDR_LINE1))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_ADDR_LINE1))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_ADDRESS_LINE2))),
        ' ',
        LTRIM(RTRIM(BILL_TO_ADDRESS_LINE2))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_ADDR_LINE2))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_ADDR_LINE2))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_CITY))),
        ' ',
        LTRIM(RTRIM(BILL_TO_CITY))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_CITY))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_CITY))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILLTO_STATE_PROV))),
        ' ',
        LTRIM(RTRIM(BILLTO_STATE_PROV))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_STATE_PROV))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_STATE_PROV))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_POSTAL_CODE))),
        ' ',
        LTRIM(RTRIM(BILL_TO_POSTAL_CODE))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_POSTAL_CD))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_POSTAL_CD))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_COUNTRY_CODE))),
        ' ',
        LTRIM(RTRIM(BILL_TO_COUNTRY_CODE))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_COUNTRY_CD))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_COUNTRY_CD))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_PHONE_NUMBER))),
        ' ',
        LTRIM(RTRIM(BILL_TO_PHONE_NUMBER))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_PHONE_NBR))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_PHONE_NBR))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(BILL_TO_EMAIL))),
        ' ',
        LTRIM(RTRIM(BILL_TO_EMAIL))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_BILL_TO_EMAIL))),
        ' ',
        LTRIM(RTRIM(lkp_BILL_TO_EMAIL))
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(CURRENCY_CODE))),
        ' ',
        LTRIM(RTRIM(CURRENCY_CODE))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_CURRENCY_CD))),
        ' ',
        LTRIM(RTRIM(lkp_CURRENCY_CD))
      )
      OR IFF(
        ISNULL(REQ_AUTH_AMOUNT),
        TO_INTEGER(999999999),
        REQ_AUTH_AMOUNT
      ) <> IFF(
        ISNULL(lkp_REQ_AUTH_AMT),
        TO_INTEGER(999999999),
        lkp_REQ_AUTH_AMT
      )
      OR IFF(
        ISNULL(REQ_SETTLEMENT_AMOUNT),
        TO_INTEGER(999999999),
        REQ_SETTLEMENT_AMOUNT
      ) <> IFF(
        ISNULL(lkp_REQ_SETTLEMENT_AMT),
        TO_INTEGER(999999999),
        lkp_REQ_SETTLEMENT_AMT
      )
      OR IFF(
        ISNULL(REQ_REFUND_AMOUNT),
        TO_INTEGER(999999999),
        REQ_REFUND_AMOUNT
      ) <> IFF(
        ISNULL(lkp_REQ_REFUND_AMT),
        TO_INTEGER(999999999),
        lkp_REQ_REFUND_AMT
      )
      OR IFF(
        ISNULL(lkp_OMS_PAYMENT_METHOD_ID),
        TO_INTEGER(999999999),
        lkp_OMS_PAYMENT_METHOD_ID
      ) <> IFF(
        ISNULL(PAYMENT_METHOD),
        TO_INTEGER(999999999),
        PAYMENT_METHOD
      )
      OR IFF (
        ISNULL(LTRIM(RTRIM(AFTERPAY_FLG))),
        ' ',
        LTRIM(RTRIM(AFTERPAY_FLG))
      ) <> IFF(
        ISNULL(LTRIM(RTRIM(lkp_AFTERPAY_FLG))),
        ' ',
        LTRIM(RTRIM(lkp_AFTERPAY_FLG))
      )
      OR IFF(
        ISNULL(CREATED_DTTM),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        CREATED_DTTM
      ) <> IFF(
        ISNULL(lkp_OMS_CREATED_TSTMP),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        lkp_OMS_CREATED_TSTMP
      )
      OR IFF(
        ISNULL(LAST_UPDATED_DTTM),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        LAST_UPDATED_DTTM
      ) <> IFF(
        ISNULL(lkp_OMS_LAST_UPDATED_TSTMP),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        lkp_OMS_LAST_UPDATED_TSTMP
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FTR_UNCHANGED_REC_5")

# COMMAND ----------
# DBTITLE 1, EXP_VALID_FLAG_6


query_6 = f"""SELECT
  PAYMENT_DETAIL_ID AS PAYMENT_DETAIL_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_NUMBER AS ENTITY_NUMBER,
  CARD_NUMBER AS CARD_NUMBER,
  BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
  BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
  BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
  BILL_TO_ADDRESS_LINE1 AS BILL_TO_ADDRESS_LINE1,
  BILL_TO_ADDRESS_LINE2 AS BILL_TO_ADDRESS_LINE2,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILLTO_STATE_PROV AS BILLTO_STATE_PROV,
  BILL_TO_POSTAL_CODE AS BILL_TO_POSTAL_CODE,
  BILL_TO_COUNTRY_CODE AS BILL_TO_COUNTRY_CODE,
  BILL_TO_PHONE_NUMBER AS BILL_TO_PHONE_NUMBER,
  BILL_TO_EMAIL AS BILL_TO_EMAIL,
  REQ_AUTH_AMOUNT AS REQ_AUTH_AMOUNT,
  REQ_SETTLEMENT_AMOUNT AS REQ_SETTLEMENT_AMOUNT,
  REQ_REFUND_AMOUNT AS REQ_REFUND_AMOUNT,
  CURRENCY_CODE AS CURRENCY_CODE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  PAYMENT_METHOD AS PAYMENT_METHOD,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  lkp_OMS_PAYMENT_DETAIL_ID AS lkp_OMS_PAYMENT_DETAIL_ID,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  now() AS UPDATE_TSTMP_exp,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS LOAD_TSTMP_exp,
  IFF(ISNULL(lkp_OMS_PAYMENT_DETAIL_ID), 1, 2) AS o_VALID_UPDATOR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FTR_UNCHANGED_REC_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_VALID_FLAG_6")

# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD_7


query_7 = f"""SELECT
  PAYMENT_DETAIL_ID AS PAYMENT_DETAIL_ID,
  ENTITY_TYPE_ID AS ENTITY_TYPE_ID,
  ENTITY_ID AS ENTITY_ID,
  ENTITY_NUMBER AS ENTITY_NUMBER,
  CARD_NUMBER AS CARD_NUMBER,
  BILL_TO_FIRST_NAME AS BILL_TO_FIRST_NAME,
  BILL_TO_MIDDLE_NAME AS BILL_TO_MIDDLE_NAME,
  BILL_TO_LAST_NAME AS BILL_TO_LAST_NAME,
  BILL_TO_ADDRESS_LINE1 AS BILL_TO_ADDRESS_LINE1,
  BILL_TO_ADDRESS_LINE2 AS BILL_TO_ADDRESS_LINE2,
  BILL_TO_CITY AS BILL_TO_CITY,
  BILLTO_STATE_PROV AS BILLTO_STATE_PROV,
  BILL_TO_POSTAL_CODE AS BILL_TO_POSTAL_CODE,
  BILL_TO_COUNTRY_CODE AS BILL_TO_COUNTRY_CODE,
  BILL_TO_PHONE_NUMBER AS BILL_TO_PHONE_NUMBER,
  BILL_TO_EMAIL AS BILL_TO_EMAIL,
  PAYMENT_METHOD AS PAYMENT_METHOD,
  AFTERPAY_FLG AS AFTERPAY_FLG,
  REQ_AUTH_AMOUNT AS REQ_AUTH_AMOUNT,
  REQ_SETTLEMENT_AMOUNT AS REQ_SETTLEMENT_AMOUNT,
  REQ_REFUND_AMOUNT AS REQ_REFUND_AMOUNT,
  CURRENCY_CODE AS CURRENCY_CODE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  UPDATE_TSTMP_exp AS UPDATE_TSTMP_exp,
  LOAD_TSTMP_exp AS LOAD_TSTMP_exp,
  o_VALID_UPDATOR AS o_VALID_UPDATOR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(o_VALID_UPDATOR, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_VALID_FLAG_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_INS_UPD_7")

# COMMAND ----------
# DBTITLE 1, OMS_A_PAYMENT_DETAIL


spark.sql("""MERGE INTO OMS_A_PAYMENT_DETAIL AS TARGET
USING
  UPD_INS_UPD_7 AS SOURCE ON TARGET.OMS_PAYMENT_DETAIL_ID = SOURCE.PAYMENT_DETAIL_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_PAYMENT_DETAIL_ID = SOURCE.PAYMENT_DETAIL_ID,
  TARGET.OMS_ENTITY_TYPE_ID = SOURCE.ENTITY_TYPE_ID,
  TARGET.OMS_ENTITY_ID = SOURCE.ENTITY_ID,
  TARGET.OMS_ENTITY_NBR = SOURCE.ENTITY_NUMBER,
  TARGET.CARD_NUMBER = SOURCE.CARD_NUMBER,
  TARGET.BILL_TO_FIRST_NAME = SOURCE.BILL_TO_FIRST_NAME,
  TARGET.BILL_TO_MIDDLE_NAME = SOURCE.BILL_TO_MIDDLE_NAME,
  TARGET.BILL_TO_LAST_NAME = SOURCE.BILL_TO_LAST_NAME,
  TARGET.BILL_TO_ADDR_LINE1 = SOURCE.BILL_TO_ADDRESS_LINE1,
  TARGET.BILL_TO_ADDR_LINE2 = SOURCE.BILL_TO_ADDRESS_LINE2,
  TARGET.BILL_TO_CITY = SOURCE.BILL_TO_CITY,
  TARGET.BILL_TO_STATE_PROV = SOURCE.BILLTO_STATE_PROV,
  TARGET.BILL_TO_POSTAL_CD = SOURCE.BILL_TO_POSTAL_CODE,
  TARGET.BILL_TO_COUNTRY_CD = SOURCE.BILL_TO_COUNTRY_CODE,
  TARGET.BILL_TO_PHONE_NBR = SOURCE.BILL_TO_PHONE_NUMBER,
  TARGET.BILL_TO_EMAIL = SOURCE.BILL_TO_EMAIL,
  TARGET.OMS_PAYMENT_METHOD_ID = SOURCE.PAYMENT_METHOD,
  TARGET.AFTERPAY_FLG = SOURCE.AFTERPAY_FLG,
  TARGET.REQ_AUTH_AMT = SOURCE.REQ_AUTH_AMOUNT,
  TARGET.REQ_SETTLEMENT_AMT = SOURCE.REQ_SETTLEMENT_AMOUNT,
  TARGET.REQ_REFUND_AMT = SOURCE.REQ_REFUND_AMOUNT,
  TARGET.CURRENCY_CD = SOURCE.CURRENCY_CODE,
  TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM,
  TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP_exp,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_ENTITY_TYPE_ID = SOURCE.ENTITY_TYPE_ID
  AND TARGET.OMS_ENTITY_ID = SOURCE.ENTITY_ID
  AND TARGET.OMS_ENTITY_NBR = SOURCE.ENTITY_NUMBER
  AND TARGET.CARD_NUMBER = SOURCE.CARD_NUMBER
  AND TARGET.BILL_TO_FIRST_NAME = SOURCE.BILL_TO_FIRST_NAME
  AND TARGET.BILL_TO_MIDDLE_NAME = SOURCE.BILL_TO_MIDDLE_NAME
  AND TARGET.BILL_TO_LAST_NAME = SOURCE.BILL_TO_LAST_NAME
  AND TARGET.BILL_TO_ADDR_LINE1 = SOURCE.BILL_TO_ADDRESS_LINE1
  AND TARGET.BILL_TO_ADDR_LINE2 = SOURCE.BILL_TO_ADDRESS_LINE2
  AND TARGET.BILL_TO_CITY = SOURCE.BILL_TO_CITY
  AND TARGET.BILL_TO_STATE_PROV = SOURCE.BILLTO_STATE_PROV
  AND TARGET.BILL_TO_POSTAL_CD = SOURCE.BILL_TO_POSTAL_CODE
  AND TARGET.BILL_TO_COUNTRY_CD = SOURCE.BILL_TO_COUNTRY_CODE
  AND TARGET.BILL_TO_PHONE_NBR = SOURCE.BILL_TO_PHONE_NUMBER
  AND TARGET.BILL_TO_EMAIL = SOURCE.BILL_TO_EMAIL
  AND TARGET.OMS_PAYMENT_METHOD_ID = SOURCE.PAYMENT_METHOD
  AND TARGET.AFTERPAY_FLG = SOURCE.AFTERPAY_FLG
  AND TARGET.REQ_AUTH_AMT = SOURCE.REQ_AUTH_AMOUNT
  AND TARGET.REQ_SETTLEMENT_AMT = SOURCE.REQ_SETTLEMENT_AMOUNT
  AND TARGET.REQ_REFUND_AMT = SOURCE.REQ_REFUND_AMOUNT
  AND TARGET.CURRENCY_CD = SOURCE.CURRENCY_CODE
  AND TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM
  AND TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP_exp
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_PAYMENT_DETAIL_ID,
    TARGET.OMS_ENTITY_TYPE_ID,
    TARGET.OMS_ENTITY_ID,
    TARGET.OMS_ENTITY_NBR,
    TARGET.CARD_NUMBER,
    TARGET.BILL_TO_FIRST_NAME,
    TARGET.BILL_TO_MIDDLE_NAME,
    TARGET.BILL_TO_LAST_NAME,
    TARGET.BILL_TO_ADDR_LINE1,
    TARGET.BILL_TO_ADDR_LINE2,
    TARGET.BILL_TO_CITY,
    TARGET.BILL_TO_STATE_PROV,
    TARGET.BILL_TO_POSTAL_CD,
    TARGET.BILL_TO_COUNTRY_CD,
    TARGET.BILL_TO_PHONE_NBR,
    TARGET.BILL_TO_EMAIL,
    TARGET.OMS_PAYMENT_METHOD_ID,
    TARGET.AFTERPAY_FLG,
    TARGET.REQ_AUTH_AMT,
    TARGET.REQ_SETTLEMENT_AMT,
    TARGET.REQ_REFUND_AMT,
    TARGET.CURRENCY_CD,
    TARGET.OMS_CREATED_TSTMP,
    TARGET.OMS_LAST_UPDATED_TSTMP,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.PAYMENT_DETAIL_ID,
    SOURCE.ENTITY_TYPE_ID,
    SOURCE.ENTITY_ID,
    SOURCE.ENTITY_NUMBER,
    SOURCE.CARD_NUMBER,
    SOURCE.BILL_TO_FIRST_NAME,
    SOURCE.BILL_TO_MIDDLE_NAME,
    SOURCE.BILL_TO_LAST_NAME,
    SOURCE.BILL_TO_ADDRESS_LINE1,
    SOURCE.BILL_TO_ADDRESS_LINE2,
    SOURCE.BILL_TO_CITY,
    SOURCE.BILLTO_STATE_PROV,
    SOURCE.BILL_TO_POSTAL_CODE,
    SOURCE.BILL_TO_COUNTRY_CODE,
    SOURCE.BILL_TO_PHONE_NUMBER,
    SOURCE.BILL_TO_EMAIL,
    SOURCE.PAYMENT_METHOD,
    SOURCE.AFTERPAY_FLG,
    SOURCE.REQ_AUTH_AMOUNT,
    SOURCE.REQ_SETTLEMENT_AMOUNT,
    SOURCE.REQ_REFUND_AMOUNT,
    SOURCE.CURRENCY_CODE,
    SOURCE.CREATED_DTTM,
    SOURCE.LAST_UPDATED_DTTM,
    SOURCE.UPDATE_TSTMP_exp,
    SOURCE.LOAD_TSTMP_exp
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Payment_Detail")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_A_Payment_Detail", mainWorkflowId, parentName)
