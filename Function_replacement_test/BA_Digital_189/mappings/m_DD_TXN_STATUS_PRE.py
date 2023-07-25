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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_TXN_STATUS_PRE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DD_TXN_STATUS_PRE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, src_DD_TXN_STATUS_0


query_0 = f"""SELECT
  JMSCorrelationID AS JMSCorrelationID,
  JMSMessageID AS JMSMessageID,
  JMSType AS JMSType,
  JMSDestination AS JMSDestination,
  JMSReplyTo AS JMSReplyTo,
  JMSDeliveryMode AS JMSDeliveryMode,
  JMSExpiration AS JMSExpiration,
  JMSPriority AS JMSPriority,
  JMSTimeStamp AS JMSTimeStamp,
  JMSRedelivered AS JMSRedelivered,
  BodyText AS BodyText
FROM
  DD_TXN_STATUS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("src_DD_TXN_STATUS_0")

# COMMAND ----------
# DBTITLE 1, sq_DD_TXN_STATUS_1


query_1 = f"""SELECT
  JMSCorrelationID AS JMSCorrelationID,
  JMSMessageID AS JMSMessageID,
  JMSType AS JMSType,
  JMSDestination AS JMSDestination,
  JMSReplyTo AS JMSReplyTo,
  JMSDeliveryMode AS JMSDeliveryMode,
  JMSExpiration AS JMSExpiration,
  JMSPriority AS JMSPriority,
  JMSTimeStamp AS JMSTimeStamp,
  JMSRedelivered AS JMSRedelivered,
  BodyText AS BodyText,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  src_DD_TXN_STATUS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("sq_DD_TXN_STATUS_1")

# COMMAND ----------
# DBTITLE 1, TxnStatusElementResource_New1_2


query_2 = f"""SELECT
  BodyText AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  sq_DD_TXN_STATUS_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("TxnStatusElementResource_New1_2")

# COMMAND ----------
# DBTITLE 1, fil_NULLS_3


query_3 = f"""SELECT
  INVALID_PAYLOAD AS INVALID_PAYLOAD,
  ERROR_STATUS AS ERROR_STATUS,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  TxnStatusElementResource_New1_2
WHERE
  NOT ISNULL(ERROR_STATUS)"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("fil_NULLS_3")

# COMMAND ----------
# DBTITLE 1, exp_ERR_CNT_4


query_4 = f"""SELECT
  v_ERR_CNT + 1 AS v_ERR_CNT,
  SETVARIABLE('SRC_ERR_CNT', v_ERR_CNT + 1) AS o_ERR_CNT,
  INVALID_PAYLOAD AS INVALID_PAYLOAD,
  ERROR_STATUS AS ERROR_STATUS,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  fil_NULLS_3"""

df_4 = spark.sql(query_4)

if df_4.count() > 0 :
  SRC_ERR_CNT = df_4.agg({'o_ERR_CNT' : 'max'}).collect()[0][0]

df_4.createOrReplaceTempView("exp_ERR_CNT_4")

# COMMAND ----------
# DBTITLE 1, exp_ERR_SCRUB_5


query_5 = f"""SELECT
  o_ERR_CNT AS o_ERR_CNT,
  REPLACECHR(false, INVALID_PAYLOAD, '"', '') AS o_INVALID_PAYLOAD,
  ERROR_STATUS AS ERROR_STATUS,
  LTRIM(RTRIM(ERROR_STATUS)) AS o_ERROR_STATUS,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  exp_ERR_CNT_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("exp_ERR_SCRUB_5")

# COMMAND ----------
# DBTITLE 1, exp_TARGET_6


query_6 = f"""SELECT
  txn_sourceID AS sourceID,
  IFF(
    IS_NUMBER(txn_loyaltyIDCust),
    TO_BIGINT(txn_loyaltyIDCust),
    NULL
  ) AS o_LOYALTY_ID_CUSTOMER,
  IFF(
    IS_NUMBER(txn_loyaltyIDClaimed),
    TO_BIGINT(txn_loyaltyIDClaimed),
    NULL
  ) AS o_LOYALTY_ID_CLAIMED,
  IFF(
    IS_NUMBER(txn_transactionID),
    TO_BIGINT(txn_transactionID),
    NULL
  ) AS o_TRANSACTION_ID,
  IFF(
    IS_DATE(txn_txnDateTime, 'YYYY-MM-DD HH24:MI:SS'),
    TO_DATE(txn_txnDateTime, 'YYYY-MM-DD HH24:MI:SS'),
    NULL
  ) AS o_TXN_TSTSMP,
  TO_CHAR(txn_status) AS o_STATUS,
  TO_CHAR(txn_statusCode) AS o_statusCode,
  SESSSTARTTIME AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  TxnStatusElementResource_New1_2"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("exp_TARGET_6")

# COMMAND ----------
# DBTITLE 1, XML_INVALID_PAYLOAD_FF


spark.sql("""INSERT INTO
  XML_INVALID_PAYLOAD_FF
SELECT
  o_ERROR_STATUS AS Message,
  o_INVALID_PAYLOAD AS Data
FROM
  exp_ERR_SCRUB_5""")

# COMMAND ----------
# DBTITLE 1, DD_TXN_STATUS_PRE


spark.sql("""INSERT INTO
  DD_TXN_STATUS_PRE
SELECT
  sourceID AS SOURCE_ID,
  o_LOYALTY_ID_CUSTOMER AS LOYALTY_ID_CUSTOMER,
  o_LOYALTY_ID_CLAIMED AS LOYALTY_ID_CLAIMED,
  o_TRANSACTION_ID AS TXN_ID,
  o_TXN_TSTSMP AS TXN_TSTSMP,
  o_STATUS AS STATUS,
  o_statusCode AS STATUS_CODE,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  exp_TARGET_6""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_TXN_STATUS_PRE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DD_TXN_STATUS_PRE", mainWorkflowId, parentName)
