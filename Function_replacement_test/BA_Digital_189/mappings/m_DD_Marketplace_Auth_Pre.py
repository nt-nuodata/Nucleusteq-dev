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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_Marketplace_Auth_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DD_Marketplace_Auth_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DD_Petsmart_Market_FF_0


query_0 = f"""SELECT
  Transaction_Timestamp__UTC AS Transaction_Timestamp__UTC,
  Date__local AS Date__local,
  Transaction_Timestamp__local AS Transaction_Timestamp__local,
  MERCHANT AS MERCHANT,
  EXTERNAL_STORE_NUMBER AS EXTERNAL_STORE_NUMBER,
  TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT,
  ACTING_CARD_FIRST_SIX AS ACTING_CARD_FIRST_SIX,
  ACTING_CARD_LAST_FOUR AS ACTING_CARD_LAST_FOUR,
  DOORDASH_DELIVERY_UUID AS DOORDASH_DELIVERY_UUID,
  DASHPASS_ORDER AS DASHPASS_ORDER,
  APPROVAL_CODE AS APPROVAL_CODE,
  NETWORKREFERENCEID AS NETWORKREFERENCEID,
  TREATS_ID AS TREATS_ID
FROM
  DD_Petsmart_Market_FF"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_DD_Petsmart_Market_FF_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DD_Petsmart_Market_FF_1


query_1 = f"""SELECT
  Transaction_Timestamp__UTC AS Transaction_Timestamp__UTC,
  Date__local AS Date__local,
  Transaction_Timestamp__local AS Transaction_Timestamp__local,
  MERCHANT AS MERCHANT,
  EXTERNAL_STORE_NUMBER AS EXTERNAL_STORE_NUMBER,
  TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT,
  ACTING_CARD_FIRST_SIX AS ACTING_CARD_FIRST_SIX,
  ACTING_CARD_LAST_FOUR AS ACTING_CARD_LAST_FOUR,
  DOORDASH_DELIVERY_UUID AS DOORDASH_DELIVERY_UUID,
  DASHPASS_ORDER AS DASHPASS_ORDER,
  APPROVAL_CODE AS APPROVAL_CODE,
  NETWORKREFERENCEID AS NETWORKREFERENCEID,
  TREATS_ID AS TREATS_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_DD_Petsmart_Market_FF_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_DD_Petsmart_Market_FF_1")

# COMMAND ----------
# DBTITLE 1, EXP_Target_2


query_2 = f"""SELECT
  Transaction_Timestamp__UTC AS Transaction_Timestamp__UTC,
  Date__local AS Date__local,
  Transaction_Timestamp__local AS Transaction_Timestamp__local,
  MERCHANT AS MERCHANT,
  LTRIM(
    RTRIM(REG_REPLACE(EXTERNAL_STORE_NUMBER, '[^0-9]', ''))
  ) AS o_EXTERNAL_STORE_NUMBER,
  TO_DECIMAL(TRANSACTION_AMOUNT, 2) AS o_TRANSACTION_AMOUNT,
  LTRIM(RTRIM(ACTING_CARD_FIRST_SIX)) AS o_ACTING_CARD_FIRST_SIX,
  LTRIM(RTRIM(ACTING_CARD_LAST_FOUR)) AS o_ACTING_CARD_LAST_FOUR,
  LTRIM(RTRIM(DOORDASH_DELIVERY_UUID)) AS o_DOORDASH_DELIVERY_UUID,
  TO_DECIMAL(DASHPASS_ORDER, 0) AS o_DASHPASS_ORDER,
  LTRIM(RTRIM(APPROVAL_CODE)) AS o_APPROVAL_CODE,
  NETWORKREFERENCEID AS NETWORKREFERENCEID,
  TREATS_ID AS TREATS_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_DD_Petsmart_Market_FF_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_Target_2")

# COMMAND ----------
# DBTITLE 1, DD_MARKETPLACE_AUTH_PRE


spark.sql("""INSERT INTO
  DD_MARKETPLACE_AUTH_PRE
SELECT
  Transaction_Timestamp__UTC AS TRANSACTION_TIMESTAMP_UTC,
  Date__local AS TRANSACTION_DATE_LOCAL,
  Transaction_Timestamp__local AS TRANSACTION_TIMESTAMP_LOCAL,
  MERCHANT AS MERCHANT,
  o_EXTERNAL_STORE_NUMBER AS EXTERNAL_STORE_NUMBER,
  o_TRANSACTION_AMOUNT AS TRANSACTION_AMOUNT,
  o_ACTING_CARD_FIRST_SIX AS ACTING_CARD_FIRST_SIX,
  o_ACTING_CARD_LAST_FOUR AS ACTING_CARD_LAST_FOUR,
  o_DOORDASH_DELIVERY_UUID AS DOORDASH_DELIVERY_UUID,
  o_DASHPASS_ORDER AS DASHPASS_ORDER,
  o_APPROVAL_CODE AS APPROVAL_CODE,
  NETWORKREFERENCEID AS NETWORK_REFERENCE_ID,
  TREATS_ID AS TREATS_ID
FROM
  EXP_Target_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_Marketplace_Auth_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DD_Marketplace_Auth_Pre", mainWorkflowId, parentName)
