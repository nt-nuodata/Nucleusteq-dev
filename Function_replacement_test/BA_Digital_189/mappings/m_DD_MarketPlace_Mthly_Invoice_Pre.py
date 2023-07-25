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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_MarketPlace_Mthly_Invoice_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DD_MarketPlace_Mthly_Invoice_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, DD_Petsmart_Monthly_Invoice_FF_0


query_0 = f"""SELECT
  TRANSACTION_DATE AS TRANSACTION_DATE,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NUMBER AS INVOICE_NUMBER
FROM
  DD_Petsmart_Monthly_Invoice_FF"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("DD_Petsmart_Monthly_Invoice_FF_0")

# COMMAND ----------
# DBTITLE 1, sq_to_DD_Petsmart_Monthly_Invoice_FF_1


query_1 = f"""SELECT
  TRANSACTION_DATE AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NUMBER AS INVOICE_NUMBER,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  DD_Petsmart_Monthly_Invoice_FF_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("sq_to_DD_Petsmart_Monthly_Invoice_FF_1")

# COMMAND ----------
# DBTITLE 1, exp_TARGET_2


query_2 = f"""SELECT
  IFF(
    IS_DATE(TXN_DT, {DT_FRMT}),
    TO_DATE(TXN_DT, 'YYYY-MM-DD'),
    NULL
  ) AS o_TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  IFF(
    IS_NUMBER(INVOICE_NUMBER),
    TO_BIGINT(INVOICE_NUMBER),
    NULL
  ) AS o_INVOICE_NUMBER,
  SESSSTARTTIME AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  sq_to_DD_Petsmart_Monthly_Invoice_FF_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("exp_TARGET_2")

# COMMAND ----------
# DBTITLE 1, DD_MARKETPLACE_MTHLY_INVOICE_PRE


spark.sql("""INSERT INTO
  DD_MARKETPLACE_MTHLY_INVOICE_PRE
SELECT
  o_TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  o_INVOICE_NUMBER AS INVOICE_NBR,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  exp_TARGET_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_MarketPlace_Mthly_Invoice_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DD_MarketPlace_Mthly_Invoice_Pre", mainWorkflowId, parentName)
