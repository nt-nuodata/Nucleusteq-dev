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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_MarketPlace_Mthly_Invc_Send_Email")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DD_MarketPlace_Mthly_Invc_Send_Email", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, src_Petsmart_Monthly_Extra_Invoice_0


query_0 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NUMBER AS INVOICE_NUMBER,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  Petsmart_Monthly_Extra_Invoice"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("src_Petsmart_Monthly_Extra_Invoice_0")

# COMMAND ----------
# DBTITLE 1, sq_Petsmart_Monthly_Extra_Invoice_1


query_1 = f"""SELECT
  TXN_DT AS TXN_DT,
  DD_DELIVERY_UUID AS DD_DELIVERY_UUID,
  DASHPASS_ORDER_FLAG AS DASHPASS_ORDER_FLAG,
  INVOICE_NUMBER AS INVOICE_NUMBER,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  src_Petsmart_Monthly_Extra_Invoice_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("sq_Petsmart_Monthly_Extra_Invoice_1")

# COMMAND ----------
# DBTITLE 1, fil_Dummy_Tgt_2


query_2 = f"""SELECT
  TXN_DT AS TXN_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  sq_Petsmart_Monthly_Extra_Invoice_1
WHERE
  FALSE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("fil_Dummy_Tgt_2")

# COMMAND ----------
# DBTITLE 1, DUMMY_TARGET


spark.sql("""INSERT INTO
  DUMMY_TARGET
SELECT
  TXN_DT AS COMMENT
FROM
  fil_Dummy_Tgt_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DD_MarketPlace_Mthly_Invc_Send_Email")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DD_MarketPlace_Mthly_Invc_Send_Email", mainWorkflowId, parentName)
