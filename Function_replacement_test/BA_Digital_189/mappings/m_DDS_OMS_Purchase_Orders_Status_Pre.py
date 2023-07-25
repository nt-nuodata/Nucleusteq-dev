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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Purchase_Orders_Status_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Purchase_Orders_Status_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PURCHASE_ORDERS_STATUS_0


query_0 = f"""SELECT
  PURCHASE_ORDERS_STATUS AS PURCHASE_ORDERS_STATUS,
  DESCRIPTION AS DESCRIPTION,
  NOTE AS NOTE
FROM
  PURCHASE_ORDERS_STATUS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PURCHASE_ORDERS_STATUS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PURCHASE_ORDERS_STATUS_1


query_1 = f"""SELECT
  PURCHASE_ORDERS_STATUS AS PURCHASE_ORDERS_STATUS,
  DESCRIPTION AS DESCRIPTION,
  NOTE AS NOTE,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PURCHASE_ORDERS_STATUS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PURCHASE_ORDERS_STATUS_1")

# COMMAND ----------
# DBTITLE 1, EXP_TRANS_2


query_2 = f"""SELECT
  PURCHASE_ORDERS_STATUS AS PURCHASE_ORDERS_STATUS,
  DESCRIPTION AS DESCRIPTION,
  NOTE AS NOTE,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PURCHASE_ORDERS_STATUS_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_TRANS_2")

# COMMAND ----------
# DBTITLE 1, OMS_PURCHASE_ORDERS_STATUS_PRE


spark.sql("""INSERT INTO
  OMS_PURCHASE_ORDERS_STATUS_PRE
SELECT
  PURCHASE_ORDERS_STATUS AS PURCHASE_ORDERS_STATUS,
  DESCRIPTION AS DESCRIPTION,
  NOTE AS NOTE,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_TRANS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Purchase_Orders_Status_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Purchase_Orders_Status_Pre", mainWorkflowId, parentName)