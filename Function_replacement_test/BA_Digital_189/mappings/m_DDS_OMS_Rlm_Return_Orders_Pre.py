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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Rlm_Return_Orders_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Rlm_Return_Orders_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0


query_0 = f"""SELECT
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  TC_PURCHASE_ORDERS_ID AS TC_PURCHASE_ORDERS_ID
FROM
  OMS_PURCH_ORDER_LOAD_CTRL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_RLM_RETURN_ORDERS_1


query_1 = f"""SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA
FROM
  RLM_RETURN_ORDERS"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_RLM_RETURN_ORDERS_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_RLM_RETURN_ORDERS_2


query_2 = f"""SELECT
  Shortcut_to_RLM_RETURN_ORDERS_1.RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  Shortcut_to_RLM_RETURN_ORDERS_1.RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  Shortcut_to_RLM_RETURN_ORDERS_1.IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_RLM_RETURN_ORDERS_1,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0
WHERE
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID = Shortcut_to_RLM_RETURN_ORDERS_1.RETURN_ORDERS_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_RLM_RETURN_ORDERS_2")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_3


query_3 = f"""SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_RLM_RETURN_ORDERS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_LOAD_TSTMP_3")

# COMMAND ----------
# DBTITLE 1, OMS_RLM_RETURN_ORDERS_PRE


spark.sql("""INSERT INTO
  OMS_RLM_RETURN_ORDERS_PRE
SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_XO_CREATED AS RETURN_ORDERS_XO_CREATED,
  IS_AUTOMATED_RMA AS IS_AUTOMATED_RMA,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Rlm_Return_Orders_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Rlm_Return_Orders_Pre", mainWorkflowId, parentName)
