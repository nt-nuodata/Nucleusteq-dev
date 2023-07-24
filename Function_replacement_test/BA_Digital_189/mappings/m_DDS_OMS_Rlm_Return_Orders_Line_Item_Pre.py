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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Rlm_Return_Orders_Line_Item_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Rlm_Return_Orders_Line_Item_Pre", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1


query_1 = f"""SELECT
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_ORDERS_LINE_ITEM_ID AS RETURN_ORDERS_LINE_ITEM_ID,
  RETURN_REASON_ID AS RETURN_REASON_ID,
  RETURN_ACTION_ID AS RETURN_ACTION_ID,
  EXPECTED_RECEIVING_CONDITION AS EXPECTED_RECEIVING_CONDITION,
  IS_RECEIVABLE AS IS_RECEIVABLE,
  RECEIVING_VARIANCE_CODE AS RECEIVING_VARIANCE_CODE,
  UNIT_SHIPPING_HANDLING_CHARGE AS UNIT_SHIPPING_HANDLING_CHARGE,
  TOTAL_FEE AS TOTAL_FEE
FROM
  RLM_RETURN_ORDERS_LINE_ITEM"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_2


query_2 = f"""SELECT
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.RETURN_ORDERS_LINE_ITEM_ID AS RETURN_ORDERS_LINE_ITEM_ID,
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.RETURN_REASON_ID AS RETURN_REASON_ID,
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.RETURN_ACTION_ID AS RETURN_ACTION_ID,
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.EXPECTED_RECEIVING_CONDITION AS EXPECTED_RECEIVING_CONDITION,
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.IS_RECEIVABLE AS IS_RECEIVABLE,
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.RECEIVING_VARIANCE_CODE AS RECEIVING_VARIANCE_CODE,
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.UNIT_SHIPPING_HANDLING_CHARGE AS UNIT_SHIPPING_HANDLING_CHARGE,
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.TOTAL_FEE AS TOTAL_FEE,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0
WHERE
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID = Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_1.RETURN_ORDERS_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_2")

# COMMAND ----------
# DBTITLE 1, EXP_TRANS_3


query_3 = f"""SELECT
  RETURN_ORDERS_LINE_ITEM_ID AS RETURN_ORDERS_LINE_ITEM_ID,
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_REASON_ID AS RETURN_REASON_ID,
  RETURN_ACTION_ID AS RETURN_ACTION_ID,
  EXPECTED_RECEIVING_CONDITION AS EXPECTED_RECEIVING_CONDITION,
  IS_RECEIVABLE AS IS_RECEIVABLE,
  RECEIVING_VARIANCE_CODE AS RECEIVING_VARIANCE_CODE,
  UNIT_SHIPPING_HANDLING_CHARGE AS UNIT_SHIPPING_HANDLING_CHARGE,
  TOTAL_FEE AS TOTAL_FEE,
  now() AS LOAD_TSTMP_exp,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_RLM_RETURN_ORDERS_LINE_ITEM_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_TRANS_3")

# COMMAND ----------
# DBTITLE 1, OMS_RLM_RETURN_ORDERS_LINE_ITEM_PRE


spark.sql("""INSERT INTO
  OMS_RLM_RETURN_ORDERS_LINE_ITEM_PRE
SELECT
  RETURN_ORDERS_LINE_ITEM_ID AS RETURN_ORDERS_LINE_ITEM_ID,
  RETURN_ORDERS_ID AS RETURN_ORDERS_ID,
  RETURN_REASON_ID AS RETURN_REASON_ID,
  RETURN_ACTION_ID AS RETURN_ACTION_ID,
  EXPECTED_RECEIVING_CONDITION AS EXPECTED_RECEIVING_CONDITION,
  IS_RECEIVABLE AS IS_RECEIVABLE,
  RECEIVING_VARIANCE_CODE AS RECEIVING_VARIANCE_CODE,
  UNIT_SHIPPING_HANDLING_CHARGE AS UNIT_SHIPPING_HANDLING_CHARGE,
  TOTAL_FEE AS TOTAL_FEE,
  LOAD_TSTMP_exp AS LOAD_TSTMP
FROM
  EXP_TRANS_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Rlm_Return_Orders_Line_Item_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Rlm_Return_Orders_Line_Item_Pre", mainWorkflowId, parentName)
