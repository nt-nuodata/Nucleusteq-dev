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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Reason_Code_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Order_Reason_Code_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_LOAD_CTRL_0


query_0 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID
FROM
  OMS_ORDER_LOAD_CTRL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ORDER_LOAD_CTRL_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ORDER_REASON_CODE_1


query_1 = f"""SELECT
  ORDER_REASON_CODE_ID AS ORDER_REASON_CODE_ID,
  ORDER_ID AS ORDER_ID,
  ORDER_LINE_ID AS ORDER_LINE_ID,
  REASON_TYPE AS REASON_TYPE,
  REASON_CODE AS REASON_CODE,
  USER_NAME AS USER_NAME,
  REASON_CODE_DTTM AS REASON_CODE_DTTM
FROM
  ORDER_REASON_CODE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_ORDER_REASON_CODE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ORDER_REASON_CODE_2


query_2 = f"""SELECT
  Shortcut_to_ORDER_REASON_CODE_1.ORDER_REASON_CODE_ID AS ORDER_REASON_CODE_ID,
  Shortcut_to_ORDER_REASON_CODE_1.ORDER_ID AS ORDER_ID,
  Shortcut_to_ORDER_REASON_CODE_1.ORDER_LINE_ID AS ORDER_LINE_ID,
  Shortcut_to_ORDER_REASON_CODE_1.REASON_TYPE AS REASON_TYPE,
  Shortcut_to_ORDER_REASON_CODE_1.REASON_CODE AS REASON_CODE,
  Shortcut_to_ORDER_REASON_CODE_1.USER_NAME AS USER_NAME,
  Shortcut_to_ORDER_REASON_CODE_1.REASON_CODE_DTTM AS REASON_CODE_DTTM,
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0.ORDER_ID AS ORDER_ID1,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ORDER_REASON_CODE_1,
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0
WHERE
  Shortcut_to_OMS_ORDER_LOAD_CTRL_0.ORDER_ID = Shortcut_to_ORDER_REASON_CODE_1.ORDER_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_ORDER_REASON_CODE_2")

# COMMAND ----------
# DBTITLE 1, EXP_TRANS_3


query_3 = f"""SELECT
  ORDER_REASON_CODE_ID AS ORDER_REASON_CODE_ID,
  ORDER_ID AS ORDER_ID,
  ORDER_LINE_ID AS ORDER_LINE_ID,
  REASON_TYPE AS REASON_TYPE,
  REASON_CODE AS REASON_CODE,
  USER_NAME AS USER_NAME,
  REASON_CODE_DTTM AS REASON_CODE_DTTM,
  now() AS LOAD_TSTMP_exp,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ORDER_REASON_CODE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_TRANS_3")

# COMMAND ----------
# DBTITLE 1, OMS_ORDER_REASON_CODE_PRE


spark.sql("""INSERT INTO
  OMS_ORDER_REASON_CODE_PRE
SELECT
  ORDER_REASON_CODE_ID AS ORDER_REASON_CODE_ID,
  ORDER_ID AS ORDER_ID,
  ORDER_LINE_ID AS ORDER_LINE_ID,
  REASON_TYPE AS REASON_TYPE,
  REASON_CODE AS REASON_CODE,
  USER_NAME AS USER_NAME,
  REASON_CODE_DTTM AS REASON_CODE_DTTM,
  LOAD_TSTMP_exp AS LOAD_TSTMP
FROM
  EXP_TRANS_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Reason_Code_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Order_Reason_Code_Pre", mainWorkflowId, parentName)
