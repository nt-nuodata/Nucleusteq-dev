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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Line_Item_Attribute_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Order_Line_Item_Attribute_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ORDER_LOAD_CTRL1_0


query_0 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID
FROM
  OMS_ORDER_LOAD_CTRL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ORDER_LOAD_CTRL1_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ORDER_LINE_ITEM_ATTRIBUTE_1


query_1 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE
FROM
  ORDER_LINE_ITEM_ATTRIBUTE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_ORDER_LINE_ITEM_ATTRIBUTE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ORDER_LINE_ITEM_ATTRIBUTE_2


query_2 = f"""SELECT
  /*+ PARALLEL (4) */
  A.ORDER_ID,
  A.LINE_ITEM_ID,
  A.ATTRIBUTE_NAME,
  A.ATTRIBUTE_SEQ,
  REPLACE(REPLACE(A.ATTRIBUTE_VALUE, CHR(10), ''), '|', '') AS ATTRIBUTE_VALUE
FROM
  {OMS_OWNER_1}.Shortcut_to_ORDER_LINE_ITEM_ATTRIBUTE_1 A
  JOIN {OMS_OWNER_2}.Shortcut_to_OMS_ORDER_LOAD_CTRL1_0 C ON A.ORDER_ID = C.ORDER_ID
WHERE
  A.ATTRIBUTE_NAME IN (
    'Proxy_FirstName',
    'Proxy_LastName',
    'Proxy_AddLine1',
    'Proxy_AddLine2',
    'Proxy_AddLine3',
    'Proxy_Add_Line1',
    'Proxy_Add_Line2',
    'Proxy_Add_Line3',
    'Proxy_City',
    'Proxy_State',
    'Proxy_PostalCd',
    'Proxy_Country',
    'Proxy_Email',
    'Proxy_Phone'
  )"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_ORDER_LINE_ITEM_ATTRIBUTE_2")

# COMMAND ----------
# DBTITLE 1, EXP_TRANS_3


query_3 = f"""SELECT
  ORDER_ID AS ORDER_ID,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  now() AS LOAD_TSTMP_exp,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ORDER_LINE_ITEM_ATTRIBUTE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_TRANS_3")

# COMMAND ----------
# DBTITLE 1, OMS_ORDER_LINE_ITEM_ATTRIBUTE_PRE


spark.sql("""INSERT INTO
  OMS_ORDER_LINE_ITEM_ATTRIBUTE_PRE
SELECT
  ORDER_ID AS ORDER_ID,
  LINE_ITEM_ID AS LINE_ITEM_ID,
  ATTRIBUTE_NAME AS ATTRIBUTE_NAME,
  ATTRIBUTE_SEQ AS ATTRIBUTE_SEQ,
  ATTRIBUTE_VALUE AS ATTRIBUTE_VALUE,
  LOAD_TSTMP_exp AS LOAD_TSTMP
FROM
  EXP_TRANS_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Order_Line_Item_Attribute_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Order_Line_Item_Attribute_Pre", mainWorkflowId, parentName)
