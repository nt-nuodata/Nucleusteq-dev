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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_PO_Line_Item_Ref_Fields_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_PO_Line_Item_Ref_Fields_Pre", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1


query_1 = f"""SELECT
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
  REF_FIELD1 AS REF_FIELD1,
  REF_FIELD2 AS REF_FIELD2,
  REF_FIELD3 AS REF_FIELD3,
  REF_FIELD4 AS REF_FIELD4,
  REF_FIELD5 AS REF_FIELD5,
  REF_FIELD6 AS REF_FIELD6,
  REF_FIELD7 AS REF_FIELD7,
  REF_FIELD8 AS REF_FIELD8,
  REF_FIELD9 AS REF_FIELD9,
  REF_FIELD10 AS REF_FIELD10,
  REF_NUM1 AS REF_NUM1,
  REF_NUM2 AS REF_NUM2,
  REF_NUM3 AS REF_NUM3,
  REF_NUM4 AS REF_NUM4,
  REF_NUM5 AS REF_NUM5,
  RX_STATUS AS RX_STATUS,
  RX_ASSIGNED_TO AS RX_ASSIGNED_TO,
  RX_ASSIGNED_DTTM AS RX_ASSIGNED_DTTM,
  RX_VET_INQUIRY AS RX_VET_INQUIRY
FROM
  PO_LINE_ITEM_REF_FIELDS"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PO_LINE_ITEM_REF_FIELDS_2


query_2 = f"""SELECT
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD1 AS REF_FIELD1,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD2 AS REF_FIELD2,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD3 AS REF_FIELD3,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD4 AS REF_FIELD4,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD5 AS REF_FIELD5,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD6 AS REF_FIELD6,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD7 AS REF_FIELD7,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD8 AS REF_FIELD8,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD9 AS REF_FIELD9,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_FIELD10 AS REF_FIELD10,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_NUM1 AS REF_NUM1,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_NUM2 AS REF_NUM2,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_NUM3 AS REF_NUM3,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_NUM4 AS REF_NUM4,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.REF_NUM5 AS REF_NUM5,
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID_CTL,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.RX_STATUS AS RX_STATUS,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.RX_ASSIGNED_TO AS RX_ASSIGNED_TO,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.RX_ASSIGNED_DTTM AS RX_ASSIGNED_DTTM,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.RX_VET_INQUIRY AS RX_VET_INQUIRY,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0,
  Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1
WHERE
  Shortcut_to_OMS_PURCH_ORDER_LOAD_CTRL_0.PURCHASE_ORDERS_ID = Shortcut_to_PO_LINE_ITEM_REF_FIELDS_1.PURCHASE_ORDERS_ID"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_PO_LINE_ITEM_REF_FIELDS_2")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_3


query_3 = f"""SELECT
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
  REF_FIELD1 AS REF_FIELD1,
  REF_FIELD2 AS REF_FIELD2,
  REF_FIELD3 AS REF_FIELD3,
  REF_FIELD4 AS REF_FIELD4,
  REF_FIELD5 AS REF_FIELD5,
  REF_FIELD6 AS REF_FIELD6,
  REF_FIELD7 AS REF_FIELD7,
  REF_FIELD8 AS REF_FIELD8,
  REF_FIELD9 AS REF_FIELD9,
  REF_FIELD10 AS REF_FIELD10,
  REF_NUM1 AS REF_NUM1,
  REF_NUM2 AS REF_NUM2,
  REF_NUM3 AS REF_NUM3,
  REF_NUM4 AS REF_NUM4,
  REF_NUM5 AS REF_NUM5,
  now() AS o_LOAD_TSTMP,
  RX_STATUS AS RX_STATUS,
  RX_ASSIGNED_TO AS RX_ASSIGNED_TO,
  RX_ASSIGNED_DTTM AS RX_ASSIGNED_DTTM,
  RX_VET_INQUIRY AS RX_VET_INQUIRY,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PO_LINE_ITEM_REF_FIELDS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_LOAD_TSTMP_3")

# COMMAND ----------
# DBTITLE 1, OMS_PO_LINE_ITEM_REF_FIELDS_PRE


spark.sql("""INSERT INTO
  OMS_PO_LINE_ITEM_REF_FIELDS_PRE
SELECT
  PURCHASE_ORDERS_LINE_ITEM_ID AS PURCHASE_ORDERS_LINE_ITEM_ID,
  PURCHASE_ORDERS_ID AS PURCHASE_ORDERS_ID,
  REF_FIELD1 AS REF_FIELD1,
  REF_FIELD2 AS REF_FIELD2,
  REF_FIELD3 AS REF_FIELD3,
  REF_FIELD4 AS REF_FIELD4,
  REF_FIELD5 AS REF_FIELD5,
  REF_FIELD6 AS REF_FIELD6,
  REF_FIELD7 AS REF_FIELD7,
  REF_FIELD8 AS REF_FIELD8,
  REF_FIELD9 AS REF_FIELD9,
  REF_FIELD10 AS REF_FIELD10,
  REF_NUM1 AS REF_NUM1,
  REF_NUM2 AS REF_NUM2,
  REF_NUM3 AS REF_NUM3,
  REF_NUM4 AS REF_NUM4,
  REF_NUM5 AS REF_NUM5,
  RX_STATUS AS RX_STATUS,
  RX_ASSIGNED_TO AS RX_ASSIGNED_TO,
  RX_ASSIGNED_DTTM AS RX_ASSIGNED_DTTM,
  RX_VET_INQUIRY AS RX_VET_INQUIRY,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_PO_Line_Item_Ref_Fields_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_PO_Line_Item_Ref_Fields_Pre", mainWorkflowId, parentName)
