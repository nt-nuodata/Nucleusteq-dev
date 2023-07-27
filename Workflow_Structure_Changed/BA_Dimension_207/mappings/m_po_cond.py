# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_po_cond")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_po_cond", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_SITE_PO_COND_PRE_0


query_0 = f"""SELECT
  PO_COND_CD AS PO_COND_CD,
  PURCH_ORG_CD AS PURCH_ORG_CD,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  PO_COND_END_DT AS PO_COND_END_DT,
  DELETE_IND AS DELETE_IND,
  PO_COND_EFF_DT AS PO_COND_EFF_DT,
  PO_COND_REC_NBR AS PO_COND_REC_NBR,
  PO_COND_RATE_AMT AS PO_COND_RATE_AMT
FROM
  VENDOR_SITE_PO_COND_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_VENDOR_SITE_PO_COND_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PO_COND1_1


query_1 = f"""SELECT
  PO_COND_CD AS PO_COND_CD,
  PO_COND_DESC AS PO_COND_DESC
FROM
  PO_COND"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PO_COND1_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_PO_COND_2


query_2 = f"""SELECT
  VPC.PO_COND_CD AS PO_COND_CD,
  'UNKNOWN' AS PO_COND_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  VENDOR_PO_COND_PRE VPC
UNION ALL
SELECT
  VSPC.PO_COND_CD AS PO_COND_CD,
  'UNKNOWN' AS PO_COND_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  VENDOR_SITE_PO_COND_PRE VSPC
EXCEPT
SELECT
  PC.PO_COND_CD AS PO_COND_CD,
  'UNKNOWN' AS PO_COND_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PO_COND1_1 PC"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_Shortcut_to_PO_COND_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_PO_COND_PRE_3


query_3 = f"""SELECT
  PO_COND_CD AS PO_COND_CD,
  PURCH_ORG_CD AS PURCH_ORG_CD,
  VENDOR_ID AS VENDOR_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  PO_COND_END_DT AS PO_COND_END_DT,
  DELETE_IND AS DELETE_IND,
  PO_COND_EFF_DT AS PO_COND_EFF_DT,
  PO_COND_REC_NBR AS PO_COND_REC_NBR,
  PO_COND_RATE_AMT AS PO_COND_RATE_AMT
FROM
  VENDOR_PO_COND_PRE"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_VENDOR_PO_COND_PRE_3")

# COMMAND ----------
# DBTITLE 1, PO_COND


spark.sql("""INSERT INTO
  PO_COND
SELECT
  PO_COND_CD AS PO_COND_CD,
  PO_COND_DESC AS PO_COND_DESC
FROM
  ASQ_Shortcut_to_PO_COND_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_po_cond")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_po_cond", mainWorkflowId, parentName)
