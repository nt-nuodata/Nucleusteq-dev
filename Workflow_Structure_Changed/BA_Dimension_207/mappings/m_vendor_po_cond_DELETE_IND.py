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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_po_cond_DELETE_IND")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_po_cond_DELETE_IND", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_PO_COND_PRE_0


query_0 = f"""SELECT
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

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_VENDOR_PO_COND_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_PO_COND1_1


query_1 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  PO_COND_CD AS PO_COND_CD,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  PO_COND_EFF_DT AS PO_COND_EFF_DT,
  PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
  PO_COND_RATE_AMT_HIST AS PO_COND_RATE_AMT_HIST,
  PO_COND_END_DT AS PO_COND_END_DT,
  DELETE_IND AS DELETE_IND,
  LOAD_DT AS LOAD_DT
FROM
  VENDOR_PO_COND"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_VENDOR_PO_COND1_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_VENDOR_SITE_PO_COND_PRE_2


query_2 = f"""SELECT
  COND.VENDOR_ID AS VENDOR_ID,
  COND.PO_COND_CD AS PO_COND_CD,
  COND.PO_COND_EFF_DT AS PO_COND_EFF_DT,
  COND.PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
  NVL(VPC.PO_COND_RATE_AMT, 0) AS PO_COND_RATE_AMT_HIST,
  COND.PO_COND_END_DT AS PO_COND_END_DT,
  COND.DELETE_IND AS DELETE_IND,
  CURRENT_DATE AS LOAD_DT,
  VPC.VENDOR_ID AS VENDOR_ID_OLD,
  COND.VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_VENDOR_PO_COND_PRE_0 COND
  LEFT OUTER JOIN Shortcut_to_VENDOR_PO_COND1_1 VPC ON COND.VENDOR_ID = VPC.VENDOR_ID
  AND COND.PO_COND_CD = VPC.PO_COND_CD
WHERE
  COND.DELETE_IND = 'X'"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_Shortcut_To_VENDOR_SITE_PO_COND_PRE_2")

# COMMAND ----------
# DBTITLE 1, UPD_VENDOR_PO_COND_3


query_3 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  PO_COND_CD AS PO_COND_CD,
  PO_COND_EFF_DT AS PO_COND_EFF_DT,
  PO_COND_RATE_AMT AS PO_COND_RATE_AMT,
  PO_COND_RATE_AMT_HIST AS PO_COND_RATE_AMT_HIST,
  PO_COND_END_DT AS PO_COND_END_DT,
  DELETE_IND AS DELETE_IND,
  LOAD_DT AS LOAD_DT,
  VENDOR_ID_OLD AS VENDOR_ID_OLD,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(ISNULL(VENDOR_ID_OLD), 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  ASQ_Shortcut_To_VENDOR_SITE_PO_COND_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_VENDOR_PO_COND_3")

# COMMAND ----------
# DBTITLE 1, VENDOR_PO_COND


spark.sql("""MERGE INTO VENDOR_PO_COND AS TARGET
USING
  UPD_VENDOR_PO_COND_3 AS SOURCE ON TARGET.PO_COND_CD = SOURCE.PO_COND_CD
  AND TARGET.VENDOR_SUBRANGE_CD = SOURCE.VENDOR_SUBRANGE_CD
  AND TARGET.VENDOR_ID = SOURCE.VENDOR_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.VENDOR_ID = SOURCE.VENDOR_ID,
  TARGET.PO_COND_CD = SOURCE.PO_COND_CD,
  TARGET.VENDOR_SUBRANGE_CD = SOURCE.VENDOR_SUBRANGE_CD,
  TARGET.PO_COND_EFF_DT = SOURCE.PO_COND_EFF_DT,
  TARGET.PO_COND_RATE_AMT = SOURCE.PO_COND_RATE_AMT,
  TARGET.PO_COND_RATE_AMT_HIST = SOURCE.PO_COND_RATE_AMT_HIST,
  TARGET.PO_COND_END_DT = SOURCE.PO_COND_END_DT,
  TARGET.DELETE_IND = SOURCE.DELETE_IND,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PO_COND_EFF_DT = SOURCE.PO_COND_EFF_DT
  AND TARGET.PO_COND_RATE_AMT = SOURCE.PO_COND_RATE_AMT
  AND TARGET.PO_COND_RATE_AMT_HIST = SOURCE.PO_COND_RATE_AMT_HIST
  AND TARGET.PO_COND_END_DT = SOURCE.PO_COND_END_DT
  AND TARGET.DELETE_IND = SOURCE.DELETE_IND
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.VENDOR_ID,
    TARGET.PO_COND_CD,
    TARGET.VENDOR_SUBRANGE_CD,
    TARGET.PO_COND_EFF_DT,
    TARGET.PO_COND_RATE_AMT,
    TARGET.PO_COND_RATE_AMT_HIST,
    TARGET.PO_COND_END_DT,
    TARGET.DELETE_IND,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.VENDOR_ID,
    SOURCE.PO_COND_CD,
    SOURCE.VENDOR_SUBRANGE_CD,
    SOURCE.PO_COND_EFF_DT,
    SOURCE.PO_COND_RATE_AMT,
    SOURCE.PO_COND_RATE_AMT_HIST,
    SOURCE.PO_COND_END_DT,
    SOURCE.DELETE_IND,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_po_cond_DELETE_IND")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_po_cond_DELETE_IND", mainWorkflowId, parentName)
