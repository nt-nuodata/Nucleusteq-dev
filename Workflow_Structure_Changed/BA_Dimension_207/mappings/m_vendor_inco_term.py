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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_inco_term")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_inco_term", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Inco_Term_0


query_0 = f"""SELECT
  IncoTermCd AS IncoTermCd,
  IncoTermDesc AS IncoTermDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  Inco_Term"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_Inco_Term_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Inco_Term_1


query_1 = f"""SELECT
  IncoTermCd AS IncoTermCd,
  IncoTermDesc AS IncoTermDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Inco_Term_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_Inco_Term_1")

# COMMAND ----------
# DBTITLE 1, LKPTRANS_2


query_2 = f"""SELECT
  VIT.INCO_TERM_CD AS INCO_TERM_CD,
  VIT.INCO_TERM_DESC AS INCO_TERM_DESC,
  SStIT1.IncoTermCd AS IncoTermCd,
  SStIT1.IncoTermDesc AS IncoTermDesc,
  SStIT1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Inco_Term_1 SStIT1
  LEFT JOIN VENDOR_INCO_TERM VIT ON VIT.INCO_TERM_CD = SStIT1.IncoTermCd"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKPTRANS_2")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_3


query_3 = f"""SELECT
  INCO_TERM_CD AS INCO_TERM_CD,
  INCO_TERM_DESC AS INCO_TERM_DESC,
  IncoTermCd AS IncoTermCd,
  IncoTermDesc AS IncoTermDesc,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(isnull(INCO_TERM_CD), 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  LKPTRANS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPDTRANS_3")

# COMMAND ----------
# DBTITLE 1, VENDOR_INCO_TERM


spark.sql("""MERGE INTO VENDOR_INCO_TERM AS TARGET
USING
  UPDTRANS_3 AS SOURCE ON TARGET.INCO_TERM_CD = SOURCE.IncoTermCd
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.INCO_TERM_CD = SOURCE.IncoTermCd,
  TARGET.INCO_TERM_DESC = SOURCE.IncoTermDesc
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.INCO_TERM_DESC = SOURCE.IncoTermDesc THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (TARGET.INCO_TERM_CD, TARGET.INCO_TERM_DESC)
VALUES
  (SOURCE.IncoTermCd, SOURCE.IncoTermDesc)""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_inco_term")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_inco_term", mainWorkflowId, parentName)
