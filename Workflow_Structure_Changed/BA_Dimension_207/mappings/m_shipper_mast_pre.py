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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_shipper_mast_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_shipper_mast_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MAST_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  MATNR AS MATNR,
  WERKS AS WERKS,
  STLAN AS STLAN,
  STLNR AS STLNR,
  STLAL AS STLAL,
  LOSVN AS LOSVN,
  LOSBS AS LOSBS,
  ANDAT AS ANDAT,
  ANNAM AS ANNAM,
  AEDAT AS AEDAT,
  AENAM AS AENAM,
  CSLTY AS CSLTY
FROM
  MAST"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_MAST_0")

# COMMAND ----------
# DBTITLE 1, SQ_MAST_1


query_1 = f"""SELECT
  Shortcut_to_MAST_0.MATNR,
  Shortcut_to_MAST_0.WERKS,
  Shortcut_to_MAST_0.STLAN,
  Shortcut_to_MAST_0.STLNR,
  Shortcut_to_MAST_0.STLAL,
  Shortcut_to_MAST_0.ANDAT,
  Shortcut_to_MAST_0.ANNAM
FROM
  SAPPR3.Shortcut_to_MAST_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_MAST_1")

# COMMAND ----------
# DBTITLE 1, SHIPPER_MAST_PRE


spark.sql("""INSERT INTO
  SHIPPER_MAST_PRE
SELECT
  MATNR AS ARTICLE_NBR,
  WERKS AS SITE_NBR,
  STLAN AS BOM_USAGE_IND,
  STLNR AS BILL_OF_MATERIAL,
  STLAL AS ALTERNATIVE_BOM,
  ANDAT AS RECORD_CREATE_DT,
  ANNAM AS RECORD_CREATE_USER
FROM
  SQ_MAST_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_shipper_mast_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_shipper_mast_pre", mainWorkflowId, parentName)
