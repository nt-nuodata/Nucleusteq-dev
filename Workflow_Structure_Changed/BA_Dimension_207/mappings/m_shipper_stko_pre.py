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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_shipper_stko_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_shipper_stko_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_STKO_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  STLTY AS STLTY,
  STLNR AS STLNR,
  STLAL AS STLAL,
  STKOZ AS STKOZ,
  DATUV AS DATUV,
  TECHV AS TECHV,
  AENNR AS AENNR,
  LKENZ AS LKENZ,
  LOEKZ AS LOEKZ,
  VGKZL AS VGKZL,
  ANDAT AS ANDAT,
  ANNAM AS ANNAM,
  AEDAT AS AEDAT,
  AENAM AS AENAM,
  BMEIN AS BMEIN,
  BMENG AS BMENG,
  CADKZ AS CADKZ,
  LABOR AS LABOR,
  LTXSP AS LTXSP,
  STKTX AS STKTX,
  STLST AS STLST,
  WRKAN AS WRKAN,
  DVDAT AS DVDAT,
  DVNAM AS DVNAM,
  AEHLP AS AEHLP,
  ALEKZ AS ALEKZ,
  GUIDX AS GUIDX
FROM
  STKO"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_STKO_0")

# COMMAND ----------
# DBTITLE 1, SQ_STKO_1


query_1 = f"""SELECT
  Shortcut_to_STKO_0.STLTY,
  Shortcut_to_STKO_0.STLNR,
  Shortcut_to_STKO_0.STLAL,
  Shortcut_to_STKO_0.STKOZ,
  Shortcut_to_STKO_0.DATUV,
  Shortcut_to_STKO_0.ANDAT,
  Shortcut_to_STKO_0.ANNAM,
  Shortcut_to_STKO_0.ALEKZ,
  Shortcut_to_STKO_0.GUIDX
FROM
  SAPPR3.Shortcut_to_STKO_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_STKO_1")

# COMMAND ----------
# DBTITLE 1, SHIPPER_STKO_PRE


spark.sql("""INSERT INTO
  SHIPPER_STKO_PRE
SELECT
  STLTY AS BOM_CATEGORY_ID,
  STLNR AS BILL_OF_MATERIAL,
  STLAL AS ALTERNATIVE_BOM,
  STKOZ AS INTERNAL_CNTR,
  DATUV AS VALID_FROM_DT,
  ANDAT AS RECORD_CREATE_DT,
  ANNAM AS RECORD_CREATE_USER,
  ALEKZ AS ALE_IND,
  GUIDX AS BOM_HEADER_CHANGE_STATUS_ID
FROM
  SQ_STKO_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_shipper_stko_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_shipper_stko_pre", mainWorkflowId, parentName)
