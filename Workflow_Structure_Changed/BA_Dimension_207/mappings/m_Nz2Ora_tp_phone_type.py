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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_tp_phone_type")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Nz2Ora_tp_phone_type", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_TP_PHONE_TYPE_0


query_0 = f"""SELECT
  TP_PHONE_TYPE_ID AS TP_PHONE_TYPE_ID,
  TP_PHONE_TYPE_DESC AS TP_PHONE_TYPE_DESC
FROM
  TP_PHONE_TYPE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_TP_PHONE_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TP_PHONE_TYPE_1


query_1 = f"""SELECT
  TP_PHONE_TYPE_ID AS TP_PHONE_TYPE_ID,
  TP_PHONE_TYPE_DESC AS TP_PHONE_TYPE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_TP_PHONE_TYPE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_TP_PHONE_TYPE_1")

# COMMAND ----------
# DBTITLE 1, TP_PHONE_TYPE


spark.sql("""INSERT INTO
  TP_PHONE_TYPE
SELECT
  TP_PHONE_TYPE_ID AS TP_PHONE_TYPE_ID,
  TP_PHONE_TYPE_DESC AS TP_PHONE_TYPE_DESC
FROM
  SQ_Shortcut_to_TP_PHONE_TYPE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_tp_phone_type")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Nz2Ora_tp_phone_type", mainWorkflowId, parentName)
