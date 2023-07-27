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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SAP_T023T_ff")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SAP_T023T_ff", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_T023T_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  SPRAS AS SPRAS,
  MATKL AS MATKL,
  WGBEZ AS WGBEZ,
  WGBEZ60 AS WGBEZ60
FROM
  T023T"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_T023T_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_T023T_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  SPRAS AS SPRAS,
  MATKL AS MATKL,
  WGBEZ AS WGBEZ,
  WGBEZ60 AS WGBEZ60,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_T023T_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_T023T_1")

# COMMAND ----------
# DBTITLE 1, T023T_ff


spark.sql("""INSERT INTO
  T023T_ff
SELECT
  MANDT AS MANDT,
  SPRAS AS SPRAS,
  MATKL AS MATKL,
  WGBEZ AS WGBEZ,
  WGBEZ60 AS WGBEZ60
FROM
  SQ_Shortcut_to_T023T_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SAP_T023T_ff")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SAP_T023T_ff", mainWorkflowId, parentName)
