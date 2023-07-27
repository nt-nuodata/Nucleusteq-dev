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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_manager_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_pb_manager_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_BrandManager_0


query_0 = f"""SELECT
  BrandManagerId AS BrandManagerId,
  BrandManagerName AS BrandManagerName,
  BrandDirectorId AS BrandDirectorId,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  BrandManager"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_BrandManager_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_BrandManager_1


query_1 = f"""SELECT
  BrandManagerId AS BrandManagerId,
  BrandManagerName AS BrandManagerName,
  BrandDirectorId AS BrandDirectorId,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_BrandManager_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_BrandManager_1")

# COMMAND ----------
# DBTITLE 1, PB_MANAGER_PRE


spark.sql("""INSERT INTO
  PB_MANAGER_PRE
SELECT
  BrandManagerId AS PB_MANAGER_ID,
  BrandManagerName AS PB_MANAGER_NAME,
  BrandDirectorId AS PB_DIRECTOR_ID
FROM
  SQ_Shortcut_to_BrandManager_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_manager_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_pb_manager_pre", mainWorkflowId, parentName)
