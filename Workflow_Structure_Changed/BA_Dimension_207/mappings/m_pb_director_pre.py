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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_director_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_pb_director_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_BrandDirector_0


query_0 = f"""SELECT
  BrandDirectorId AS BrandDirectorId,
  BrandDirectorName AS BrandDirectorName,
  MerchVpId AS MerchVpId,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  BrandDirector"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_BrandDirector_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_BrandDirector_1


query_1 = f"""SELECT
  BrandDirectorId AS BrandDirectorId,
  BrandDirectorName AS BrandDirectorName,
  MerchVpId AS MerchVpId,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_BrandDirector_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_BrandDirector_1")

# COMMAND ----------
# DBTITLE 1, PB_DIRECTOR_PRE


spark.sql("""INSERT INTO
  PB_DIRECTOR_PRE
SELECT
  BrandDirectorId AS PB_DIRECTOR_ID,
  BrandDirectorName AS PB_DIRECTOR_NAME
FROM
  SQ_Shortcut_to_BrandDirector_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pb_director_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_pb_director_pre", mainWorkflowId, parentName)
