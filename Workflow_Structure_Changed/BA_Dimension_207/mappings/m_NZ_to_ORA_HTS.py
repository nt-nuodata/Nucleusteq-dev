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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ_to_ORA_HTS")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ_to_ORA_HTS", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_To_HTS_0


query_0 = f"""SELECT
  HTS_CODE_ID AS HTS_CODE_ID,
  HTS_CODE_DESC AS HTS_CODE_DESC
FROM
  HTS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_HTS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_HTS_1


query_1 = f"""SELECT
  HTS_CODE_ID AS HTS_CODE_ID,
  HTS_CODE_DESC AS HTS_CODE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_HTS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_HTS_1")

# COMMAND ----------
# DBTITLE 1, HTS


spark.sql("""INSERT INTO
  HTS
SELECT
  HTS_CODE_ID AS HTS_CODE_ID,
  HTS_CODE_DESC AS HTS_CODE_DESC
FROM
  SQ_Shortcut_To_HTS_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ_to_ORA_HTS")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ_to_ORA_HTS", mainWorkflowId, parentName)
