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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_reason")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_reason", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_REASON_0


query_0 = f"""SELECT
  REASON_ID AS REASON_ID,
  SALES_TYPE_ID AS SALES_TYPE_ID,
  REASON_DESC AS REASON_DESC
FROM
  REASON"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_REASON_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_REASON_1


query_1 = f"""SELECT
  REASON_ID AS REASON_ID,
  SALES_TYPE_ID AS SALES_TYPE_ID,
  REASON_DESC AS REASON_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_REASON_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_REASON_1")

# COMMAND ----------
# DBTITLE 1, REASON


spark.sql("""INSERT INTO
  REASON
SELECT
  REASON_ID AS REASON_ID,
  SALES_TYPE_ID AS SALES_TYPE_ID,
  REASON_DESC AS REASON_DESC
FROM
  SQ_Shortcut_to_REASON_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_reason")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_reason", mainWorkflowId, parentName)
