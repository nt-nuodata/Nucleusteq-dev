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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_payment_type")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_payment_type", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PAYMENT_TYPE2_0


query_0 = f"""SELECT
  PAYMENT_TYPE_ID AS PAYMENT_TYPE_ID,
  PAYMENT_TYPE_DESC AS PAYMENT_TYPE_DESC,
  TENDER_TYPE_ID AS TENDER_TYPE_ID
FROM
  PAYMENT_TYPE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PAYMENT_TYPE2_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PAYMENT_TYPE_1


query_1 = f"""SELECT
  PAYMENT_TYPE_ID AS PAYMENT_TYPE_ID,
  PAYMENT_TYPE_DESC AS PAYMENT_TYPE_DESC,
  TENDER_TYPE_ID AS TENDER_TYPE_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PAYMENT_TYPE2_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PAYMENT_TYPE_1")

# COMMAND ----------
# DBTITLE 1, PAYMENT_TYPE


spark.sql("""INSERT INTO
  PAYMENT_TYPE
SELECT
  PAYMENT_TYPE_ID AS PAYMENT_TYPE_ID,
  PAYMENT_TYPE_DESC AS PAYMENT_TYPE_DESC,
  TENDER_TYPE_ID AS TENDER_TYPE_ID
FROM
  SQ_Shortcut_to_PAYMENT_TYPE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_payment_type")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_payment_type", mainWorkflowId, parentName)
