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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_PRIMARY_VENDOR")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_PRIMARY_VENDOR", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_To_PRIMARY_VENDOR_0


query_0 = f"""SELECT
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME
FROM
  PRIMARY_VENDOR"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_PRIMARY_VENDOR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_PRIMARY_VENDOR_1


query_1 = f"""SELECT
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_PRIMARY_VENDOR_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_PRIMARY_VENDOR_1")

# COMMAND ----------
# DBTITLE 1, PRIMARY_VENDOR


spark.sql("""INSERT INTO
  PRIMARY_VENDOR
SELECT
  PRIMARY_VENDOR_ID AS PRIMARY_VENDOR_ID,
  PRIMARY_VENDOR_NAME AS PRIMARY_VENDOR_NAME
FROM
  SQ_Shortcut_To_PRIMARY_VENDOR_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_PRIMARY_VENDOR")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_PRIMARY_VENDOR", mainWorkflowId, parentName)
