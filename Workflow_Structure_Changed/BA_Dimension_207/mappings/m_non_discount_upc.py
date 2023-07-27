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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_non_discount_upc")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_non_discount_upc", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_prm_415_FLAT_0


query_0 = f"""SELECT
  UPC AS UPC
FROM
  prm_415_FLAT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_prm_415_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_prm_415_FLAT_1


query_1 = f"""SELECT
  UPC AS UPC_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_prm_415_FLAT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_prm_415_FLAT_1")

# COMMAND ----------
# DBTITLE 1, NON_DISCOUNT_UPC


spark.sql("""INSERT INTO
  NON_DISCOUNT_UPC
SELECT
  UPC_ID AS UPC_ID
FROM
  SQ_Shortcut_to_prm_415_FLAT_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_non_discount_upc")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_non_discount_upc", mainWorkflowId, parentName)
