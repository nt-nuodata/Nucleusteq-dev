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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_PURCH_GROUP_VENDOR_INSERT")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_PURCH_GROUP_VENDOR_INSERT", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DM_PG_VENDOR_0


query_0 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  PURCH_GROUP_ID AS PURCH_GROUP_ID
FROM
  DM_PG_VENDOR"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_DM_PG_VENDOR_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_DM_PG_VENDOR_1


query_1 = f"""SELECT
  Shortcut_to_DM_PG_VENDOR_0.VENDOR_ID AS VENDOR_ID,
  Shortcut_to_DM_PG_VENDOR_0.PURCH_GROUP_ID AS PURCH_GROUP_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  NCAST.Shortcut_to_DM_PG_VENDOR_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_DM_PG_VENDOR_1")

# COMMAND ----------
# DBTITLE 1, PURCH_GROUP_VENDOR


spark.sql("""INSERT INTO
  PURCH_GROUP_VENDOR
SELECT
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  VENDOR_ID AS VENDOR_ID
FROM
  ASQ_Shortcut_to_DM_PG_VENDOR_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_PURCH_GROUP_VENDOR_INSERT")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_PURCH_GROUP_VENDOR_INSERT", mainWorkflowId, parentName)
