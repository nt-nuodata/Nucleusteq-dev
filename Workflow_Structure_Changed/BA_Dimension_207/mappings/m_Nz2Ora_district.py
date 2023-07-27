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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_district")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Nz2Ora_district", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DISTRICT_0


query_0 = f"""SELECT
  DISTRICT_ID AS DISTRICT_ID,
  DISTRICT_DESC AS DISTRICT_DESC,
  DISTRICT_SITE_LOGIN_ID AS DISTRICT_SITE_LOGIN_ID,
  DISTRICT_SALON_LOGIN_ID AS DISTRICT_SALON_LOGIN_ID,
  DISTRICT_HOTEL_LOGIN_ID AS DISTRICT_HOTEL_LOGIN_ID
FROM
  DISTRICT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_DISTRICT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DISTRICT_1


query_1 = f"""SELECT
  DISTRICT_ID AS DISTRICT_ID,
  DISTRICT_DESC AS DISTRICT_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_DISTRICT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_DISTRICT_1")

# COMMAND ----------
# DBTITLE 1, DISTRICT_Ora


spark.sql("""INSERT INTO
  DISTRICT_Ora
SELECT
  DISTRICT_ID AS DISTRICT_ID,
  DISTRICT_DESC AS DISTRICT_DESC
FROM
  SQ_Shortcut_to_DISTRICT_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_district")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Nz2Ora_district", mainWorkflowId, parentName)
