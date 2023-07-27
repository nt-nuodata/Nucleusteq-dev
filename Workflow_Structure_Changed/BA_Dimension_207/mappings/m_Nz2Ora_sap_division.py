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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_sap_division")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Nz2Ora_sap_division", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SAP_DIVISION_0


query_0 = f"""SELECT
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
  MERCH_SVP_ID AS MERCH_SVP_ID,
  MERCH_VP_ID AS MERCH_VP_ID
FROM
  SAP_DIVISION"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SAP_DIVISION_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SAP_DIVISION_1


query_1 = f"""SELECT
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
  MERCH_SVP_ID AS MERCH_SVP_ID,
  MERCH_VP_ID AS MERCH_VP_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SAP_DIVISION_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SAP_DIVISION_1")

# COMMAND ----------
# DBTITLE 1, SAP_DIVISION_Ora


spark.sql("""INSERT INTO
  SAP_DIVISION_Ora
SELECT
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  SAP_DIVISION_DESC AS SAP_DIVISION_DESC,
  MERCH_SVP_ID AS MERCH_SVP_ID,
  MERCH_VP_ID AS MERCH_VP_ID
FROM
  SQ_Shortcut_to_SAP_DIVISION_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_sap_division")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Nz2Ora_sap_division", mainWorkflowId, parentName)
