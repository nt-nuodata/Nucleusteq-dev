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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_sap_dept")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Nz2Ora_sap_dept", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SAP_DEPT_0


query_0 = f"""SELECT
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DEPT_DESC AS SAP_DEPT_DESC,
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  MERCH_DIVISIONAL_ID AS MERCH_DIVISIONAL_ID,
  BUYER_ID AS BUYER_ID
FROM
  SAP_DEPT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SAP_DEPT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SAP_DEPT_1


query_1 = f"""SELECT
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DEPT_DESC AS SAP_DEPT_DESC,
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  MERCH_DIVISIONAL_ID AS MERCH_DIVISIONAL_ID,
  BUYER_ID AS BUYER_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SAP_DEPT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SAP_DEPT_1")

# COMMAND ----------
# DBTITLE 1, SAP_DEPT


spark.sql("""INSERT INTO
  SAP_DEPT
SELECT
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DEPT_DESC AS SAP_DEPT_DESC,
  SAP_DIVISION_ID AS SAP_DIVISION_ID,
  MERCH_DIVISIONAL_ID AS MERCH_DIVISIONAL_ID,
  BUYER_ID AS BUYER_ID
FROM
  SQ_Shortcut_to_SAP_DEPT_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_sap_dept")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Nz2Ora_sap_dept", mainWorkflowId, parentName)
