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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_sap_category")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Nz2Ora_sap_category", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SAP_CATEGORY_0


query_0 = f"""SELECT
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
  SAP_CLASS_ID AS SAP_CLASS_ID,
  GL_CATEGORY_CD AS GL_CATEGORY_CD,
  SAP_PRICING_CATEGORY_ID AS SAP_PRICING_CATEGORY_ID,
  UPD_TSTMP AS UPD_TSTMP
FROM
  SAP_CATEGORY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SAP_CATEGORY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SAP_CATEGORY_1


query_1 = f"""SELECT
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
  SAP_CLASS_ID AS SAP_CLASS_ID,
  GL_CATEGORY_CD AS GL_CATEGORY_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SAP_CATEGORY_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SAP_CATEGORY_1")

# COMMAND ----------
# DBTITLE 1, SAP_CATEGORY_Ora


spark.sql("""INSERT INTO
  SAP_CATEGORY_Ora
SELECT
  SAP_CATEGORY_ID AS SAP_CATEGORY_ID,
  SAP_CATEGORY_DESC AS SAP_CATEGORY_DESC,
  SAP_CLASS_ID AS SAP_CLASS_ID,
  GL_CATEGORY_CD AS GL_CATEGORY_CD
FROM
  SQ_Shortcut_to_SAP_CATEGORY_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Nz2Ora_sap_category")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Nz2Ora_sap_category", mainWorkflowId, parentName)
