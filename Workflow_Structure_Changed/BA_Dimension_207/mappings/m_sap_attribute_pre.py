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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_attribute_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_attribute_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SAP_ATTRIBUTE_FILE_0


query_0 = f"""SELECT
  DELETE_FLAG AS DELETE_FLAG,
  ARTICLE AS ARTICLE,
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID
FROM
  SAP_ATTRIBUTE_FILE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SAP_ATTRIBUTE_FILE_0")

# COMMAND ----------
# DBTITLE 1, SQ_SAP_ATTRIBUTE_FILE_1


query_1 = f"""SELECT
  ARTICLE AS ARTICLE,
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  DELETE_FLAG AS DELETE_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SAP_ATTRIBUTE_FILE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_SAP_ATTRIBUTE_FILE_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_COLUMNS_2


query_2 = f"""SELECT
  ARTICLE AS ARTICLE,
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  DELETE_FLAG AS DELETE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_SAP_ATTRIBUTE_FILE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_COLUMNS_2")

# COMMAND ----------
# DBTITLE 1, SAP_ATTRIBUTE_PRE


spark.sql("""INSERT INTO
  SAP_ATTRIBUTE_PRE
SELECT
  ARTICLE AS SKU_NBR,
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  DELETE_FLAG AS DELETE_FLAG
FROM
  EXP_LOAD_COLUMNS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_attribute_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_attribute_pre", mainWorkflowId, parentName)
