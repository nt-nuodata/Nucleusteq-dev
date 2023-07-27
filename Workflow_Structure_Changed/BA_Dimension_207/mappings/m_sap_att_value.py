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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_att_value")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_att_value", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SAP_ATT_VALUE_PRE_0


query_0 = f"""SELECT
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  LANGUAGE_CD AS LANGUAGE_CD,
  SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC
FROM
  SAP_ATT_VALUE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SAP_ATT_VALUE_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_SAP_ATT_VALUE_PRE_1


query_1 = f"""SELECT
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SAP_ATT_VALUE_PRE_0
EXCEPT
SELECT
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  SAP_ATT_VALUE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SAP_ATT_VALUE_PRE_1")

# COMMAND ----------
# DBTITLE 1, SAP_ATT_VALUE


spark.sql("""INSERT INTO
  SAP_ATT_VALUE
SELECT
  SAP_ATT_VALUE_ID AS SAP_ATT_VALUE_ID,
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_VALUE_DESC AS SAP_ATT_VALUE_DESC
FROM
  ASQ_Shortcut_to_SAP_ATT_VALUE_PRE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_att_value")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_att_value", mainWorkflowId, parentName)
