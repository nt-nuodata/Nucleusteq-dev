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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_att_code_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_att_code_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, ATT_CODES_0


query_0 = f"""SELECT
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_CODE_DESC AS SAP_ATT_CODE_DESC
FROM
  ATT_CODES"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("ATT_CODES_0")

# COMMAND ----------
# DBTITLE 1, SQ_ATT_CODES_1


query_1 = f"""SELECT
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_CODE_DESC AS SAP_ATT_CODE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  ATT_CODES_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_ATT_CODES_1")

# COMMAND ----------
# DBTITLE 1, EXP_SAP_ATT_CODE_2


query_2 = f"""SELECT
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  SAP_ATT_CODE_DESC AS SAP_ATT_CODE_DESC,
  RTRIM(SAP_ATT_CODE_DESC) AS OUT_SAP_ATT_CODE_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_ATT_CODES_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_SAP_ATT_CODE_2")

# COMMAND ----------
# DBTITLE 1, SAP_ATT_CODE_PRE


spark.sql("""INSERT INTO
  SAP_ATT_CODE_PRE
SELECT
  SAP_ATT_CODE_ID AS SAP_ATT_CODE_ID,
  OUT_SAP_ATT_CODE_DESC AS SAP_ATT_CODE_DESC
FROM
  EXP_SAP_ATT_CODE_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_att_code_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_att_code_pre", mainWorkflowId, parentName)