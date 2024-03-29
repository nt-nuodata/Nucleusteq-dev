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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_att_type_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_att_type_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, ATT_TYPES_0


query_0 = f"""SELECT
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC
FROM
  ATT_TYPES"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("ATT_TYPES_0")

# COMMAND ----------
# DBTITLE 1, SQ_ATT_TYPES_1


query_1 = f"""SELECT
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  ATT_TYPES_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_ATT_TYPES_1")

# COMMAND ----------
# DBTITLE 1, EXP_SAP_ATT_TYPE_2


query_2 = f"""SELECT
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  RTRIM(SAP_ATT_TYPE_DESC) AS OUT_SAP_ATT_TYPE_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_ATT_TYPES_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_SAP_ATT_TYPE_2")

# COMMAND ----------
# DBTITLE 1, SAP_ATT_TYPE_PRE


spark.sql("""INSERT INTO
  SAP_ATT_TYPE_PRE
SELECT
  SAP_ATT_TYPE_ID AS SAP_ATT_TYPE_ID,
  OUT_SAP_ATT_TYPE_DESC AS SAP_ATT_TYPE_DESC
FROM
  EXP_SAP_ATT_TYPE_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_att_type_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_att_type_pre", mainWorkflowId, parentName)
