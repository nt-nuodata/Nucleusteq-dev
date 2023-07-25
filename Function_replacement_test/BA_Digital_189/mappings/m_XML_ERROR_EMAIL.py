# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ../WorkflowUtility

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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_XML_ERROR_EMAIL")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_XML_ERROR_EMAIL", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, src_XML_INVALID_PAYLOAD_FF_0


query_0 = f"""SELECT
  Error_Message AS Error_Message,
  Invalid_Data AS Invalid_Data
FROM
  XML_INVALID_PAYLOAD_FF"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("src_XML_INVALID_PAYLOAD_FF_0")

# COMMAND ----------
# DBTITLE 1, sq_XML_INVALID_PAYLOAD_FF_1


query_1 = f"""SELECT
  Error_Message AS Error_Message,
  Invalid_Data AS Invalid_Data,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  src_XML_INVALID_PAYLOAD_FF_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("sq_XML_INVALID_PAYLOAD_FF_1")

# COMMAND ----------
# DBTITLE 1, fil_FALSE_2


query_2 = f"""SELECT
  Error_Message AS Error_Message,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  sq_XML_INVALID_PAYLOAD_FF_1
WHERE
  FALSE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("fil_FALSE_2")

# COMMAND ----------
# DBTITLE 1, DUMMY_TARGET


spark.sql("""INSERT INTO
  DUMMY_TARGET
SELECT
  TXN_DT AS COMMENT,
  Error_Message AS COMMENT
FROM
  fil_FALSE_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_XML_ERROR_EMAIL")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_XML_ERROR_EMAIL", mainWorkflowId, parentName)
