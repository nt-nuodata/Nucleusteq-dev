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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Discount_Type_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_A_Discount_Type_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_A_DISCOUNT_TYPE_0


query_0 = f"""SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME
FROM
  A_DISCOUNT_TYPE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_A_DISCOUNT_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_A_DISCOUNT_TYPE_1


query_1 = f"""SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_A_DISCOUNT_TYPE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_A_DISCOUNT_TYPE_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_A_DISCOUNT_TYPE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, OMS_A_DISCOUNT_TYPE_PRE


spark.sql("""INSERT INTO
  OMS_A_DISCOUNT_TYPE_PRE
SELECT
  DISCOUNT_TYPE_ID AS DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_A_Discount_Type_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_A_Discount_Type_Pre", mainWorkflowId, parentName)
