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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_PURCH_GROUP_VENDOR")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_PURCH_GROUP_VENDOR", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PURCH_GROUP_VENDOR_0


query_0 = f"""SELECT
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  VENDOR_ID AS VENDOR_ID
FROM
  PURCH_GROUP_VENDOR"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PURCH_GROUP_VENDOR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PURCH_GROUP_VENDOR_1


query_1 = f"""SELECT
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  VENDOR_ID AS VENDOR_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PURCH_GROUP_VENDOR_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PURCH_GROUP_VENDOR_1")

# COMMAND ----------
# DBTITLE 1, Exp_Conversion_2


query_2 = f"""SELECT
  TO_BIGINT(PURCH_GROUP_ID) AS PURCH_GROUP_ID,
  TO_BIGINT(VENDOR_ID) AS VENDOR_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PURCH_GROUP_VENDOR_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Exp_Conversion_2")

# COMMAND ----------
# DBTITLE 1, PURCH_GROUP_VENDOR


spark.sql("""INSERT INTO
  PURCH_GROUP_VENDOR
SELECT
  PURCH_GROUP_ID AS PURCH_GROUP_ID,
  VENDOR_ID AS VENDOR_ID
FROM
  Exp_Conversion_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_PURCH_GROUP_VENDOR")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_PURCH_GROUP_VENDOR", mainWorkflowId, parentName)
