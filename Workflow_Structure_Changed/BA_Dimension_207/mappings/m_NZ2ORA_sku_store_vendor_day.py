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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_sku_store_vendor_day")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_sku_store_vendor_day", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_STORE_VENDOR_DAY_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  DELETE_IND AS DELETE_IND,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCURE_STORE_NBR AS PROCURE_STORE_NBR,
  LOAD_DT AS LOAD_DT
FROM
  SKU_STORE_VENDOR_DAY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SKU_STORE_VENDOR_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_STORE_VENDOR_DAY_1


query_1 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  DELETE_IND AS DELETE_IND,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCURE_STORE_NBR AS PROCURE_STORE_NBR,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SKU_STORE_VENDOR_DAY_0
WHERE
  store_nbr in (2940, 2941, 2801, 2859, 2877)
  and delete_ind <> 'X'"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_SKU_STORE_VENDOR_DAY_1")

# COMMAND ----------
# DBTITLE 1, SKU_STORE_VENDOR_DAY


spark.sql("""INSERT INTO
  SKU_STORE_VENDOR_DAY
SELECT
  SKU_NBR AS SKU_NBR,
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  VENDOR_ID AS VENDOR_ID,
  DELETE_IND AS DELETE_IND,
  DELETE_IND AS DELETE_IND,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCURE_SITE_ID AS PROCURE_STORE_NBR,
  PROCURE_STORE_NBR AS PROCURE_STORE_NBR,
  o_CURRENT_DATE AS LOAD_DT,
  LOAD_DT AS LOAD_DT
FROM
  SQ_Shortcut_To_SKU_STORE_VENDOR_DAY_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_sku_store_vendor_day")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_sku_store_vendor_day", mainWorkflowId, parentName)
