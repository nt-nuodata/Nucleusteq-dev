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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_SKU_UOM")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NZ2ORA_SKU_UOM", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, TRUNC_PROD_TABLES


Stored Procedure Transformation not supported

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_UOM_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  UOM_CD AS UOM_CD,
  UOM_NUMERATOR AS UOM_NUMERATOR,
  UOM_DENOMINATOR AS UOM_DENOMINATOR,
  LENGTH_AMT AS LENGTH_AMT,
  WIDTH_AMT AS WIDTH_AMT,
  HEIGHT_AMT AS HEIGHT_AMT,
  DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
  VOLUME_AMT AS VOLUME_AMT,
  VOLUME_UOM_CD AS VOLUME_UOM_CD,
  WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
  WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
  WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  SCM_VOLUME_UOM_CD AS SCM_VOLUME_UOM_CD,
  SCM_VOLUME_AMT AS SCM_VOLUME_AMT,
  SCM_WEIGHT_UOM_CD AS SCM_WEIGHT_UOM_CD,
  SCM_WEIGHT_NET_AMT AS SCM_WEIGHT_NET_AMT,
  DELETE_DT AS DELETE_DT,
  LOAD_DT AS LOAD_DT
FROM
  SKU_UOM"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SKU_UOM_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_UOM_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  UOM_CD AS UOM_CD,
  UOM_NUMERATOR AS UOM_NUMERATOR,
  UOM_DENOMINATOR AS UOM_DENOMINATOR,
  LENGTH_AMT AS LENGTH_AMT,
  WIDTH_AMT AS WIDTH_AMT,
  HEIGHT_AMT AS HEIGHT_AMT,
  DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
  VOLUME_AMT AS VOLUME_AMT,
  VOLUME_UOM_CD AS VOLUME_UOM_CD,
  WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
  WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
  WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  DELETE_DT AS DELETE_DT,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_UOM_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_UOM_1")

# COMMAND ----------
# DBTITLE 1, SKU_UOM


spark.sql("""INSERT INTO
  SKU_UOM
SELECT
  PRODUCT_ID AS PRODUCT_ID,
  UOM_CD AS UOM_CD,
  UOM_NUMERATOR AS UOM_NUMERATOR,
  UOM_DENOMINATOR AS UOM_DENOMINATOR,
  LENGTH_AMT AS LENGTH_AMT,
  WIDTH_AMT AS WIDTH_AMT,
  HEIGHT_AMT AS HEIGHT_AMT,
  DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
  VOLUME_AMT AS VOLUME_AMT,
  VOLUME_UOM_CD AS VOLUME_UOM_CD,
  WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
  WEIGHT_UOM_CD AS WEIGHT_UOM_CD,
  WEIGHT_NET_AMT AS WEIGHT_NET_AMT,
  DELETE_DT AS DELETE_DT,
  LOAD_DT AS LOAD_DT
FROM
  SQ_Shortcut_to_SKU_UOM_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NZ2ORA_SKU_UOM")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NZ2ORA_SKU_UOM", mainWorkflowId, parentName)
