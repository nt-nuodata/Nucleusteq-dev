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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_uom_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_uom_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_UOM_0


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

df_0.createOrReplaceTempView("Shortcut_To_SKU_UOM_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_UOM_1


query_1 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  UOM_CD AS UOM_CD,
  DELETE_IND AS DELETE_IND,
  UOM_NUMERATOR AS UOM_NUMERATOR,
  UOM_DENOMINATOR AS UOM_DENOMINATOR,
  LENGTH_AMT AS LENGTH_AMT,
  WIDTH_AMT AS WIDTH_AMT,
  HEIGHT_AMT AS HEIGHT_AMT,
  DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
  VOLUME_AMT AS VOLUME_AMT,
  VOLUME_UNIT_DESC AS VOLUME_UNIT_DESC,
  WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
  WEIGHT_UNIT_DESC AS WEIGHT_UNIT_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SKU_UOM_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_SKU_UOM_1")

# COMMAND ----------
# DBTITLE 1, FLT_REMOVE_RF_SKUS_2


query_2 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  UOM_CD AS UOM_CD,
  DELETE_IND AS DELETE_IND,
  UOM_NUMERATOR AS UOM_NUMERATOR,
  UOM_DENOMINATOR AS UOM_DENOMINATOR,
  LENGTH_AMT AS LENGTH_AMT,
  WIDTH_AMT AS WIDTH_AMT,
  HEIGHT_AMT AS HEIGHT_AMT,
  DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
  VOLUME_AMT AS VOLUME_AMT,
  VOLUME_UNIT_DESC AS VOLUME_UNIT_DESC,
  WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
  WEIGHT_UNIT_DESC AS WEIGHT_UNIT_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SKU_UOM_1
WHERE
  IFF(IS_NUMBER(SKU_NBR), TRUE, FALSE)"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("FLT_REMOVE_RF_SKUS_2")

# COMMAND ----------
# DBTITLE 1, SKU_UOM_PRE


spark.sql("""INSERT INTO
  SKU_UOM_PRE
SELECT
  SKU_NBR AS SKU_NBR,
  UOM_CD AS UOM_CD,
  DELETE_IND AS DELETE_IND,
  UOM_NUMERATOR AS UOM_NUMERATOR,
  UOM_DENOMINATOR AS UOM_DENOMINATOR,
  LENGTH_AMT AS LENGTH_AMT,
  WIDTH_AMT AS WIDTH_AMT,
  HEIGHT_AMT AS HEIGHT_AMT,
  DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
  VOLUME_AMT AS VOLUME_AMT,
  VOLUME_UNIT_DESC AS VOLUME_UNIT_DESC,
  WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
  WEIGHT_UNIT_DESC AS WEIGHT_UNIT_DESC
FROM
  FLT_REMOVE_RF_SKUS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_uom_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_uom_pre", mainWorkflowId, parentName)
