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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_minimum")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_minimum", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_VENDOR_MINIMUM_PRE_0


query_0 = f"""SELECT
  VENDOR_ID AS VENDOR_ID,
  STORE_NBR AS STORE_NBR,
  SKU_NBR AS SKU_NBR,
  SEARCH_LEVEL AS SEARCH_LEVEL,
  DELETE_IND AS DELETE_IND,
  MIN_AMT AS MIN_AMT,
  MAX_AMT AS MAX_AMT,
  MIN_QTY AS MIN_QTY,
  MIN_QTY_UOM_CD AS MIN_QTY_UOM_CD,
  MAX_QTY AS MAX_QTY,
  MAX_QTY_UOM_CD AS MAX_QTY_UOM_CD,
  MIN_VOLUME_CFT AS MIN_VOLUME_CFT,
  MAX_VOLUME_CFT AS MAX_VOLUME_CFT,
  MIN_WEIGHT_LBS AS MIN_WEIGHT_LBS,
  MAX_WEIGHT_LBS AS MAX_WEIGHT_LBS,
  LOAD_DT AS LOAD_DT
FROM
  VENDOR_MINIMUM_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_VENDOR_MINIMUM_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_VENDOR_MINIMUM_PRE_1


query_1 = f"""SELECT
  VMP.VENDOR_ID AS VENDOR_ID,
  SITE.LOCATION_ID AS LOCATION_ID,
  SKU.PRODUCT_ID AS PRODUCT_ID,
  VMP.SEARCH_LEVEL AS SEARCH_LEVEL,
  VMP.MIN_AMT AS MIN_AMT,
  VMP.MAX_AMT AS MAX_AMT,
  VMP.MIN_QTY AS MIN_QTY,
  VMP.MIN_QTY_UOM_CD AS MIN_QTY_UOM_CD,
  VMP.MAX_QTY AS MAX_QTY,
  VMP.MAX_QTY_UOM_CD AS MAX_QTY_UOM_CD,
  VMP.MIN_VOLUME_CFT AS MIN_VOLUME_CFT,
  VMP.MAX_VOLUME_CFT AS MAX_VOLUME_CFT,
  VMP.MIN_WEIGHT_LBS AS MIN_WEIGHT_LBS,
  VMP.MAX_WEIGHT_LBS AS MAX_WEIGHT_LBS,
  CURRENT_TIMESTAMP AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_VENDOR_MINIMUM_PRE_0 VMP,
  SKU_PROFILE SKU,
  SITE_PROFILE SITE
WHERE
  VMP.STORE_NBR = SITE.STORE_NBR
  AND VMP.SKU_NBR = SKU.SKU_NBR"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_VENDOR_MINIMUM_PRE_1")

# COMMAND ----------
# DBTITLE 1, VENDOR_MINIMUM


spark.sql("""INSERT INTO
  VENDOR_MINIMUM
SELECT
  VENDOR_ID AS VENDOR_ID,
  LOCATION_ID AS LOCATION_ID,
  PRODUCT_ID AS PRODUCT_ID,
  SEARCH_LEVEL AS SEARCH_LEVEL,
  MIN_AMT AS MIN_AMT,
  MAX_AMT AS MAX_AMT,
  MIN_QTY AS MIN_QTY,
  MIN_QTY_UOM_CD AS MIN_QTY_UOM_CD,
  MAX_QTY AS MAX_QTY,
  MAX_QTY_UOM_CD AS MAX_QTY_UOM_CD,
  MIN_VOLUME_CFT AS MIN_VOLUME_CFT,
  MAX_VOLUME_CFT AS MAX_VOLUME_CFT,
  MIN_WEIGHT_LBS AS MIN_WEIGHT_LBS,
  MAX_WEIGHT_LBS AS MAX_WEIGHT_LBS,
  LOAD_DT AS LOAD_DT
FROM
  ASQ_Shortcut_To_VENDOR_MINIMUM_PRE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_minimum")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_minimum", mainWorkflowId, parentName)
