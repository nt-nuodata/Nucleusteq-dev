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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_minimum_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_minimum_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_VENDOR_MINIMUM_FLATFILE_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  VENDOR_ID AS VENDOR_ID,
  STORE_NBR AS STORE_NBR,
  SKU_NBR AS SKU_NBR,
  SEARCH_LEVEL AS SEARCH_LEVEL,
  MIN_DOLLARS AS MIN_DOLLARS,
  MAX_DOLLARS AS MAX_DOLLARS,
  MIN_QTY AS MIN_QTY,
  MIN_QTY_UOM AS MIN_QTY_UOM,
  MAX_QTY AS MAX_QTY,
  MAX_QTY_UOM AS MAX_QTY_UOM,
  MIN_VOLUME_CFT AS MIN_VOLUME_CFT,
  MAX_VOLUMNE_CFT AS MAX_VOLUMNE_CFT,
  MIN_WEIGHT_LBS AS MIN_WEIGHT_LBS,
  MAX_WEIGHT_LBS AS MAX_WEIGHT_LBS
FROM
  VENDOR_MINIMUM_FLATFILE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_VENDOR_MINIMUM_FLATFILE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_VENDOR_MINIMUM_FLATFILE_1


query_1 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  SKU_NBR AS SKU_NBR,
  SEARCH_LEVEL AS SEARCH_LEVEL,
  MIN_DOLLARS AS MIN_DOLLARS,
  MAX_DOLLARS AS MAX_DOLLARS,
  MIN_QTY AS MIN_QTY,
  MIN_QTY_UOM AS MIN_QTY_UOM,
  MAX_QTY AS MAX_QTY,
  MAX_QTY_UOM AS MAX_QTY_UOM,
  MIN_VOLUME_CFT AS MIN_VOLUME_CFT,
  MAX_VOLUMNE_CFT AS MAX_VOLUME_CFT,
  MIN_WEIGHT_LBS AS MIN_WEIGHT_LBS,
  MAX_WEIGHT_LBS AS MAX_WEIGHT_LBS,
  DELETE_IND AS DELETE_IND,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_VENDOR_MINIMUM_FLATFILE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_VENDOR_MINIMUM_FLATFILE_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


query_2 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  ADD_TO_DATE(now(), 'DAY', -1) AS V_LOAD_DT,
  ADD_TO_DATE(now(), 'DAY', -1) AS LOAD_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_VENDOR_MINIMUM_FLATFILE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, VENDOR_MINIMUM_PRE


spark.sql("""INSERT INTO
  VENDOR_MINIMUM_PRE
SELECT
  SSTVMF1.VENDOR_ID AS VENDOR_ID,
  SSTVMF1.STORE_NBR AS STORE_NBR,
  SSTVMF1.SKU_NBR AS SKU_NBR,
  SSTVMF1.SEARCH_LEVEL AS SEARCH_LEVEL,
  SSTVMF1.DELETE_IND AS DELETE_IND,
  SSTVMF1.MIN_DOLLARS AS MIN_AMT,
  SSTVMF1.MAX_DOLLARS AS MAX_AMT,
  SSTVMF1.MIN_QTY AS MIN_QTY,
  SSTVMF1.MIN_QTY_UOM AS MIN_QTY_UOM_CD,
  SSTVMF1.MAX_QTY AS MAX_QTY,
  SSTVMF1.MAX_QTY_UOM AS MAX_QTY_UOM_CD,
  SSTVMF1.MIN_VOLUME_CFT AS MIN_VOLUME_CFT,
  SSTVMF1.MAX_VOLUME_CFT AS MAX_VOLUME_CFT,
  SSTVMF1.MIN_WEIGHT_LBS AS MIN_WEIGHT_LBS,
  SSTVMF1.MAX_WEIGHT_LBS AS MAX_WEIGHT_LBS,
  E2.LOAD_DT AS LOAD_DT
FROM
  SQ_Shortcut_To_VENDOR_MINIMUM_FLATFILE_1 SSTVMF1
  INNER JOIN EXPTRANS_2 E2 ON SSTVMF1.Monotonically_Increasing_Id = E2.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_minimum_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_minimum_pre", mainWorkflowId, parentName)
