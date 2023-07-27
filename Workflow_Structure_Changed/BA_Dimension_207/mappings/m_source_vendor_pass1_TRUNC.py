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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_source_vendor_pass1_TRUNC")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_source_vendor_pass1_TRUNC", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_LISTING_DAY_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  LISTING_END_DT AS LISTING_END_DT,
  LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
  LISTING_EFF_DT AS LISTING_EFF_DT,
  LISTING_MODULE_ID AS LISTING_MODULE_ID,
  LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
  NEGATE_FLAG AS NEGATE_FLAG,
  STRUCT_COMP_CD AS STRUCT_COMP_CD,
  STRUCT_ARTICLE_NBR AS STRUCT_ARTICLE_NBR,
  DELETE_IND AS DELETE_IND,
  LOAD_DT AS LOAD_DT
FROM
  LISTING_DAY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_LISTING_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_LISTING_DAY_1


query_1 = f"""SELECT
  'M_SOURCE_VENDOR' AS MAP_NAME,
  monotonically_increasing_id() AS Monotonically_Increasing_Id"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_LISTING_DAY_1")

# COMMAND ----------
# DBTITLE 1, EXP_VENDER_PRE_2


query_2 = f"""SELECT
  0 AS SKU_NBR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_LISTING_DAY_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_VENDER_PRE_2")

# COMMAND ----------
# DBTITLE 1, FILTRANS_3


query_3 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_VENDER_PRE_2
WHERE
  SKU_NBR <> 0"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("FILTRANS_3")

# COMMAND ----------
# DBTITLE 1, SOURCE_VENDOR_PRE


spark.sql("""INSERT INTO
  SOURCE_VENDOR_PRE
SELECT
  SKU_NBR AS SKU_NBR,
  NULL AS STORE_NBR,
  NULL AS VENDOR_ID,
  NULL AS UNIT_NUMERATOR,
  NULL AS UNIT_DENOMINATOR,
  NULL AS DELIV_EFF_DT,
  NULL AS DELIV_END_DT,
  NULL AS VENDOR_TYPE
FROM
  FILTRANS_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_source_vendor_pass1_TRUNC")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_source_vendor_pass1_TRUNC", mainWorkflowId, parentName)
