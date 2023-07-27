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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_hazmat_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_hazmat_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_HAZMAT_FLAT_0


query_0 = f"""SELECT
  RTV_DEPT_CD AS RTV_DEPT_CD,
  RTV_DESC AS RTV_DESC,
  HAZ_FLAG AS HAZ_FLAG,
  AEROSOL_FLAG AS AEROSOL_FLAG
FROM
  SKU_HAZMAT_FLAT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SKU_HAZMAT_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_HAZMAT_FLAT_1


query_1 = f"""SELECT
  RTV_DEPT_CD AS RTV_DEPT_CD,
  RTV_DESC AS RTV_DESC,
  HAZ_FLAG AS HAZ_FLAG,
  AEROSOL_FLAG AS AEROSOL_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_HAZMAT_FLAT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_HAZMAT_FLAT_1")

# COMMAND ----------
# DBTITLE 1, SKU_HAZMAT_PRE


spark.sql("""INSERT INTO
  SKU_HAZMAT_PRE
SELECT
  RTV_DEPT_CD AS RTV_DEPT_CD,
  RTV_DESC AS RTV_DESC,
  HAZ_FLAG AS HAZ_FLAG,
  AEROSOL_FLAG AS AEROSOL_FLAG
FROM
  SQ_Shortcut_to_SKU_HAZMAT_FLAT_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_hazmat_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_hazmat_pre", mainWorkflowId, parentName)
