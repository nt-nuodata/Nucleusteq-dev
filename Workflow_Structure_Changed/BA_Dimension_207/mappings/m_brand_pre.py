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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_brand_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_brand_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Brand_0


query_0 = f"""SELECT
  BrandCd AS BrandCd,
  BrandName AS BrandName,
  BrandTypeCd AS BrandTypeCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  BrandClassificationCd AS BrandClassificationCd
FROM
  Brand"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_Brand_0")

# COMMAND ----------
# DBTITLE 1, SQ_ArtMast_Brand_1


query_1 = f"""SELECT
  BrandCd AS BrandCd,
  BrandName AS BrandName,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Brand_0
WHERE
  Shortcut_to_Brand_0.BrandClassificationCd is not null"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_ArtMast_Brand_1")

# COMMAND ----------
# DBTITLE 1, BRAND_PRE


spark.sql("""INSERT INTO
  BRAND_PRE
SELECT
  BrandCd AS BRAND_CD,
  BrandName AS BRAND_NAME
FROM
  SQ_ArtMast_Brand_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_brand_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_brand_pre", mainWorkflowId, parentName)
