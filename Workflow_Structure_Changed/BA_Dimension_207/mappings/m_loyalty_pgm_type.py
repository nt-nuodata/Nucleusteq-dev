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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_loyalty_pgm_type")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_loyalty_pgm_type", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_LOYALTY_PRE_0


query_0 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  STORE_DESC AS STORE_DESC,
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
  LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
  LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
  LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
  LOAD_DT AS LOAD_DT
FROM
  LOYALTY_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_LOYALTY_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_LOYALTY_PRE_1


query_1 = f"""SELECT
  DISTINCT LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_LOYALTY_PRE_0
WHERE
  LOYALTY_PGM_TYPE_ID IS NOT NULL
  AND LOYALTY_PGM_TYPE_DESC IS NOT NULL"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_LOYALTY_PRE_1")

# COMMAND ----------
# DBTITLE 1, LOYALTY_PGM_TYPE


spark.sql("""INSERT INTO
  LOYALTY_PGM_TYPE
SELECT
  LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC
FROM
  ASQ_Shortcut_To_LOYALTY_PRE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_loyalty_pgm_type")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_loyalty_pgm_type", mainWorkflowId, parentName)
