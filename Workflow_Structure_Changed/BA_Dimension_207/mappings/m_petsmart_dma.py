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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_petsmart_dma")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_petsmart_dma", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_LOYALTY_PRE_Insert_0


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

df_0.createOrReplaceTempView("Shortcut_To_LOYALTY_PRE_Insert_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_LOYALTY_PRE_Insert_1


query_1 = f"""SELECT
  DISTINCT RTRIM(LP.PETSMART_DMA_CD),
  MAX(RTRIM(LP.PETSMART_DMA_DESC))
FROM
  Shortcut_To_LOYALTY_PRE_Insert_0 LP
WHERE
  NOT EXISTS (
    SELECT
      *
    FROM
      Shortcut_To_LOYALTY_PRE_Insert_0 LP,
      PETSMART_DMA PD
    WHERE
      RTRIM(LP.PETSMART_DMA_CD) = RTRIM(PD.PETSMART_DMA_CD)
  )
GROUP BY
  LP.PETSMART_DMA_CD
ORDER BY
  MAX(RTRIM(LP.PETSMART_DMA_DESC))"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_LOYALTY_PRE_Insert_1")

# COMMAND ----------
# DBTITLE 1, UPD_INSERT_2


query_2 = f"""SELECT
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_To_LOYALTY_PRE_Insert_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("UPD_INSERT_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_LOYALTY_PRE_Update_3


query_3 = f"""SELECT
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

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_To_LOYALTY_PRE_Update_3")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_LOYALTY_PRE_Update_4


query_4 = f"""SELECT
  DISTINCT RTRIM(LP.PETSMART_DMA_CD),
  MAX(RTRIM(LP.PETSMART_DMA_DESC))
FROM
  Shortcut_To_LOYALTY_PRE_Update_3 LP
GROUP BY
  LP.PETSMART_DMA_CD
ORDER BY
  MAX(RTRIM(LP.PETSMART_DMA_DESC))"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("ASQ_Shortcut_To_LOYALTY_PRE_Update_4")

# COMMAND ----------
# DBTITLE 1, UPD_UPDATE_EXISTING_5


query_5 = f"""SELECT
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_To_LOYALTY_PRE_Update_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("UPD_UPDATE_EXISTING_5")

# COMMAND ----------
# DBTITLE 1, PETSMART_DMA


spark.sql("""INSERT INTO
  PETSMART_DMA
SELECT
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC FROMUPD_INSERT_2""")

# COMMAND ----------
# DBTITLE 1, PETSMART_DMA


spark.sql("""MERGE INTO PETSMART_DMA AS TARGET
USING
  UPD_UPDATE_EXISTING_5 AS SOURCE ON TARGET.PETSMART_DMA_CD = SOURCE.PETSMART_DMA_CD
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.PETSMART_DMA_CD = SOURCE.PETSMART_DMA_CD,
  TARGET.PETSMART_DMA_DESC = SOURCE.PETSMART_DMA_DESC""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_petsmart_dma")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_petsmart_dma", mainWorkflowId, parentName)
