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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_container")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_container", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ContainerType_0


query_0 = f"""SELECT
  ContainerTypeCd AS ContainerTypeCd,
  ContainerTypeDesc AS ContainerTypeDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  ContainerType"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ContainerType_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ContainerType_1


query_1 = f"""SELECT
  ContainerTypeCd AS ContainerTypeCd,
  ContainerTypeDesc AS ContainerTypeDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ContainerType_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ContainerType_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_CONTAINER_2


query_2 = f"""SELECT
  CONTAINER_CD AS CONTAINER_CD,
  CONTAINER_DESC AS CONTAINER_DESC,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_CONTAINER"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SKU_CONTAINER_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_CONTAINER_3


query_3 = f"""SELECT
  CONTAINER_CD AS CONTAINER_CD,
  CONTAINER_DESC AS CONTAINER_DESC,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_CONTAINER_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_CONTAINER_3")

# COMMAND ----------
# DBTITLE 1, JNR_MasterOuterJoin_4


query_4 = f"""SELECT
  DETAIL.ContainerTypeCd AS ContainerTypeCd,
  DETAIL.ContainerTypeDesc AS ContainerTypeDesc,
  MASTER.CONTAINER_CD AS M_CONTAINER_CD,
  MASTER.CONTAINER_DESC AS M_CONTAINER_DESC,
  MASTER.LOAD_TSTMP AS M_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKU_CONTAINER_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_ContainerType_1 DETAIL ON MASTER.CONTAINER_CD = DETAIL.ContainerTypeCd"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_MasterOuterJoin_4")

# COMMAND ----------
# DBTITLE 1, EXP_CheckChanges_5


query_5 = f"""SELECT
  ContainerTypeCd AS ContainerTypeCd,
  ContainerTypeDesc AS ContainerTypeDesc,
  IFF(ISNULL(M_LOAD_TSTMP), now(), M_LOAD_TSTMP) AS M_LOAD_TSTMP_NOTNULL,
  IFF(
    ISNULL(M_CONTAINER_CD),
    'DD_INSERT',
    IFF(
      M_CONTAINER_DESC <> ContainerTypeDesc,
      'DD_UPDATE',
      'DD_REJECT'
    )
  ) AS UpdateStrategy,
  now() AS Update_Dt,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_MasterOuterJoin_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_CheckChanges_5")

# COMMAND ----------
# DBTITLE 1, FIL_RemoveRejected_6


query_6 = f"""SELECT
  ContainerTypeCd AS ContainerTypeCd,
  ContainerTypeDesc AS ContainerTypeDesc,
  M_LOAD_TSTMP_NOTNULL AS M_LOAD_TSTMP_NOTNULL,
  UpdateStrategy AS UpdateStrategy,
  Update_Dt AS Update_Dt,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_CheckChanges_5
WHERE
  UpdateStrategy <> 'DD_REJECT'"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("FIL_RemoveRejected_6")

# COMMAND ----------
# DBTITLE 1, UPD_SetStrategy_7


query_7 = f"""SELECT
  ContainerTypeCd AS ContainerTypeCd,
  ContainerTypeDesc AS ContainerTypeDesc,
  M_LOAD_TSTMP_NOTNULL AS M_LOAD_TSTMP_NOTNULL,
  UpdateStrategy AS UpdateStrategy,
  Update_Dt AS Update_Dt,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  UpdateStrategy AS UPDATE_STRATEGY_FLAG
FROM
  FIL_RemoveRejected_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_SetStrategy_7")

# COMMAND ----------
# DBTITLE 1, SKU_CONTAINER


spark.sql("""MERGE INTO SKU_CONTAINER AS TARGET
USING
  UPD_SetStrategy_7 AS SOURCE ON TARGET.CONTAINER_CD = SOURCE.ContainerTypeCd
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.CONTAINER_CD = SOURCE.ContainerTypeCd,
  TARGET.CONTAINER_DESC = SOURCE.ContainerTypeDesc,
  TARGET.UPDATE_TSTMP = SOURCE.Update_Dt,
  TARGET.LOAD_TSTMP = SOURCE.M_LOAD_TSTMP_NOTNULL
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.CONTAINER_DESC = SOURCE.ContainerTypeDesc
  AND TARGET.UPDATE_TSTMP = SOURCE.Update_Dt
  AND TARGET.LOAD_TSTMP = SOURCE.M_LOAD_TSTMP_NOTNULL THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.CONTAINER_CD,
    TARGET.CONTAINER_DESC,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.ContainerTypeCd,
    SOURCE.ContainerTypeDesc,
    SOURCE.Update_Dt,
    SOURCE.M_LOAD_TSTMP_NOTNULL
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_container")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_container", mainWorkflowId, parentName)
