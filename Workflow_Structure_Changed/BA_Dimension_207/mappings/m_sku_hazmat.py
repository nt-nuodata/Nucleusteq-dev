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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_hazmat")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_hazmat", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_HAZMAT_PRE_0


query_0 = f"""SELECT
  RTV_DEPT_CD AS RTV_DEPT_CD,
  RTV_DESC AS RTV_DESC,
  HAZ_FLAG AS HAZ_FLAG,
  AEROSOL_FLAG AS AEROSOL_FLAG
FROM
  SKU_HAZMAT_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SKU_HAZMAT_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_SKU_HAZMAT_PRE_1


query_1 = f"""SELECT
  RTV_DEPT_CD AS RTV_DEPT_CD,
  RTV_DESC AS RTV_DESC,
  HAZ_FLAG AS HAZ_FLAG,
  AEROSOL_FLAG AS AEROSOL_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_HAZMAT_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SKU_HAZMAT_PRE_1")

# COMMAND ----------
# DBTITLE 1, lkp_SKU_HAZMAT_2


query_2 = f"""SELECT
  SH.ADD_DT AS ADD_DT,
  AStSHP1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_to_SKU_HAZMAT_PRE_1 AStSHP1
  LEFT JOIN SKU_HAZMAT SH ON RTV_DEPT_CD = AStSHP1.RTV_DEPT_CD"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("lkp_SKU_HAZMAT_2")

# COMMAND ----------
# DBTITLE 1, exp_SKU_HAZMAT_3


query_3 = f"""SELECT
  lSH2.RTV_DEPT_CD AS INSERT_UPDATE_FLAG,
  AStSHP1.RTV_DEPT_CD AS RTV_DEPT_CD,
  AStSHP1.RTV_DESC AS RTV_DESC,
  DECODE(AStSHP1.HAZ_FLAG, 'N', 0, 'Y', 1) AS o_HAZ_FLAG,
  DECODE(AStSHP1.AEROSOL_FLAG, 'N', 0, 'Y', 1) AS o_AEROSOL_FLAG,
  IFF(ISNULL(lSH2.ADD_DT), now(), lSH2.ADD_DT) AS ADD_DT,
  now() AS LOAD_DT,
  lSH2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  lkp_SKU_HAZMAT_2 lSH2
  INNER JOIN ASQ_Shortcut_to_SKU_HAZMAT_PRE_1 AStSHP1 ON lSH2.Monotonically_Increasing_Id = AStSHP1.Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("exp_SKU_HAZMAT_3")

# COMMAND ----------
# DBTITLE 1, upd_SKU_HAZMAT_ins_upd_4


query_4 = f"""SELECT
  RTV_DEPT_CD AS RTV_DEPT_CD,
  RTV_DESC AS RTV_DESC,
  o_HAZ_FLAG AS HAZ_FLAG,
  o_AEROSOL_FLAG AS AEROSOL_FLAG,
  ADD_DT AS ADD_DT,
  LOAD_DT AS LOAD_DT,
  INSERT_UPDATE_FLAG AS INSERT_UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    ISNULL(INSERT_UPDATE_FLAG),
    'DD_INSERT',
    'DD_UPDATE'
  ) AS UPDATE_STRATEGY_FLAG
FROM
  exp_SKU_HAZMAT_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("upd_SKU_HAZMAT_ins_upd_4")

# COMMAND ----------
# DBTITLE 1, SKU_HAZMAT


spark.sql("""MERGE INTO SKU_HAZMAT AS TARGET
USING
  upd_SKU_HAZMAT_ins_upd_4 AS SOURCE ON TARGET.RTV_DEPT_CD = SOURCE.RTV_DEPT_CD
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.RTV_DEPT_CD = SOURCE.RTV_DEPT_CD,
  TARGET.RTV_DESC = SOURCE.RTV_DESC,
  TARGET.HAZ_FLAG = SOURCE.HAZ_FLAG,
  TARGET.AEROSOL_FLAG = SOURCE.AEROSOL_FLAG,
  TARGET.ADD_DT = SOURCE.ADD_DT,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.RTV_DESC = SOURCE.RTV_DESC
  AND TARGET.HAZ_FLAG = SOURCE.HAZ_FLAG
  AND TARGET.AEROSOL_FLAG = SOURCE.AEROSOL_FLAG
  AND TARGET.ADD_DT = SOURCE.ADD_DT
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.RTV_DEPT_CD,
    TARGET.RTV_DESC,
    TARGET.HAZ_FLAG,
    TARGET.AEROSOL_FLAG,
    TARGET.ADD_DT,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.RTV_DEPT_CD,
    SOURCE.RTV_DESC,
    SOURCE.HAZ_FLAG,
    SOURCE.AEROSOL_FLAG,
    SOURCE.ADD_DT,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_hazmat")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_hazmat", mainWorkflowId, parentName)
