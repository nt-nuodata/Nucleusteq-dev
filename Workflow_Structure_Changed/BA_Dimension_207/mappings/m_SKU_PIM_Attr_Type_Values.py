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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_PIM_Attr_Type_Values")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SKU_PIM_Attr_Type_Values", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_0


query_0 = f"""SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  DEL_IND AS DEL_IND,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_PIM_ATTR_TYPE_VALUES_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_1


query_1 = f"""SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  DEL_IND AS DEL_IND,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_2


query_2 = f"""SELECT
  SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
  SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
  SKU_PIM_ATTR_VALUE_DESC AS SKU_PIM_ATTR_VALUE_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_PIM_ATTR_TYPE_VALUES"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_3


query_3 = f"""SELECT
  SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
  SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
  SKU_PIM_ATTR_VALUE_DESC AS SKU_PIM_ATTR_VALUE_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_3")

# COMMAND ----------
# DBTITLE 1, Jnr_SKU_PIM_Attr_Type_Values_4


query_4 = f"""SELECT
  DETAIL.PIM_ATTR_ID AS PIM_ATTR_ID,
  DETAIL.PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  DETAIL.PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
  DETAIL.ENTRY_POSITION AS ENTRY_POSITION,
  DETAIL.DEL_IND AS DEL_IND,
  MASTER.SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
  MASTER.SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
  MASTER.SKU_PIM_ATTR_VALUE_DESC AS SKU_PIM_ATTR_VALUE_DESC,
  MASTER.ENTRY_POSITION AS ENTRY_POSITION1,
  MASTER.LOAD_TSTMP AS LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_SKU_PIM_ATTR_TYPE_VALUES_PRE_1 DETAIL ON MASTER.SKU_PIM_ATTR_TYPE_ID = DETAIL.PIM_ATTR_ID
  AND MASTER.SKU_PIM_ATTR_TYPE_VALUE_ID = DETAIL.PIM_ATTR_VAL_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Jnr_SKU_PIM_Attr_Type_Values_4")

# COMMAND ----------
# DBTITLE 1, Fil_SKU_PIM_Attr_Type_Values_5


query_5 = f"""SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  DEL_IND AS DEL_IND,
  SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
  SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
  SKU_PIM_ATTR_VALUE_DESC AS SKU_PIM_ATTR_VALUE_DESC,
  ENTRY_POSITION1 AS ENTRY_POSITION1,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Jnr_SKU_PIM_Attr_Type_Values_4
WHERE
  (
    NOT ISNULL(PIM_ATTR_ID)
    AND ISNULL(SKU_PIM_ATTR_TYPE_ID)
    AND ISNULL(DEL_IND)
  )
  OR (
    NOT ISNULL(PIM_ATTR_ID)
    AND NOT ISNULL(SKU_PIM_ATTR_TYPE_ID)
    AND (
      PIM_ATTR_VAL_DESC <> SKU_PIM_ATTR_VALUE_DESC
      OR ENTRY_POSITION <> ENTRY_POSITION1
      OR DEL_IND = 'X'
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Fil_SKU_PIM_Attr_Type_Values_5")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_6


query_6 = f"""SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(LOAD_TSTMP), now(), LOAD_TSTMP) AS LOAD_TSTMP,
  IFF(
    ISNULL(LOAD_TSTMP),
    'DD_INSERT',
    IFF(DEL_IND = 'X', 'DD_DELETE', 'DD_UPDATE')
  ) AS LoadStrategyFlag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_SKU_PIM_Attr_Type_Values_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXPTRANS_6")

# COMMAND ----------
# DBTITLE 1, Ups_SKU_PIM_Attr_Type_Values_7


query_7 = f"""SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  LoadStrategyFlag AS LoadStrategyFlag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  LoadStrategyFlag AS UPDATE_STRATEGY_FLAG
FROM
  EXPTRANS_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Ups_SKU_PIM_Attr_Type_Values_7")

# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR_TYPE_VALUES


spark.sql("""MERGE INTO SKU_PIM_ATTR_TYPE_VALUES AS TARGET
USING
  Ups_SKU_PIM_Attr_Type_Values_7 AS SOURCE ON TARGET.SKU_PIM_ATTR_TYPE_ID = SOURCE.PIM_ATTR_ID
  AND TARGET.SKU_PIM_ATTR_TYPE_VALUE_ID = SOURCE.PIM_ATTR_VAL_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.SKU_PIM_ATTR_TYPE_ID = SOURCE.PIM_ATTR_ID,
  TARGET.SKU_PIM_ATTR_TYPE_VALUE_ID = SOURCE.PIM_ATTR_VAL_ID,
  TARGET.SKU_PIM_ATTR_VALUE_DESC = SOURCE.PIM_ATTR_VAL_DESC,
  TARGET.ENTRY_POSITION = SOURCE.ENTRY_POSITION,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.SKU_PIM_ATTR_VALUE_DESC = SOURCE.PIM_ATTR_VAL_DESC
  AND TARGET.ENTRY_POSITION = SOURCE.ENTRY_POSITION
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.SKU_PIM_ATTR_TYPE_ID,
    TARGET.SKU_PIM_ATTR_TYPE_VALUE_ID,
    TARGET.SKU_PIM_ATTR_VALUE_DESC,
    TARGET.ENTRY_POSITION,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.PIM_ATTR_ID,
    SOURCE.PIM_ATTR_VAL_ID,
    SOURCE.PIM_ATTR_VAL_DESC,
    SOURCE.ENTRY_POSITION,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_PIM_Attr_Type_Values")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SKU_PIM_Attr_Type_Values", mainWorkflowId, parentName)
