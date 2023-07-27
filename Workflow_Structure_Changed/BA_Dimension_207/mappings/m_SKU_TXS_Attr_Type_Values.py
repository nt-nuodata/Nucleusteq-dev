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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_TXS_Attr_Type_Values")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SKU_TXS_Attr_Type_Values", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_PRE_0


query_0 = f"""SELECT
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
  DEL_IND AS DEL_IND,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_TXS_ATTR_TYPE_VALUES_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_PRE_1


query_1 = f"""SELECT
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
  DEL_IND AS DEL_IND,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES1_2


query_2 = f"""SELECT
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_TXS_ATTR_TYPE_VALUES"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES1_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_3


query_3 = f"""SELECT
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_3")

# COMMAND ----------
# DBTITLE 1, Jnr_SKU_TXS_Attr_Type_Values_4


query_4 = f"""SELECT
  DETAIL.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  DETAIL.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  DETAIL.SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
  DETAIL.DEL_IND AS DEL_IND,
  DETAIL.UPDATE_USER AS UPDATE_USER,
  DETAIL.LOAD_USER AS LOAD_USER,
  MASTER.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID1,
  MASTER.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID1,
  MASTER.SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC1,
  MASTER.UPDATE_USER AS UPDATE_USER1,
  MASTER.LOAD_USER AS LOAD_USER1,
  MASTER.LOAD_TSTMP AS LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_3 MASTER
  RIGHT JOIN SQ_Shortcut_to_SKU_TXS_ATTR_TYPE_VALUES_PRE_1 DETAIL ON MASTER.SKU_TXS_ATTR_TYPE_ID = DETAIL.SKU_TXS_ATTR_TYPE_ID
  AND MASTER.SKU_TXS_ATTR_TYPE_VALUE_ID = DETAIL.SKU_TXS_ATTR_TYPE_VALUE_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Jnr_SKU_TXS_Attr_Type_Values_4")

# COMMAND ----------
# DBTITLE 1, Fil_SKU_TXS_Attr_Type_Value_5


query_5 = f"""SELECT
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
  DEL_IND AS DEL_IND,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  SKU_TXS_ATTR_TYPE_ID1 AS SKU_TXS_ATTR_TYPE_ID1,
  SKU_TXS_ATTR_TYPE_VALUE_ID1 AS SKU_TXS_ATTR_TYPE_VALUE_ID1,
  SKU_TXS_ATTR_TYPE_VALUE_DESC1 AS SKU_TXS_ATTR_TYPE_VALUE_DESC1,
  UPDATE_USER1 AS UPDATE_USER1,
  LOAD_USER1 AS LOAD_USER1,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Jnr_SKU_TXS_Attr_Type_Values_4
WHERE
  (
    NOT ISNULL(SKU_TXS_ATTR_TYPE_ID)
    AND ISNULL(SKU_TXS_ATTR_TYPE_ID1)
  )
  OR (
    NOT ISNULL(SKU_TXS_ATTR_TYPE_ID)
    AND NOT ISNULL(SKU_TXS_ATTR_TYPE_ID1)
    AND (
      DEL_IND = 'X'
      OR SKU_TXS_ATTR_TYPE_VALUE_DESC <> SKU_TXS_ATTR_TYPE_VALUE_DESC1
      OR UPDATE_USER <> UPDATE_USER1
      OR LOAD_USER <> LOAD_USER1
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Fil_SKU_TXS_Attr_Type_Value_5")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_TXS_Attr_Type_Values_6


query_6 = f"""SELECT
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  sysdate AS UPDATE_TSTMP,
  IFF(
    NOT ISNULL(SKU_TXS_ATTR_TYPE_ID)
    AND ISNULL(LOAD_TSTMP),
    'DD_INSERT',
    IFF(
      NOT ISNULL(SKU_TXS_ATTR_TYPE_ID)
      AND NOT ISNULL(LOAD_TSTMP)
      AND DEL_IND = 'X',
      'DD_DELETE',
      'DD_UPDATE'
    )
  ) AS UpdateStrategy,
  IFF(ISNULL(LOAD_TSTMP), now(), LOAD_TSTMP) AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_SKU_TXS_Attr_Type_Value_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Exp_SKU_TXS_Attr_Type_Values_6")

# COMMAND ----------
# DBTITLE 1, Ups_SKU_TXS_Attr_Type_Values_7


query_7 = f"""SELECT
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKU_TXS_ATTR_TYPE_VALUE_DESC,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  UpdateStrategy AS UpdateStrategy,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  UpdateStrategy AS UPDATE_STRATEGY_FLAG
FROM
  Exp_SKU_TXS_Attr_Type_Values_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Ups_SKU_TXS_Attr_Type_Values_7")

# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR_TYPE_VALUES


spark.sql("""MERGE INTO SKU_TXS_ATTR_TYPE_VALUES AS TARGET
USING
  Ups_SKU_TXS_Attr_Type_Values_7 AS SOURCE ON TARGET.SKU_TXS_ATTR_TYPE_VALUE_ID = SOURCE.SKU_TXS_ATTR_TYPE_VALUE_ID
  AND TARGET.SKU_TXS_ATTR_TYPE_ID = SOURCE.SKU_TXS_ATTR_TYPE_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.SKU_TXS_ATTR_TYPE_ID = SOURCE.SKU_TXS_ATTR_TYPE_ID,
  TARGET.SKU_TXS_ATTR_TYPE_VALUE_ID = SOURCE.SKU_TXS_ATTR_TYPE_VALUE_ID,
  TARGET.SKU_TXS_ATTR_TYPE_VALUE_DESC = SOURCE.SKU_TXS_ATTR_TYPE_VALUE_DESC,
  TARGET.UPDATE_USER = SOURCE.UPDATE_USER,
  TARGET.LOAD_USER = SOURCE.LOAD_USER,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.SKU_TXS_ATTR_TYPE_VALUE_DESC = SOURCE.SKU_TXS_ATTR_TYPE_VALUE_DESC
  AND TARGET.UPDATE_USER = SOURCE.UPDATE_USER
  AND TARGET.LOAD_USER = SOURCE.LOAD_USER
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.SKU_TXS_ATTR_TYPE_ID,
    TARGET.SKU_TXS_ATTR_TYPE_VALUE_ID,
    TARGET.SKU_TXS_ATTR_TYPE_VALUE_DESC,
    TARGET.UPDATE_USER,
    TARGET.LOAD_USER,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.SKU_TXS_ATTR_TYPE_ID,
    SOURCE.SKU_TXS_ATTR_TYPE_VALUE_ID,
    SOURCE.SKU_TXS_ATTR_TYPE_VALUE_DESC,
    SOURCE.UPDATE_USER,
    SOURCE.LOAD_USER,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_TXS_Attr_Type_Values")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SKU_TXS_Attr_Type_Values", mainWorkflowId, parentName)
