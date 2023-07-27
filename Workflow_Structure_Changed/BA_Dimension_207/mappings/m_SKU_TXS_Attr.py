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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_TXS_Attr")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SKU_TXS_Attr", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_TXS_ATTR_PRE_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  DEL_IND AS DEL_IND,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_TXS_ATTR_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SKU_TXS_ATTR_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1


query_1 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  DEL_IND AS DEL_IND,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_TXS_ATTR_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1")

# COMMAND ----------
# DBTITLE 1, Lkp_Sku_Profile_2


query_2 = f"""SELECT
  SStSTAP1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1 SStSTAP1
  LEFT JOIN SKU_PROFILE SP ON SP.SKU_NBR = SStSTAP1.SKU_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Lkp_Sku_Profile_2")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_TXS_Attr_Pre_3


query_3 = f"""SELECT
  LSP2.PRODUCT_ID AS PRODUCT_ID,
  SStSTAP1.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SStSTAP1.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  SStSTAP1.DEL_IND AS DEL_IND,
  SStSTAP1.UPDATE_USER AS UPDATE_USER,
  SStSTAP1.LOAD_USER AS LOAD_USER,
  SStSTAP1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKU_TXS_ATTR_PRE_1 SStSTAP1
  INNER JOIN Lkp_Sku_Profile_2 LSP2 ON SStSTAP1.Monotonically_Increasing_Id = LSP2.Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Exp_SKU_TXS_Attr_Pre_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_TXS_ATTR_4


query_4 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_TXS_ATTR"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_SKU_TXS_ATTR_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_TXS_ATTR_5


query_5 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_TXS_ATTR_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("SQ_Shortcut_to_SKU_TXS_ATTR_5")

# COMMAND ----------
# DBTITLE 1, Jnr_SKU_TXS_Attr_6


query_6 = f"""SELECT
  DETAIL.PRODUCT_ID AS PRODUCT_ID,
  DETAIL.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  DETAIL.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  DETAIL.DEL_IND AS DEL_IND,
  DETAIL.UPDATE_USER AS UPDATE_USER,
  DETAIL.LOAD_USER AS LOAD_USER,
  MASTER.PRODUCT_ID AS PRODUCT_ID1,
  MASTER.SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID1,
  MASTER.SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID1,
  MASTER.UPDATE_USER AS UPDATE_USER1,
  MASTER.LOAD_USER AS LOAD_USER1,
  MASTER.LOAD_TSTMP AS LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKU_TXS_ATTR_5 MASTER
  RIGHT JOIN Exp_SKU_TXS_Attr_Pre_3 DETAIL ON MASTER.PRODUCT_ID = DETAIL.PRODUCT_ID
  AND MASTER.SKU_TXS_ATTR_TYPE_ID = DETAIL.SKU_TXS_ATTR_TYPE_ID"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Jnr_SKU_TXS_Attr_6")

# COMMAND ----------
# DBTITLE 1, Fil_SKU_TXS_Attr_7


query_7 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  DEL_IND AS DEL_IND,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  PRODUCT_ID1 AS PRODUCT_ID1,
  SKU_TXS_ATTR_TYPE_ID1 AS SKU_TXS_ATTR_TYPE_ID1,
  SKU_TXS_ATTR_TYPE_VALUE_ID1 AS SKU_TXS_ATTR_TYPE_VALUE_ID1,
  UPDATE_USER1 AS UPDATE_USER1,
  LOAD_USER1 AS LOAD_USER1,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Jnr_SKU_TXS_Attr_6
WHERE
  NOT ISNULL(PRODUCT_ID)
  AND ISNULL(PRODUCT_ID1)
  OR (
    NOT ISNULL(PRODUCT_ID)
    AND NOT ISNULL(PRODUCT_ID1)
    OR (
      DEL_IND = 'X'
      OR DEL_IND = '1'
      OR SKU_TXS_ATTR_TYPE_VALUE_ID <> SKU_TXS_ATTR_TYPE_VALUE_ID1
      OR UPDATE_USER <> UPDATE_USER1
      OR LOAD_USER <> LOAD_USER1
    )
  )"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Fil_SKU_TXS_Attr_7")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_TXS_Attr_8


query_8 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(LOAD_TSTMP), now(), LOAD_TSTMP) AS LOAD_TSTMP,
  IFF(
    ISNULL(LOAD_TSTMP),
    'DD_INSERT',
    IFF(
      DEL_IND = 'X'
      OR DEL_IND = '1',
      'DD_DELETE',
      'DD_UPDATE'
    )
  ) AS LoadStrategy,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_SKU_TXS_Attr_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("Exp_SKU_TXS_Attr_8")

# COMMAND ----------
# DBTITLE 1, Ups_SKU_TXS_Attr_9


query_9 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_TXS_ATTR_TYPE_ID AS SKU_TXS_ATTR_TYPE_ID,
  SKU_TXS_ATTR_TYPE_VALUE_ID AS SKU_TXS_ATTR_TYPE_VALUE_ID,
  UPDATE_USER AS UPDATE_USER,
  LOAD_USER AS LOAD_USER,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  LoadStrategy AS LoadStrategy,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  LoadStrategy AS UPDATE_STRATEGY_FLAG
FROM
  Exp_SKU_TXS_Attr_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("Ups_SKU_TXS_Attr_9")

# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR


spark.sql("""MERGE INTO SKU_TXS_ATTR AS TARGET
USING
  Ups_SKU_TXS_Attr_9 AS SOURCE ON TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  AND TARGET.SKU_TXS_ATTR_TYPE_ID = SOURCE.SKU_TXS_ATTR_TYPE_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.SKU_TXS_ATTR_TYPE_ID = SOURCE.SKU_TXS_ATTR_TYPE_ID,
  TARGET.SKU_TXS_ATTR_TYPE_VALUE_ID = SOURCE.SKU_TXS_ATTR_TYPE_VALUE_ID,
  TARGET.UPDATE_USER = SOURCE.UPDATE_USER,
  TARGET.LOAD_USER = SOURCE.LOAD_USER,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.SKU_TXS_ATTR_TYPE_VALUE_ID = SOURCE.SKU_TXS_ATTR_TYPE_VALUE_ID
  AND TARGET.UPDATE_USER = SOURCE.UPDATE_USER
  AND TARGET.LOAD_USER = SOURCE.LOAD_USER
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PRODUCT_ID,
    TARGET.SKU_TXS_ATTR_TYPE_ID,
    TARGET.SKU_TXS_ATTR_TYPE_VALUE_ID,
    TARGET.UPDATE_USER,
    TARGET.LOAD_USER,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.PRODUCT_ID,
    SOURCE.SKU_TXS_ATTR_TYPE_ID,
    SOURCE.SKU_TXS_ATTR_TYPE_VALUE_ID,
    SOURCE.UPDATE_USER,
    SOURCE.LOAD_USER,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_TXS_Attr")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SKU_TXS_Attr", mainWorkflowId, parentName)
