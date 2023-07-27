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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_PIM_Attr")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SKU_PIM_Attr", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Lkp_Sku_Profile


spark.sql(
  """CREATE OR REPLACE FUNCTION Lkp_Sku_Profile (in_ArticleNbr INT)
RETURNS INT
READS SQL DATA SQL SECURITY DEFINER
RETURN SELECT FIRST(PRODUCT_ID) FROM SKU_PROFILE WHERE SKU_NBR = in_ArticleNbr"""
)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_PIM_ATTR_PRE_1


query_1 = f"""SELECT
  ARTICLE_NBR AS ARTICLE_NBR,
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  SLICE_IND AS SLICE_IND,
  SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
  DEL_IND AS DEL_IND,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_PIM_ATTR_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_SKU_PIM_ATTR_PRE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_PRE_2


query_2 = f"""SELECT
  ARTICLE_NBR AS ARTICLE_NBR,
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  SLICE_IND AS SLICE_IND,
  SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
  DEL_IND AS DEL_IND,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_PIM_ATTR_PRE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_PRE_2")

# COMMAND ----------
# DBTITLE 1, Exp_SkuNbr_3


query_3 = f"""SELECT
  LKP_SKU_PROFILE(TO_INTEGER(ARTICLE_NBR)) AS PRODUCT_ID,
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  SLICE_IND AS SLICE_IND,
  SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
  IFF(DEL_IND = 'X', 1, 0) AS DEL_IND1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKU_PIM_ATTR_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Exp_SkuNbr_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKU_PIM_ATTR_4


query_4 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
  SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
  SLICE_IND AS SLICE_IND,
  SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SKU_PIM_ATTR"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_SKU_PIM_ATTR_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKU_PIM_ATTR_5


query_5 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
  SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
  SLICE_IND AS SLICE_IND,
  SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKU_PIM_ATTR_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("SQ_Shortcut_to_SKU_PIM_ATTR_5")

# COMMAND ----------
# DBTITLE 1, Jnr_SKU_PIM_Attr_6


query_6 = f"""SELECT
  DETAIL.PRODUCT_ID AS PRODUCT_ID,
  DETAIL.PIM_ATTR_ID AS PIM_ATTR_ID,
  DETAIL.PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  DETAIL.SLICE_IND AS SLICE_IND1,
  DETAIL.SLICE_SEQ_NBR AS SLICE_SEQ_NBR1,
  DETAIL.DEL_IND1 AS DEL_IND,
  MASTER.PRODUCT_ID AS PRODUCT_ID1,
  MASTER.SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
  MASTER.SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
  MASTER.SLICE_IND AS SLICE_IND,
  MASTER.SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
  MASTER.LOAD_TSTMP AS LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKU_PIM_ATTR_5 MASTER
  RIGHT JOIN Exp_SkuNbr_3 DETAIL ON MASTER.PRODUCT_ID = DETAIL.PRODUCT_ID
  AND MASTER.SKU_PIM_ATTR_TYPE_ID = DETAIL.PIM_ATTR_ID
  AND MASTER.SKU_PIM_ATTR_TYPE_VALUE_ID = DETAIL.PIM_ATTR_VAL_ID"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Jnr_SKU_PIM_Attr_6")

# COMMAND ----------
# DBTITLE 1, Fil_SKU_PIM_Attr_7


query_7 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  SLICE_IND1 AS SLICE_IND1,
  SLICE_SEQ_NBR1 AS SLICE_SEQ_NBR1,
  DEL_IND AS DEL_IND,
  PRODUCT_ID1 AS PRODUCT_ID1,
  SKU_PIM_ATTR_TYPE_ID AS SKU_PIM_ATTR_TYPE_ID,
  SKU_PIM_ATTR_TYPE_VALUE_ID AS SKU_PIM_ATTR_TYPE_VALUE_ID,
  SLICE_IND AS SLICE_IND,
  SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Jnr_SKU_PIM_Attr_6
WHERE
  (
    NOT ISNULL(PRODUCT_ID)
    AND ISNULL(PRODUCT_ID1)
    AND DEL_IND <> 1
  )
  OR (
    NOT ISNULL(PRODUCT_ID)
    AND NOT ISNULL(PRODUCT_ID1)
    AND (
      SLICE_IND1 <> SLICE_IND
      OR SLICE_SEQ_NBR1 <> SLICE_SEQ_NBR
      OR DEL_IND = 1
    )
  )"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Fil_SKU_PIM_Attr_7")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_PIM_Attr_8


query_8 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  SLICE_IND1 AS SLICE_IND,
  SLICE_SEQ_NBR1 AS SLICE_SEQ_NBR,
  DEL_IND AS DEL_IND,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(LOAD_TSTMP), now(), LOAD_TSTMP) AS LOAD_TSTMP,
  IFF(
    ISNULL(LOAD_TSTMP),
    'DD_INSERT',
    IFF(DEL_IND = 1, 'DD_DELETE', 'DD_UPDATE')
  ) AS LoadStrategyFlag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_SKU_PIM_Attr_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("Exp_SKU_PIM_Attr_8")

# COMMAND ----------
# DBTITLE 1, Ups_SKU_PIM_Attr_9


query_9 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  SLICE_IND AS SLICE_IND,
  SLICE_SEQ_NBR AS SLICE_SEQ_NBR,
  DEL_IND AS DEL_IND,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  LoadStrategyFlag AS LoadStrategyFlag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  LoadStrategyFlag AS UPDATE_STRATEGY_FLAG
FROM
  Exp_SKU_PIM_Attr_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("Ups_SKU_PIM_Attr_9")

# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR


spark.sql("""MERGE INTO SKU_PIM_ATTR AS TARGET
USING
  Ups_SKU_PIM_Attr_9 AS SOURCE ON TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  AND TARGET.SKU_PIM_ATTR_TYPE_ID = SOURCE.PIM_ATTR_ID
  AND TARGET.SKU_PIM_ATTR_TYPE_VALUE_ID = SOURCE.PIM_ATTR_VAL_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.SKU_PIM_ATTR_TYPE_ID = SOURCE.PIM_ATTR_ID,
  TARGET.SKU_PIM_ATTR_TYPE_VALUE_ID = SOURCE.PIM_ATTR_VAL_ID,
  TARGET.SLICE_IND = SOURCE.SLICE_IND,
  TARGET.SLICE_SEQ_NBR = SOURCE.SLICE_SEQ_NBR,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.SLICE_IND = SOURCE.SLICE_IND
  AND TARGET.SLICE_SEQ_NBR = SOURCE.SLICE_SEQ_NBR
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PRODUCT_ID,
    TARGET.SKU_PIM_ATTR_TYPE_ID,
    TARGET.SKU_PIM_ATTR_TYPE_VALUE_ID,
    TARGET.SLICE_IND,
    TARGET.SLICE_SEQ_NBR,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.PRODUCT_ID,
    SOURCE.PIM_ATTR_ID,
    SOURCE.PIM_ATTR_VAL_ID,
    SOURCE.SLICE_IND,
    SOURCE.SLICE_SEQ_NBR,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_PIM_Attr")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SKU_PIM_Attr", mainWorkflowId, parentName)
