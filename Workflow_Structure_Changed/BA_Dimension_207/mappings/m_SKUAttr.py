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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKUAttr")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SKUAttr", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_FF_sku_txs_attr_0


query_0 = f"""SELECT
  FF_SKU_NBR AS FF_SKU_NBR,
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
  FF_DEL_IND AS FF_DEL_IND,
  FF_USER AS FF_USER
FROM
  FF_sku_txs_attr"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_FF_sku_txs_attr_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_FF_sku_txs_attr_1


query_1 = f"""SELECT
  FF_SKU_NBR AS FF_SKU_NBR,
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
  FF_DEL_IND AS FF_DEL_IND,
  FF_USER AS FF_USER,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_FF_sku_txs_attr_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_FF_sku_txs_attr_1")

# COMMAND ----------
# DBTITLE 1, Fil_FF_Source_2


query_2 = f"""SELECT
  FF_SKU_NBR AS FF_SKU_NBR,
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
  FF_DEL_IND AS FF_DEL_IND,
  FF_USER AS FF_USER,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_FF_sku_txs_attr_1
WHERE
  NOT ISNULL(FF_SKU_NBR)
  AND NOT ISNULL(FF_SKU_TXS_ATTR_TYPE_ID)
  AND NOT ISNULL(FF_SKU_TXS_ATTR_TYPE_VALUE_ID)
  AND NOT ISNULL(FF_DEL_IND)"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Fil_FF_Source_2")

# COMMAND ----------
# DBTITLE 1, Lkp_SkuAttrTypeValues_3


query_3 = f"""SELECT
  S.SKUAttrTypeID AS SKUAttrTypeID,
  FFS2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_FF_Source_2 FFS2
  LEFT JOIN SKUAttrTypeValues S ON S.SKUAttrTypeID = FFS2.FF_SKU_TXS_ATTR_TYPE_ID
  AND S.SKUAttrTypeValueID = FFS2.FF_SKU_TXS_ATTR_TYPE_VALUE_ID"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Lkp_SkuAttrTypeValues_3")

# COMMAND ----------
# DBTITLE 1, Lkp_SKuAttr_4


query_4 = f"""SELECT
  S.LoadDt AS LoadDt,
  S.LoadUser AS LoadUser,
  FFS2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_FF_Source_2 FFS2
  LEFT JOIN SKUAttr S ON S.SKUNbr = FFS2.FF_SKU_NBR
  AND S.SKUAttrTypeID = FFS2.FF_SKU_TXS_ATTR_TYPE_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Lkp_SKuAttr_4")

# COMMAND ----------
# DBTITLE 1, Fil_SkuAttrTypeValues_5


query_5 = f"""SELECT
  FFS2.FF_SKU_NBR AS FF_SKU_NBR,
  FFS2.FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FFS2.FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
  LS4.LoadDt AS LoadDt,
  LS3.SKUAttrTypeID AS SKUAttrTypeID,
  FFS2.FF_DEL_IND AS FF_DEL_IND,
  LS4.LoadUser AS LoadUser,
  FFS2.FF_USER AS FF_USER,
  FFS2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_FF_Source_2 FFS2
  INNER JOIN Lkp_SKuAttr_4 LS4 ON FFS2.Monotonically_Increasing_Id = LS4.Monotonically_Increasing_Id
  INNER JOIN Lkp_SkuAttrTypeValues_3 LS3 ON LS4.Monotonically_Increasing_Id = LS3.Monotonically_Increasing_Id
WHERE
  NOT ISNULL(LS3.SKUAttrTypeID)"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Fil_SkuAttrTypeValues_5")

# COMMAND ----------
# DBTITLE 1, Exp_SkuAttr_6


query_6 = f"""SELECT
  FF_SKU_NBR AS FF_SKU_NBR,
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
  FF_DEL_IND AS FF_DEL_IND,
  IFF(ISNULL(LoadDt), now(), LoadDt) AS LoadDt,
  sysdate AS UpdateDt,
  IFF(ISNULL(LoadUser), RTRIM(LTRIM(FF_USER)), LoadUser) AS LoadUser,
  IFF(ISNULL(LoadUser), 'ETL', FF_USER) AS UpdateUser,
  IFF(ISNULL(LoadDt), dd_insert, dd_update) AS UpdateStrategy,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_SkuAttrTypeValues_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Exp_SkuAttr_6")

# COMMAND ----------
# DBTITLE 1, Ups_SkuAttr_7


query_7 = f"""SELECT
  FF_SKU_NBR AS FF_SKU_NBR,
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_ID AS FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
  FF_DEL_IND AS DelInd,
  LoadDt AS LoadDt,
  UpdateDt AS UpdateDt,
  LoadUser AS LoadUser,
  UpdateStrategy AS UpdateStrategy,
  UpdateUser AS UpdateUser,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  UpdateStrategy AS UPDATE_STRATEGY_FLAG
FROM
  Exp_SkuAttr_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Ups_SkuAttr_7")

# COMMAND ----------
# DBTITLE 1, SKUAttr


spark.sql("""MERGE INTO SKUAttr AS TARGET
USING
  Ups_SkuAttr_7 AS SOURCE ON TARGET.SKUNbr = SOURCE.FF_SKU_NBR
  AND TARGET.SKUAttrTypeID = SOURCE.FF_SKU_TXS_ATTR_TYPE_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.SKUNbr = SOURCE.FF_SKU_NBR,
  TARGET.SKUAttrTypeID = SOURCE.FF_SKU_TXS_ATTR_TYPE_ID,
  TARGET.SKUAttrTypeValueID = SOURCE.FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
  TARGET.DelInd = SOURCE.DelInd,
  TARGET.UpdateUser = SOURCE.UpdateUser,
  TARGET.UpdateDt = SOURCE.UpdateDt,
  TARGET.LoadUser = SOURCE.LoadUser,
  TARGET.LoadDt = SOURCE.LoadDt
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.SKUAttrTypeValueID = SOURCE.FF_SKU_TXS_ATTR_TYPE_VALUE_ID
  AND TARGET.DelInd = SOURCE.DelInd
  AND TARGET.UpdateUser = SOURCE.UpdateUser
  AND TARGET.UpdateDt = SOURCE.UpdateDt
  AND TARGET.LoadUser = SOURCE.LoadUser
  AND TARGET.LoadDt = SOURCE.LoadDt THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.SKUNbr,
    TARGET.SKUAttrTypeID,
    TARGET.SKUAttrTypeValueID,
    TARGET.DelInd,
    TARGET.UpdateUser,
    TARGET.UpdateDt,
    TARGET.LoadUser,
    TARGET.LoadDt
  )
VALUES
  (
    SOURCE.FF_SKU_NBR,
    SOURCE.FF_SKU_TXS_ATTR_TYPE_ID,
    SOURCE.FF_SKU_TXS_ATTR_TYPE_VALUE_ID,
    SOURCE.DelInd,
    SOURCE.UpdateUser,
    SOURCE.UpdateDt,
    SOURCE.LoadUser,
    SOURCE.LoadDt
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKUAttr")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SKUAttr", mainWorkflowId, parentName)
