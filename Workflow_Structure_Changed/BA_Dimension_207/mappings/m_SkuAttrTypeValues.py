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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SkuAttrTypeValues")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SkuAttrTypeValues", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_FF_sku_txs_attr_type_values_0


query_0 = f"""SELECT
  FF_SKU_NBR AS FF_SKU_NBR,
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC
FROM
  FF_sku_txs_attr_type_values"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_FF_sku_txs_attr_type_values_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_FF_sku_txs_attr_type_values_1


query_1 = f"""SELECT
  FF_SKU_NBR AS FF_SKU_NBR,
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_FF_sku_txs_attr_type_values_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_FF_sku_txs_attr_type_values_1")

# COMMAND ----------
# DBTITLE 1, Srt_Distinct_2


query_2 = f"""SELECT
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_FF_sku_txs_attr_type_values_1
ORDER BY
  FF_SKU_TXS_ATTR_TYPE_ID ASC,
  FF_SKU_TXS_ATTR_TYPE_VALUE_DESC ASC"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Srt_Distinct_2")

# COMMAND ----------
# DBTITLE 1, Lkp_SkuAttrTypeValues_3


query_3 = f"""SELECT
  S.SKUAttrTypeValueID AS SKUAttrTypeValueID,
  S.LoadDt AS LoadDt,
  SD2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Srt_Distinct_2 SD2
  LEFT JOIN SKUAttrTypeValues S ON S.SKUAttrTypeID = SD2.FF_SKU_TXS_ATTR_TYPE_ID
  AND S.SKUAttrTypeValueDesc = SD2.FF_SKU_TXS_ATTR_TYPE_VALUE_DESC"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Lkp_SkuAttrTypeValues_3")

# COMMAND ----------
# DBTITLE 1, Fil_SkuAttrTypeValues_4


query_4 = f"""SELECT
  SD2.FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  SD2.FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
  LS3.SKUAttrTypeValueID AS SKUAttrTypeValueID,
  LS3.LoadDt AS LoadDt,
  LS3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Lkp_SkuAttrTypeValues_3 LS3
  INNER JOIN Srt_Distinct_2 SD2 ON LS3.Monotonically_Increasing_Id = SD2.Monotonically_Increasing_Id
WHERE
  ISNULL(LS3.SKUAttrTypeValueID)"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Fil_SkuAttrTypeValues_4")

# COMMAND ----------
# DBTITLE 1, Exp_SKUAttrTypeValues_5


query_5 = f"""SELECT
  FF_SKU_TXS_ATTR_TYPE_ID AS FF_SKU_TXS_ATTR_TYPE_ID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS FF_SKU_TXS_ATTR_TYPE_VALUE_DESC,
  'ETL' AS LoadUser,
  sysdate AS UpdateDt,
  IFF(ISNULL(LoadDt), now(), LoadDt) AS LoadDt,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_SkuAttrTypeValues_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Exp_SKUAttrTypeValues_5")

# COMMAND ----------
# DBTITLE 1, SKUAttrTypeValues


spark.sql("""INSERT INTO
  SKUAttrTypeValues
SELECT
  FF_SKU_TXS_ATTR_TYPE_ID AS SKUAttrTypeID,
  NULL AS SKUAttrTypeValueID,
  FF_SKU_TXS_ATTR_TYPE_VALUE_DESC AS SKUAttrTypeValueDesc,
  NULL AS DelInd,
  NULL AS UpdateUser,
  UpdateDt AS UpdateDt,
  LoadUser AS LoadUser,
  LoadDt AS LoadDt
FROM
  Exp_SKUAttrTypeValues_5""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SkuAttrTypeValues")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SkuAttrTypeValues", mainWorkflowId, parentName)
