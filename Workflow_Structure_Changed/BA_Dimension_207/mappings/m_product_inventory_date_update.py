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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_product_inventory_date_update")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_product_inventory_date_update", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_INVENTORY_PRE_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  ON_HAND_QTY AS ON_HAND_QTY,
  XFER_IN_TRANS_QTY AS XFER_IN_TRANS_QTY,
  MAP_AMT AS MAP_AMT,
  PRICE_CHANGE_DT AS PRICE_CHANGE_DT,
  VALUATED_STOCK_QTY AS VALUATED_STOCK_QTY,
  VALUATED_STOCK_AMT AS VALUATED_STOCK_AMT,
  PREV_PRICE_AMT AS PREV_PRICE_AMT
FROM
  INVENTORY_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_INVENTORY_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_INVENTORY_PRE_1


query_1 = f"""SELECT
  DISTINCT Shortcut_To_INVENTORY_PRE_0.DAY_DT AS DAY_DT,
  (Shortcut_To_INVENTORY_PRE_0.SKU_NBR) AS SKU_NBR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_INVENTORY_PRE_0
WHERE
  Shortcut_To_INVENTORY_PRE_0.DAY_DT = DATE_TRUNC('DAY', CURRENT_TIMESTAMP) - 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_INVENTORY_PRE_1")

# COMMAND ----------
# DBTITLE 1, LKPTRANS_2


query_2 = f"""SELECT
  P.PRODUCT_ID AS PRODUCT_ID,
  P.DATE_FIRST_INV AS DATE_FIRST_INV,
  P.DATE_LAST_INV AS DATE_LAST_INV,
  ASTIP1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_To_INVENTORY_PRE_1 ASTIP1
  LEFT JOIN PRODUCT P ON P.SKU_NBR = ASTIP1.SKU_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKPTRANS_2")

# COMMAND ----------
# DBTITLE 1, EXP_check_inventory_date_3


query_3 = f"""SELECT
  L2.PRODUCT_ID AS PRODUCT_ID,
  IFF(
    ASTIP1.DAY_DT < L2.DATE_FIRST_INV,
    ASTIP1.DAY_DT,
    L2.DATE_FIRST_INV
  ) AS OUT_DATE_FIRST_INV,
  IFF(
    ASTIP1.DAY_DT > L2.DATE_LAST_INV,
    ASTIP1.DAY_DT,
    L2.DATE_LAST_INV
  ) AS OUT_DATE_LAST_INV,
  L2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKPTRANS_2 L2
  INNER JOIN ASQ_Shortcut_To_INVENTORY_PRE_1 ASTIP1 ON L2.Monotonically_Increasing_Id = ASTIP1.Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_check_inventory_date_3")

# COMMAND ----------
# DBTITLE 1, UPD_update_product_inventory_date_4


query_4 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  OUT_DATE_FIRST_INV AS OUT_DATE_FIRST_INV,
  OUT_DATE_LAST_INV AS OUT_DATE_LAST_INV,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(ISNULL(PRODUCT_ID), 'DD_REJECT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_check_inventory_date_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("UPD_update_product_inventory_date_4")

# COMMAND ----------
# DBTITLE 1, PRODUCT


spark.sql("""MERGE INTO PRODUCT AS TARGET
USING
  UPD_update_product_inventory_date_4 AS SOURCE ON TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PRODUCT_ID = SOURCE.PRODUCT_ID,
  TARGET.DATE_FIRST_INV = SOURCE.OUT_DATE_FIRST_INV,
  TARGET.DATE_LAST_INV = SOURCE.OUT_DATE_LAST_INV
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.DATE_FIRST_INV = SOURCE.OUT_DATE_FIRST_INV
  AND TARGET.DATE_LAST_INV = SOURCE.OUT_DATE_LAST_INV THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PRODUCT_ID,
    TARGET.DATE_FIRST_INV,
    TARGET.DATE_LAST_INV
  )
VALUES
  (
    SOURCE.PRODUCT_ID,
    SOURCE.OUT_DATE_FIRST_INV,
    SOURCE.OUT_DATE_LAST_INV
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_product_inventory_date_update")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_product_inventory_date_update", mainWorkflowId, parentName)
