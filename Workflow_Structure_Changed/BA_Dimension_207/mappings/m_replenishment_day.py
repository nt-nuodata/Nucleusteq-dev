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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_replenishment_day")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_replenishment_day", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_REPLENISHMENT_DAY_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  DELETE_IND AS DELETE_IND,
  SAFETY_QTY AS SAFETY_QTY,
  SERVICE_LVL_RT AS SERVICE_LVL_RT,
  REORDER_POINT_QTY AS REORDER_POINT_QTY,
  PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  PRESENT_QTY AS PRESENT_QTY,
  PROMO_QTY AS PROMO_QTY,
  LOAD_DT AS LOAD_DT
FROM
  REPLENISHMENT_DAY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_REPLENISHMENT_DAY_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_REPLENISHMENT_PRE_1


query_1 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  DELETE_IND AS DELETE_IND,
  SAFETY_QTY AS SAFETY_QTY,
  SERVICE_LVL_RT AS SERVICE_LVL_RT,
  REORDER_POINT_QTY AS REORDER_POINT_QTY,
  PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  PRESENT_QTY AS PRESENT_QTY,
  PROMO_QTY AS PROMO_QTY,
  LOAD_DT AS LOAD_DT
FROM
  REPLENISHMENT_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_To_REPLENISHMENT_PRE_1")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_REPLENISHMENT_DAY_2


query_2 = f"""SELECT
  RP.SKU_NBR AS SKU_NBR,
  RP.STORE_NBR AS STORE_NBR,
  RP.DELETE_IND AS DELETE_IND,
  RP.SAFETY_QTY AS SAFETY_QTY,
  RP.SERVICE_LVL_RT AS SERVICE_LVL_RT,
  RP.REORDER_POINT_QTY AS REORDER_POINT_QTY,
  RP.PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  RP.TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  RP.PRESENT_QTY AS PRESENT_QTY,
  RP.PROMO_QTY AS PROMO_QTY,
  CURRENT_TIMESTAMP AS LOAD_DT,
  RD.SKU_NBR AS OLD_SKU_NBR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_REPLENISHMENT_PRE_1 RP
  LEFT OUTER JOIN Shortcut_To_REPLENISHMENT_DAY_0 RD ON RP.SKU_NBR = RD.SKU_NBR
  AND RP.STORE_NBR = RD.STORE_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_Shortcut_to_REPLENISHMENT_DAY_2")

# COMMAND ----------
# DBTITLE 1, UPD_REPLENISHMENT_DAY_3


query_3 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  DELETE_IND AS DELETE_IND,
  SAFETY_QTY AS SAFETY_QTY,
  SERVICE_LVL_RT AS SERVICE_LVL_RT,
  REORDER_POINT_QTY AS REORDER_POINT_QTY,
  PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  PRESENT_QTY AS PRESENT_QTY,
  PROMO_QTY AS PROMO_QTY,
  LOAD_DT AS LOAD_DT,
  OLD_SKU_NBR AS OLD_SKU_NBR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(ISNULL(OLD_SKU_NBR), 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  ASQ_Shortcut_to_REPLENISHMENT_DAY_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_REPLENISHMENT_DAY_3")

# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_DAY


spark.sql("""MERGE INTO REPLENISHMENT_DAY AS TARGET
USING
  UPD_REPLENISHMENT_DAY_3 AS SOURCE ON TARGET.SKU_NBR = SOURCE.SKU_NBR
  AND TARGET.STORE_NBR = SOURCE.STORE_NBR
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.SKU_NBR = SOURCE.SKU_NBR,
  TARGET.STORE_NBR = SOURCE.STORE_NBR,
  TARGET.DELETE_IND = SOURCE.DELETE_IND,
  TARGET.SAFETY_QTY = SOURCE.SAFETY_QTY,
  TARGET.SERVICE_LVL_RT = SOURCE.SERVICE_LVL_RT,
  TARGET.REORDER_POINT_QTY = SOURCE.REORDER_POINT_QTY,
  TARGET.PLAN_DELIV_DAYS = SOURCE.PLAN_DELIV_DAYS,
  TARGET.TARGET_STOCK_QTY = SOURCE.TARGET_STOCK_QTY,
  TARGET.PRESENT_QTY = SOURCE.PRESENT_QTY,
  TARGET.PROMO_QTY = SOURCE.PROMO_QTY,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.DELETE_IND = SOURCE.DELETE_IND
  AND TARGET.SAFETY_QTY = SOURCE.SAFETY_QTY
  AND TARGET.SERVICE_LVL_RT = SOURCE.SERVICE_LVL_RT
  AND TARGET.REORDER_POINT_QTY = SOURCE.REORDER_POINT_QTY
  AND TARGET.PLAN_DELIV_DAYS = SOURCE.PLAN_DELIV_DAYS
  AND TARGET.TARGET_STOCK_QTY = SOURCE.TARGET_STOCK_QTY
  AND TARGET.PRESENT_QTY = SOURCE.PRESENT_QTY
  AND TARGET.PROMO_QTY = SOURCE.PROMO_QTY
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.SKU_NBR,
    TARGET.STORE_NBR,
    TARGET.DELETE_IND,
    TARGET.SAFETY_QTY,
    TARGET.SERVICE_LVL_RT,
    TARGET.REORDER_POINT_QTY,
    TARGET.PLAN_DELIV_DAYS,
    TARGET.TARGET_STOCK_QTY,
    TARGET.PRESENT_QTY,
    TARGET.PROMO_QTY,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.SKU_NBR,
    SOURCE.STORE_NBR,
    SOURCE.DELETE_IND,
    SOURCE.SAFETY_QTY,
    SOURCE.SERVICE_LVL_RT,
    SOURCE.REORDER_POINT_QTY,
    SOURCE.PLAN_DELIV_DAYS,
    SOURCE.TARGET_STOCK_QTY,
    SOURCE.PRESENT_QTY,
    SOURCE.PROMO_QTY,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_replenishment_day")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_replenishment_day", mainWorkflowId, parentName)