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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_replenishment_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_replenishment_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_REPLENISHMENT_FILE_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  PURCH_GRP_ID AS PURCH_GRP_ID,
  PROCURE_TYPE_CD AS PROCURE_TYPE_CD,
  ROUNDNG_VALUE AS ROUNDNG_VALUE,
  REPL_TYPE_CD AS REPL_TYPE_CD,
  SAFETY_QTY AS SAFETY_QTY,
  SERVICE_LVL_RT AS SERVICE_LVL_RT,
  ABC_INDICATOR_CD AS ABC_INDICATOR_CD,
  MAX_STOCK_QTY AS MAX_STOCK_QTY,
  REORDER_POINT_QTY AS REORDER_POINT_QTY,
  PERIOD_CD AS PERIOD_CD,
  PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  PROFIT_CENTER_ID AS PROFIT_CENTER_ID,
  MULTIPLIER_RT AS MULTIPLIER_RT,
  FDC_INDICATOR_CD AS FDC_INDICATOR_CD,
  MAX_TARGET_STOCK_QTY AS MAX_TARGET_STOCK_QTY,
  MIN_TARGET_STOCK_QTY AS MIN_TARGET_STOCK_QTY,
  TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  TARGET_COVERAGE_CNT AS TARGET_COVERAGE_CNT,
  PRESENT_QTY AS PRESENT_QTY
FROM
  REPLENISHMENT_FILE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_REPLENISHMENT_FILE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_REPLENISHMENT_FILE_1


query_1 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  SAFETY_QTY AS SAFETY_QTY,
  SERVICE_LVL_RT AS SERVICE_LVL_RT,
  REORDER_POINT_QTY AS REORDER_POINT_QTY,
  PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  PRESENT_QTY AS PRESENT_QTY,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_REPLENISHMENT_FILE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_REPLENISHMENT_FILE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_EXP_COMMON_DATE_TRANS_2


query_2 = f"""SELECT
  IFF(
    DELETE_DT = '00000000',
    null,
    TO_DATE(DELETE_DT, 'MMDDYYYY')
  ) AS o_MMDDYYYY_W_DEFAULT_TIME,
  IFF(
    DELETE_DT = '00000000',
    null,
    TO_DATE(DELETE_DT, 'YYYYMMDD')
  ) AS o_YYYYMMDD_W_DEFAULT_TIME,
  TO_DATE(
    ('9999-12-31.' || i_TIME_ONLY),
    'YYYY-MM-DD.HH24MISS'
  ) AS o_TIME_W_DEFAULT_DATE,
  IFF(
    DELETE_DT = '00000000',
    null,
    TO_DATE(
      (DELETE_DT || '.' || i_TIME_ONLY),
      'MMDDYYYY.HH24:MI:SS'
    )
  ) AS o_MMDDYYYY_W_TIME,
  IFF(
    DELETE_DT = '00000000',
    null,
    TO_DATE(
      (DELETE_DT || '.' || i_TIME_ONLY),
      'YYYYMMDD.HH24:MI:SS'
    )
  ) AS o_YYYYMMDD_W_TIME,
  TRUNC(SESSSTARTTIME) AS o_CURRENT_DATE,
  TRUNC(SESSSTARTTIME) AS v_CURRENT_DATE,
  ADD_TO_DATE(TRUNC(SESSSTARTTIME), 'DD', -1) AS o_CURRENT_DATE_MINUS1,
  TO_DATE('0001-01-01', 'YYYY-MM-DD') AS o_DEFAULT_EFF_DATE,
  TO_DATE('9999-12-31', 'YYYY-MM-DD') AS o_DEFAULT_END_DATE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_REPLENISHMENT_FILE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_To_EXP_COMMON_DATE_TRANS_2")

# COMMAND ----------
# DBTITLE 1, REPLENISHMENT_PRE


spark.sql("""INSERT INTO
  REPLENISHMENT_PRE
SELECT
  SSTRF1.SKU_NBR AS SKU_NBR,
  SSTRF1.STORE_NBR AS STORE_NBR,
  SSTRF1.DELETE_IND AS DELETE_IND,
  SSTRF1.SAFETY_QTY AS SAFETY_QTY,
  SSTRF1.SERVICE_LVL_RT AS SERVICE_LVL_RT,
  SSTRF1.REORDER_POINT_QTY AS REORDER_POINT_QTY,
  SSTRF1.PLAN_DELIV_DAYS AS PLAN_DELIV_DAYS,
  SSTRF1.TARGET_STOCK_QTY AS TARGET_STOCK_QTY,
  SSTRF1.PRESENT_QTY AS PRESENT_QTY,
  NULL AS PROMO_QTY,
  STECDT2.o_CURRENT_DATE AS LOAD_DT
FROM
  Shortcut_To_EXP_COMMON_DATE_TRANS_2 STECDT2
  INNER JOIN SQ_Shortcut_To_REPLENISHMENT_FILE_1 SSTRF1 ON STECDT2.Monotonically_Increasing_Id = SSTRF1.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_replenishment_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_replenishment_pre", mainWorkflowId, parentName)
