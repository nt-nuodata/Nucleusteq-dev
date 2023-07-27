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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_cost_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_store_cost_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_STORE_COST_FLAT_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  COND_TYPE_CD AS COND_TYPE_CD,
  VENDOR_ID AS VENDOR_ID,
  SKU_NBR AS SKU_NBR,
  PURCH_ORG_CD AS PURCH_ORG_CD,
  STORE_NBR AS STORE_NBR,
  PURCH_INFO_CD AS PURCH_INFO_CD,
  COND_END_DT AS COND_END_DT,
  COND_EFF_DT AS COND_EFF_DT,
  COND_RECORD_NBR AS COND_RECORD_NBR,
  COND_AMT AS COND_AMT,
  PROMOTION_CD AS PROMOTION_CD,
  COND_RT_UNIT AS COND_RT_UNIT,
  COND_PRICE_UNIT AS COND_PRICE_UNIT,
  COND_UNIT AS COND_UNIT,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  COND_DENOMINATOR AS COND_DENOMINATOR
FROM
  SKU_STORE_COST_FLAT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SKU_STORE_COST_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SKU_STORE_COST_FLAT_1


query_1 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  COND_TYPE_CD AS COND_TYPE_CD,
  VENDOR_ID AS VENDOR_ID,
  SKU_NBR AS SKU_NBR,
  PURCH_ORG_CD AS PURCH_ORG_CD,
  STORE_NBR AS STORE_NBR,
  PURCH_INFO_CD AS PURCH_INFO_CD,
  COND_END_DT AS COND_END_DT,
  COND_EFF_DT AS COND_EFF_DT,
  COND_RECORD_NBR AS COND_RECORD_NBR,
  COND_AMT AS COND_AMT,
  PROMOTION_CD AS PROMOTION_CD,
  COND_RT_UNIT AS COND_RT_UNIT,
  COND_PRICE_UNIT AS COND_PRICE_UNIT,
  COND_UNIT AS COND_UNIT,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  COND_DENOMINATOR AS COND_DENOMINATOR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SKU_STORE_COST_FLAT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_SKU_STORE_COST_FLAT_1")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_EFF_DATE_2


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SKU_STORE_COST_FLAT_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_COMMON_EFF_DATE_2")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_END_DATE_3


query_3 = f"""SELECT
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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SKU_STORE_COST_FLAT_1"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_COMMON_END_DATE_3")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_VENDOR_TRANS_4


query_4 = f"""SELECT
  IFF(
    SUBSTR(VENDOR_ID, 1, 1) = 'V',
    TO_FLOAT(SUBSTR(VENDOR_ID, 2, 4)) + 900000,
    TO_FLOAT(VENDOR_ID)
  ) AS VENDOR_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SKU_STORE_COST_FLAT_1"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_COMMON_VENDOR_TRANS_4")

# COMMAND ----------
# DBTITLE 1, FIL_Invalid_Records_5


query_5 = f"""SELECT
  SSTSSCF1.SKU_NBR AS SKU_NBR,
  SSTSSCF1.STORE_NBR AS STORE_NBR,
  ECVT4.VENDOR_ID AS VENDOR_ID,
  SSTSSCF1.PURCH_ORG_CD AS PURCH_ORG_CD,
  SSTSSCF1.COND_TYPE_CD AS COND_TYPE_CD,
  SSTSSCF1.PURCH_INFO_CD AS PURCH_INFO_CD,
  ECED3.o_YYYYMMDD_END_DT_W_DEFAULT_TIME AS COND_END_DT,
  SSTSSCF1.DELETE_IND AS DELETE_IND,
  ECED2.o_YYYYMMDD_EFF_DT_W_DEFAULT_TIME AS COND_EFF_DT,
  SSTSSCF1.COND_RECORD_NBR AS COND_RECORD_NBR,
  SSTSSCF1.COND_AMT AS COND_AMT,
  SSTSSCF1.PROMOTION_CD AS PROMOTION_CD,
  SSTSSCF1.COND_RT_UNIT AS COND_RT_UNIT,
  SSTSSCF1.COND_PRICE_UNIT AS COND_PRICE_UNIT,
  SSTSSCF1.COND_UNIT AS COND_UNIT,
  SSTSSCF1.UNIT_NUMERATOR AS UNIT_NUMERATOR,
  SSTSSCF1.COND_DENOMINATOR AS COND_DENOMINATOR,
  ECED3.o_CURRENT_DATE_MINUS1 AS LOAD_DT,
  ECVT4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_COMMON_VENDOR_TRANS_4 ECVT4
  INNER JOIN SQ_Shortcut_To_SKU_STORE_COST_FLAT_1 SSTSSCF1 ON ECVT4.Monotonically_Increasing_Id = SSTSSCF1.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_END_DATE_3 ECED3 ON SSTSSCF1.Monotonically_Increasing_Id = ECED3.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_EFF_DATE_2 ECED2 ON ECED3.Monotonically_Increasing_Id = ECED2.Monotonically_Increasing_Id
WHERE
  SSTSSCF1.PURCH_INFO_CD = '0'"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_Invalid_Records_5")

# COMMAND ----------
# DBTITLE 1, SKU_STORE_COST_PRE


spark.sql("""INSERT INTO
  SKU_STORE_COST_PRE
SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  PURCH_ORG_CD AS PURCH_ORG_CD,
  COND_TYPE_CD AS COND_TYPE_CD,
  PURCH_INFO_CD AS PURCH_INFO_CD,
  COND_END_DT AS COND_END_DT,
  DELETE_IND AS DELETE_IND,
  COND_EFF_DT AS COND_EFF_DT,
  COND_RECORD_NBR AS COND_RECORD_NBR,
  COND_AMT AS COND_AMT,
  PROMOTION_CD AS PROMOTION_CD,
  COND_RT_UNIT AS COND_RT_UNIT,
  COND_PRICE_UNIT AS COND_PRICE_UNIT,
  COND_UNIT AS COND_UNIT,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  COND_DENOMINATOR AS UNIT_DENOMINATOR,
  LOAD_DT AS LOAD_DT
FROM
  FIL_Invalid_Records_5""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_cost_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_store_cost_pre", mainWorkflowId, parentName)
