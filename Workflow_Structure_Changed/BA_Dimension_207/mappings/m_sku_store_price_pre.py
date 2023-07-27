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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_price_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_store_price_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_STORE_PRICE_FLAT_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  CONDITION_TYPE AS CONDITION_TYPE,
  SALES_ORG AS SALES_ORG,
  STORE_NBR AS STORE_NBR,
  ARTICLE_ID AS ARTICLE_ID,
  TO_DATE AS TO_DATE,
  FROM_DATE AS FROM_DATE,
  CONDITION_NUMBER AS CONDITION_NUMBER,
  COND_AMT AS COND_AMT,
  PROMOTION_CD AS PROMOTION_CD,
  COND_RT_UNIT AS COND_RT_UNIT,
  COND_PRICE_UNIT AS COND_PRICE_UNIT,
  COND_UNIT AS COND_UNIT,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  PRICING_REASON_CD AS PRICING_REASON_CD
FROM
  SKU_STORE_PRICE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SKU_STORE_PRICE_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SITE_PRICE_1


query_1 = f"""SELECT
  ARTICLE_ID AS ARTICLE_ID,
  STORE_NBR AS STORE_NBR,
  SALES_ORG AS SALES_ORG,
  CONDITION_TYPE AS CONDITION_TYPE,
  TO_DATE AS TO_DATE,
  FROM_DATE AS FROM_DATE,
  DELETE_IND AS DELETE_IND,
  CONDITION_NUMBER AS CONDITION_NUMBER,
  COND_AMT AS COND_AMT,
  PROMOTION_CD AS PROMOTION_CD,
  COND_RT_UNIT AS COND_RT_UNIT,
  COND_PRICE_UNIT AS COND_PRICING_UNIT,
  COND_UNIT AS COND_UNIT,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  PRICING_REASON_CD AS PRICING_REASON_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SKU_STORE_PRICE_FLAT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_SITE_PRICE_1")

# COMMAND ----------
# DBTITLE 1, EXP_SKU_STORE_PRICE_PRE_2


query_2 = f"""SELECT
  DECODE(
    TRUE,
    RTRIM(PRICING_REASON_CD) = '',
    NULL,
    RTRIM(PRICING_REASON_CD)
  ) AS out_PRICING_REASON_CD,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SITE_PRICE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_SKU_STORE_PRICE_PRE_2")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_EFF_DATE_3


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SITE_PRICE_1"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_COMMON_EFF_DATE_3")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_END_DT_4


query_4 = f"""SELECT
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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SITE_PRICE_1"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_COMMON_END_DT_4")

# COMMAND ----------
# DBTITLE 1, SKU_STORE_PRICE_PRE


spark.sql("""INSERT INTO
  SKU_STORE_PRICE_PRE
SELECT
  SSTSP1.ARTICLE_ID AS SKU_NBR,
  SSTSP1.STORE_NBR AS STORE_NBR,
  SSTSP1.SALES_ORG AS SALES_ORG_CD,
  SSTSP1.CONDITION_TYPE AS COND_TYPE_CD,
  ECED4.o_YYYYMMDD_EFF_DT_W_DEFAULT_TIME AS COND_END_DT,
  ECED3.o_YYYYMMDD_EFF_DT_W_DEFAULT_TIME AS COND_EFF_DT,
  SSTSP1.CONDITION_NUMBER AS COND_RECORD_NBR,
  SSTSP1.DELETE_IND AS DELETE_IND,
  SSTSP1.PROMOTION_CD AS PROMOTION_CD,
  SSTSP1.COND_AMT AS COND_AMT,
  SSTSP1.COND_RT_UNIT AS COND_RT_UNIT,
  SSTSP1.COND_PRICING_UNIT AS COND_PRICE_UNIT,
  SSTSP1.COND_UNIT AS COND_UNIT,
  SSTSP1.UNIT_NUMERATOR AS UNIT_NUMERATOR,
  SSTSP1.UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  ESSPP2.out_PRICING_REASON_CD AS PRICING_REASON_CD
FROM
  SQ_Shortcut_To_SITE_PRICE_1 SSTSP1
  INNER JOIN EXP_COMMON_EFF_DATE_3 ECED3 ON SSTSP1.Monotonically_Increasing_Id = ECED3.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_END_DT_4 ECED4 ON ECED3.Monotonically_Increasing_Id = ECED4.Monotonically_Increasing_Id
  INNER JOIN EXP_SKU_STORE_PRICE_PRE_2 ESSPP2 ON ECED4.Monotonically_Increasing_Id = ESSPP2.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_price_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_store_price_pre", mainWorkflowId, parentName)
