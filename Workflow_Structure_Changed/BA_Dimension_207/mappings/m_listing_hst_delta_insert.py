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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_listing_hst_delta_insert")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_listing_hst_delta_insert", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_LISTING_PRE_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  STORE_NBR AS STORE_NBR,
  SKU_NBR AS SKU_NBR,
  LISTING_END_DT AS LISTING_END_DT,
  LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
  LISTING_EFF_DT AS LISTING_EFF_DT,
  LISTING_MODULE_ID AS LISTING_MODULE_ID,
  LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
  NEGATE_FLAG AS NEGATE_FLAG,
  STRUCT_COMP_FLAG AS STRUCT_COMP_FLAG,
  STRUCT_ARTICLE_FLAG AS STRUCT_ARTICLE_FLAG
FROM
  LISTING_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_LISTING_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_LISTING_PRE_1


query_1 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  SKU_NBR AS SKU_NBR,
  LISTING_END_DT AS LISTING_END_DT,
  LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
  LISTING_EFF_DT AS LISTING_EFF_DT,
  LISTING_MODULE_ID AS LISTING_MODULE_ID,
  LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
  NEGATE_FLAG AS NEGATE_FLAG,
  STRUCT_COMP_FLAG AS STRUCT_COMP_FLAG,
  STRUCT_ARTICLE_FLAG AS STRUCT_ARTICLE_FLAG,
  DELETE_IND AS DELETE_IND,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_LISTING_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_LISTING_PRE_1")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_EFF_DT_2


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_LISTING_PRE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_COMMON_EFF_DT_2")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_END_DT_3


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_LISTING_PRE_1"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_COMMON_END_DT_3")

# COMMAND ----------
# DBTITLE 1, LISTING_HST


spark.sql("""INSERT INTO
  LISTING_HST
SELECT
  SSTLP1.SKU_NBR AS SKU_NBR,
  SSTLP1.STORE_NBR AS STORE_NBR,
  ECED3.o_YYYYMMDD_END_DT_W_DEFAULT_TIME AS LISTING_END_DT,
  SSTLP1.LISTING_SEQ_NBR AS LISTING_SEQ_NBR,
  ECED2.o_YYYYMMDD_EFF_DT_W_DEFAULT_TIME AS LISTING_EFF_DT,
  SSTLP1.LISTING_MODULE_ID AS LISTING_MODULE_ID,
  SSTLP1.LISTING_SOURCE_ID AS LISTING_SOURCE_ID,
  SSTLP1.NEGATE_FLAG AS NEGATE_FLAG,
  SSTLP1.STRUCT_COMP_FLAG AS STRUCT_COMP_CD,
  SSTLP1.STRUCT_ARTICLE_FLAG AS STRUCT_ARTICLE_NBR,
  SSTLP1.DELETE_IND AS DELETE_IND,
  ECED2.o_CURRENT_DATE AS LOAD_DT
FROM
  SQ_Shortcut_To_LISTING_PRE_1 SSTLP1
  INNER JOIN EXP_COMMON_END_DT_3 ECED3 ON SSTLP1.Monotonically_Increasing_Id = ECED3.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_EFF_DT_2 ECED2 ON ECED3.Monotonically_Increasing_Id = ECED2.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_listing_hst_delta_insert")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_listing_hst_delta_insert", mainWorkflowId, parentName)
