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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_vendor_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_store_vendor_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_ARTICLE_SITE_VENDOR_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  ARTICLE_ID AS ARTICLE_ID,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCUR_SITE_ID AS PROCUR_SITE_ID
FROM
  ARTICLE_SITE_VENDOR"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_ARTICLE_SITE_VENDOR_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1


query_1 = f"""SELECT
  ARTICLE_ID AS ARTICLE_ID,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCUR_SITE_ID AS PROCUR_SITE_ID,
  DELETE_IND AS DELETE_IND,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_ARTICLE_SITE_VENDOR_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_END_DATE_TRANS_2


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_COMMON_END_DATE_TRANS_2")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_EFF_DATE_TRANS_3


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_COMMON_EFF_DATE_TRANS_3")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_VENDOR_TRANS_4


query_4 = f"""SELECT
  IFF(
    SUBSTR(VENDOR_ID, 1, 1) = 'V',
    TO_FLOAT(SUBSTR(VENDOR_ID, 2, 4)) + 900000,
    TO_FLOAT(VENDOR_ID)
  ) AS VENDOR_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_COMMON_VENDOR_TRANS_4")

# COMMAND ----------
# DBTITLE 1, AGGTRANS_5


query_5 = f"""SELECT
  SSTASV1.ARTICLE_ID AS ARTICLE_ID,
  SSTASV1.STORE_NBR AS STORE_NBR,
  ECVT4.VENDOR_ID AS VENDOR_ID,
  MAX(ECEDT3.o_YYYYMMDD_EFF_DT_W_DEFAULT_TIME) AS o_FROM_DATE,
  ECEDT2.o_YYYYMMDD_END_DT_W_DEFAULT_TIME AS o_To_Date,
  SSTASV1.FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  SSTASV1.PROCUR_SITE_ID AS PROCUR_SITE_ID,
  SSTASV1.DELETE_IND AS DELETE_IND,
  last(SSTASV1.Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ARTICLE_SITE_VENDOR1_1 SSTASV1
  INNER JOIN EXP_COMMON_VENDOR_TRANS_4 ECVT4 ON SSTASV1.Monotonically_Increasing_Id = ECVT4.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_EFF_DATE_TRANS_3 ECEDT3 ON ECVT4.Monotonically_Increasing_Id = ECEDT3.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_END_DATE_TRANS_2 ECEDT2 ON ECEDT3.Monotonically_Increasing_Id = ECEDT2.Monotonically_Increasing_Id
GROUP BY
  ARTICLE_ID,
  STORE_NBR,
  VENDOR_ID"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("AGGTRANS_5")

# COMMAND ----------
# DBTITLE 1, SKU_STORE_VENDOR_PRE


spark.sql("""INSERT INTO
  SKU_STORE_VENDOR_PRE
SELECT
  ARTICLE_ID AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  DELETE_IND AS DELETE_IND,
  o_FROM_DATE AS SOURCE_LIST_EFF_DT,
  o_To_Date AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCUR_SITE_ID AS PROCURE_SITE_ID
FROM
  AGGTRANS_5""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_vendor_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_store_vendor_pre", mainWorkflowId, parentName)
