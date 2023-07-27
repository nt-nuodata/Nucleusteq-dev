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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_vendor_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_vendor_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_ARTICLE_VENDOR1_0


query_0 = f"""SELECT
  DELETE_FLAG AS DELETE_FLAG,
  ARTICLE_ID AS ARTICLE_ID,
  VENDOR_ID AS VENDOR_ID,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  COUNTRY_CD AS COUNTRY_CD,
  DELIV_EFF_DT AS DELIV_EFF_DT,
  DELIV_END_DT AS DELIV_END_DT,
  REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
  ROUNDING_PROFILE_CD AS ROUNDING_PROFILE_CD,
  VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD
FROM
  ARTICLE_VENDOR"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_ARTICLE_VENDOR1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_ARTICLE_VENDOR_1


query_1 = f"""SELECT
  ARTICLE_ID AS ARTICLE_ID,
  VENDOR_ID AS VENDOR_ID,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  COUNTRY_CD AS COUNTRY_CD,
  DELIV_EFF_DT AS DELIV_EFF_DT,
  DELIV_END_DT AS DELIV_END_DT,
  REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
  ROUNDING_PROFILE_CD AS ROUNDING_PROFILE,
  VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
  DELETE_FLAG AS DELETE_FLAG,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_ARTICLE_VENDOR1_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_ARTICLE_VENDOR_1")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_VENDOR_TRANS_2


query_2 = f"""SELECT
  IFF(
    SUBSTR(VENDOR_ID, 1, 1) = 'V',
    TO_FLOAT(SUBSTR(VENDOR_ID, 2, 4)) + 900000,
    TO_FLOAT(VENDOR_ID)
  ) AS VENDOR_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ARTICLE_VENDOR_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_COMMON_VENDOR_TRANS_2")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_END_DATE_TRANS1_3


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ARTICLE_VENDOR_1"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_COMMON_END_DATE_TRANS1_3")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_EFF_DATE_TRANS_4


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ARTICLE_VENDOR_1"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_COMMON_EFF_DATE_TRANS_4")

# COMMAND ----------
# DBTITLE 1, AGGTRANS_5


query_5 = f"""SELECT
  SSTAV1.ARTICLE_ID AS ARTICLE_ID,
  ECVT2.VENDOR_ID AS VENDOR_ID,
  SSTAV1.DELETE_FLAG AS DELETE_FLAG,
  SSTAV1.UNIT_NUMERATOR AS UNIT_NUMERATOR,
  SSTAV1.UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  SSTAV1.COUNTRY_CD AS COUNTRY_CD,
  MAX(ECEDT4.o_YYYYMMDD_EFF_DT_W_DEFAULT_TIME) AS o_From_DT,
  ECEDT3.o_YYYYMMDD_END_DT_W_DEFAULT_TIME AS o_To_Date,
  IFF(
    isnull(SSTAV1.REGULAR_VENDOR_CD),
    ' ',
    SSTAV1.REGULAR_VENDOR_CD
  ) AS out_REGULAR_VENDOR_CD,
  SSTAV1.ROUNDING_PROFILE AS ROUNDING_PROFILE,
  SSTAV1.VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR,
  SSTAV1.VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  last(SSTAV1.Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ARTICLE_VENDOR_1 SSTAV1
  INNER JOIN EXP_COMMON_END_DATE_TRANS1_3 ECEDT3 ON SSTAV1.Monotonically_Increasing_Id = ECEDT3.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_VENDOR_TRANS_2 ECVT2 ON ECEDT3.Monotonically_Increasing_Id = ECVT2.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_EFF_DATE_TRANS_4 ECEDT4 ON ECVT2.Monotonically_Increasing_Id = ECEDT4.Monotonically_Increasing_Id
GROUP BY
  ARTICLE_ID,
  VENDOR_ID"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("AGGTRANS_5")

# COMMAND ----------
# DBTITLE 1, SKU_VENDOR_PRE


spark.sql("""INSERT INTO
  SKU_VENDOR_PRE
SELECT
  ARTICLE_ID AS SKU_NBR,
  VENDOR_ID AS VENDOR_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  DELETE_FLAG AS DELETE_IND,
  UNIT_NUMERATOR AS UNIT_NUMERATOR,
  UNIT_DENOMINATOR AS UNIT_DENOMINATOR,
  o_From_DT AS DELIV_EFF_DT,
  o_To_Date AS DELIV_END_DT,
  out_REGULAR_VENDOR_CD AS REGULAR_VENDOR_CD,
  COUNTRY_CD AS COUNTRY_CD,
  ROUNDING_PROFILE AS ROUNDING_PROFILE_CD,
  VENDOR_ARTICLE_NBR AS VENDOR_ARTICLE_NBR
FROM
  AGGTRANS_5""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_vendor_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_vendor_pre", mainWorkflowId, parentName)
