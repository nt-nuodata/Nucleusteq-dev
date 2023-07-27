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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_site_po_cond_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_site_po_cond_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, VENDOR_SITE_PO_COND_A907_FLAT_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  COND_TYPE_CD AS COND_TYPE_CD,
  PURCH_ORD_CD AS PURCH_ORD_CD,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  COND_END_DT AS COND_END_DT,
  COND_START_DT AS COND_START_DT,
  COND_REC_NBR AS COND_REC_NBR,
  COND_RT_AMT AS COND_RT_AMT,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD
FROM
  VENDOR_SITE_PO_COND_A907_FLAT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("VENDOR_SITE_PO_COND_A907_FLAT_0")

# COMMAND ----------
# DBTITLE 1, SQ_VENDOR_SITE_PO_COND_A907_FILE_1


query_1 = f"""SELECT
  COND_TYPE_CD AS COND_TYPE_CD,
  PURCH_ORD_CD AS PURCH_ORD_CD,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  COND_END_DT AS COND_END_DT,
  DELETE_IND AS DELETE_IND,
  COND_START_DT AS COND_START_DT,
  COND_REC_NBR AS COND_REC_NBR,
  COND_RT_AMT AS COND_RT_AMT,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  VENDOR_SITE_PO_COND_A907_FLAT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_VENDOR_SITE_PO_COND_A907_FILE_1")

# COMMAND ----------
# DBTITLE 1, EXP_VENDOR_SUBRANGE_CD_2


query_2 = f"""SELECT
  IFF(
    ISNULL(VENDOR_SUBRANGE_CD),
    ' ',
    VENDOR_SUBRANGE_CD
  ) AS VENDOR_SUBRANGE_CD,
  STORE_NBR AS in_STORE_NBR,
  STORE_NBR AS STORE_NBR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_VENDOR_SITE_PO_COND_A907_FILE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_VENDOR_SUBRANGE_CD_2")

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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_VENDOR_SITE_PO_COND_A907_FILE_1"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_COMMON_END_DATE_3")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_START_DATE_4


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_VENDOR_SITE_PO_COND_A907_FILE_1"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_COMMON_START_DATE_4")

# COMMAND ----------
# DBTITLE 1, VENDOR_SITE_PO_COND_PRE


spark.sql("""INSERT INTO
  VENDOR_SITE_PO_COND_PRE
SELECT
  SVSPCAF1.COND_TYPE_CD AS PO_COND_CD,
  SVSPCAF1.PURCH_ORD_CD AS PURCH_ORG_CD,
  SVSPCAF1.STORE_NBR AS STORE_NBR,
  SVSPCAF1.VENDOR_ID AS VENDOR_ID,
  EVSC2.VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  ECED3.o_YYYYMMDD_W_DEFAULT_TIME AS PO_COND_END_DT,
  SVSPCAF1.DELETE_IND AS DELETE_IND,
  ECSD4.o_YYYYMMDD_W_DEFAULT_TIME AS PO_COND_EFF_DT,
  SVSPCAF1.COND_REC_NBR AS PO_COND_REC_NBR,
  SVSPCAF1.COND_RT_AMT AS PO_COND_RATE_AMT
FROM
  SQ_VENDOR_SITE_PO_COND_A907_FILE_1 SVSPCAF1
  INNER JOIN EXP_COMMON_START_DATE_4 ECSD4 ON SVSPCAF1.Monotonically_Increasing_Id = ECSD4.Monotonically_Increasing_Id
  INNER JOIN EXP_COMMON_END_DATE_3 ECED3 ON ECSD4.Monotonically_Increasing_Id = ECED3.Monotonically_Increasing_Id
  INNER JOIN EXP_VENDOR_SUBRANGE_CD_2 EVSC2 ON ECED3.Monotonically_Increasing_Id = EVSC2.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_site_po_cond_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_site_po_cond_pre", mainWorkflowId, parentName)
