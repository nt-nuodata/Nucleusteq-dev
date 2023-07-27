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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ztb_promo_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ztb_promo_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_ZPROMO_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POGID AS POGID,
  REPL_START_DATE AS REPL_START_DATE,
  REPL_END_DATE AS REPL_END_DATE,
  LIST_START_DATE AS LIST_START_DATE,
  LIST_END_DATE AS LIST_END_DATE,
  PROMO_QTY AS PROMO_QTY,
  LAST_CNG_DATE AS LAST_CNG_DATE,
  STATUS AS STATUS
FROM
  ZPROMO"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_ZPROMO_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_ZPROMO_1


query_1 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POGID AS POGID,
  REPL_START_DATE AS REPL_START_DATE,
  REPL_END_DATE AS REPL_END_DATE,
  LIST_START_DATE AS LIST_START_DATE,
  LIST_END_DATE AS LIST_END_DATE,
  PROMO_QTY AS PROMO_QTY,
  LAST_CNG_DATE AS LAST_CNG_DATE,
  STATUS AS STATUS,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_ZPROMO_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_ZPROMO_1")

# COMMAND ----------
# DBTITLE 1, EXP_DATE3_2


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ZPROMO_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_DATE3_2")

# COMMAND ----------
# DBTITLE 1, EXP_DATE1_3


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ZPROMO_1"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_DATE1_3")

# COMMAND ----------
# DBTITLE 1, EXP_DATE5_4


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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ZPROMO_1"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_DATE5_4")

# COMMAND ----------
# DBTITLE 1, EXP_DATE4_5


query_5 = f"""SELECT
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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ZPROMO_1"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_DATE4_5")

# COMMAND ----------
# DBTITLE 1, EXP_DATE2_6


query_6 = f"""SELECT
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
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ZPROMO_1"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_DATE2_6")

# COMMAND ----------
# DBTITLE 1, EXP_INSERT_SYSDATE_7


query_7 = f"""SELECT
  TRUNC(now()) AS INSERT_SYSDATE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_ZPROMO_1"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("EXP_INSERT_SYSDATE_7")

# COMMAND ----------
# DBTITLE 1, ZTB_PROMO_PRE


spark.sql("""INSERT INTO
  ZTB_PROMO_PRE
SELECT
  SSTZ1.ARTICLE AS SKU_NBR,
  SSTZ1.SITE AS STORE_NBR,
  SSTZ1.POGID AS POG_NBR,
  ED2.o_YYYYMMDD_W_DEFAULT_TIME AS REPL_START_DT,
  ED3.o_YYYYMMDD_W_DEFAULT_TIME AS REPL_END_DT,
  ED6.o_YYYYMMDD_W_DEFAULT_TIME AS LIST_START_DT,
  ED5.o_YYYYMMDD_W_DEFAULT_TIME AS LIST_END_DT,
  SSTZ1.PROMO_QTY AS PROMO_QTY,
  ED4.o_YYYYMMDD_W_DEFAULT_TIME AS LAST_CHNG_DT,
  SSTZ1.STATUS AS POG_STATUS,
  SSTZ1.DELETE_IND AS DELETE_IND,
  NULL AS DATE_ADDED,
  EIS7.INSERT_SYSDATE AS DATE_REFRESHED,
  NULL AS DATE_DELETED
FROM
  SQ_Shortcut_To_ZPROMO_1 SSTZ1
  INNER JOIN EXP_INSERT_SYSDATE_7 EIS7 ON SSTZ1.Monotonically_Increasing_Id = EIS7.Monotonically_Increasing_Id
  INNER JOIN EXP_DATE5_4 ED4 ON EIS7.Monotonically_Increasing_Id = ED4.Monotonically_Increasing_Id
  INNER JOIN EXP_DATE4_5 ED5 ON ED4.Monotonically_Increasing_Id = ED5.Monotonically_Increasing_Id
  INNER JOIN EXP_DATE3_2 ED2 ON ED5.Monotonically_Increasing_Id = ED2.Monotonically_Increasing_Id
  INNER JOIN EXP_DATE2_6 ED6 ON ED2.Monotonically_Increasing_Id = ED6.Monotonically_Increasing_Id
  INNER JOIN EXP_DATE1_3 ED3 ON ED6.Monotonically_Increasing_Id = ED3.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ztb_promo_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ztb_promo_pre", mainWorkflowId, parentName)
