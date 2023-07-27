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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_site_profile_plsql")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_site_profile_plsql", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_SITE_PROFILE_PRE_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  DP_PURCH_GROUP_ID AS DP_PURCH_GROUP_ID,
  ACTIVITY_CLASS_CD AS ACTIVITY_CLASS_CD,
  FIRST_DPR_DT AS FIRST_DPR_DT,
  LAST_DPR_DT AS LAST_DPR_DT,
  FIRST_ON_HAND_DT AS FIRST_ON_HAND_DT,
  LAST_ON_HAND_DT AS LAST_ON_HAND_DT,
  FIRST_ON_ORDER_DT AS FIRST_ON_ORDER_DT,
  LAST_ON_ORDER_DT AS LAST_ON_ORDER_DT,
  FIRST_SALE_DT AS FIRST_SALE_DT,
  LAST_SALE_DT AS LAST_SALE_DT,
  LOAD_DT AS LOAD_DT
FROM
  SKU_SITE_PROFILE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SKU_SITE_PROFILE_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_QUERY_ARGUMENTS_1


query_1 = f"""SELECT
  JOB_NAME AS JOB_NAME,
  LAST_CHANGE_DT AS LAST_CHANGE_DT,
  LAST_CHANGE_USER_ID AS LAST_CHANGE_USER_ID,
  TABLE_NAME AS TABLE_NAME,
  SQL_DESC AS SQL_DESC,
  SQL_TX AS SQL_TX,
  SQL_TX2 AS SQL_TX2,
  SQL_TX3 AS SQL_TX3
FROM
  QUERY_ARGUMENTS"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_To_QUERY_ARGUMENTS_1")

# COMMAND ----------
# DBTITLE 1, ASQ_PLSQL_2


query_2 = f"""SELECT
  'M_SKU_SITE_PROFILE_PLSQL' AS JOB_NAME,
  monotonically_increasing_id() AS Monotonically_Increasing_Id"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("ASQ_PLSQL_2")

# COMMAND ----------
# DBTITLE 1, OUT_MPLT_GENERIC_SQL_15


query_3 = f"""SELECT
  JOB_NAME AS MAP_NAME
FROM
  ASQ_PLSQL_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("mGSmGS_Input")

# COMMAND ----------
# DBTITLE 1, INP_MPLT_GENERIC_SQL_4


query_4 = f"""SELECT
  MAP_NAME AS MAP_NAME,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  mGSmGS_Input"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("INP_MPLT_GENERIC_SQL_4")

# COMMAND ----------
# DBTITLE 1, EXP_START_TIME_5


query_5 = f"""SELECT
  MAP_NAME AS MAP_NAME,
  TO_CHAR(SESSSTARTTIME, 'YYYY-MM-DD HH:MI:SS AM') AS var_SESS_START_TIME,
  TO_CHAR(SESSSTARTTIME, 'YYYY-MM-DD HH:MI:SS AM') AS SESS_START_TIME,
  'INSERT INTO QUERY_LOG  VALUES(' || CHR(39) || MAP_NAME || CHR(39) || ',TO_DATE(' || CHR(39) || TO_CHAR(SESSSTARTTIME, 'YYYY-MM-DD HH:MI:SS AM') || CHR(39) || ',' || CHR(39) || 'YYYY-MM-DD HH:MI:SS AM' || CHR(39) || '),NULL, 0,0,' || CHR(39) || 'JOB_STARTED' || CHR(39) || ', 0, 0, 0, 0, ' || CHR(39) || 'Netezza User' || CHR(39) || ')' AS SQL_QUERY_LOG_INSERT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  INP_MPLT_GENERIC_SQL_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_START_TIME_5")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_LOG_INSERT_6


query_6 = f"""SELECT
  MAP_NAME AS null,
  SESS_START_TIME AS null,
  SQL_QUERY_LOG_INSERT AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_START_TIME_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("SQL_QUERY_LOG_INSERT_6")

# COMMAND ----------
# DBTITLE 1, EXP_ONE_ROW_FILTER_7


query_7 = f"""SELECT
  MAP_NAME_output AS MAP_NAME_output,
  SESS_START_TIME_output AS SESS_START_TIME_output,
  var_COUNTER + 1 AS var_COUNTER,
  var_COUNTER + 1 + 1 + 1 + 1 + 1 + 1 + 1 AS out_COUNTER,
  var_COUNTER AS var_COUNTER_prev,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_QUERY_LOG_INSERT_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("EXP_ONE_ROW_FILTER_7")

# COMMAND ----------
# DBTITLE 1, FIL_ONE_ROW_8


query_8 = f"""SELECT
  MAP_NAME_output AS MAP_NAME_output,
  SESS_START_TIME_output AS SESS_START_TIME_output,
  out_COUNTER AS out_COUNTER,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_ONE_ROW_FILTER_7
WHERE
  out_COUNTER = 2"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("FIL_ONE_ROW_8")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_ARG_DATA_FETCH_9


query_9 = f"""SELECT
  MAP_NAME_output AS null,
  SESS_START_TIME_output AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_ONE_ROW_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("SQL_QUERY_ARG_DATA_FETCH_9")

# COMMAND ----------
# DBTITLE 1, EXP_TXT_CONCAT_10


query_10 = f"""SELECT
  SQLError AS SQLError,
  IN_MAP_NAME_output AS IN_MAP_NAME_output,
  SESS_START_TIME_output AS SESS_START_TIME_output,
  out_SQL_TX || DECODE(TRUE, ISNULL(out_SQL_TX2), ' ', out_SQL_TX2) || DECODE(TRUE, ISNULL(out_SQL_TX3), ' ', out_SQL_TX3) AS out_SQL_TXT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_QUERY_ARG_DATA_FETCH_9"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("EXP_TXT_CONCAT_10")

# COMMAND ----------
# DBTITLE 1, SQL_RUN_SQL_FROM_QUERY_ARG_11


query_11 = f"""SELECT
  out_SQL_TXT AS null,
  SQLError AS null,
  IN_MAP_NAME_output AS null,
  SESS_START_TIME_output AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_TXT_CONCAT_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("SQL_RUN_SQL_FROM_QUERY_ARG_11")

# COMMAND ----------
# DBTITLE 1, EXP_ERROR_MSG_12


query_12 = f"""SELECT
  DECODE(
    TRUE,
    ISNULL(SQLError)
    AND ISNULL(SQL_Error_output),
    'NO ERRORS ENCOUNTERED',
    'error  = ' || SQLError
  ) AS out_SQL_Error,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_RUN_SQL_FROM_QUERY_ARG_11"""

df_12 = spark.sql(query_12)

df_12.createOrReplaceTempView("EXP_ERROR_MSG_12")

# COMMAND ----------
# DBTITLE 1, SQL_QUERY_LOG_UPDATE_13


query_13 = f"""SELECT
  null AS null,
  null AS null,
  out_SQL_Error AS null,
  null AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_ERROR_MSG_12
UNION ALL
SELECT
  MAP_NAME_output AS null,
  NumRowsAffected AS null,
  null AS null,
  SESS_START_TIME_output AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_RUN_SQL_FROM_QUERY_ARG_11"""

df_13 = spark.sql(query_13)

df_13.createOrReplaceTempView("SQL_QUERY_LOG_UPDATE_13")

# COMMAND ----------
# DBTITLE 1, EXP_OUTPUT_14


query_14 = f"""SELECT
  MAP_NAME_output AS MAP_NAME,
  DECODE(
    TRUE,
    Sql_Error_output = 'NO ERRORS ENCOUNTERED',
    'SUCCEEDED',
    ABORT('FAILURE IN SQL')
  ) AS out_MPLT_STATUS,
  Sql_Error_output AS out_MPLT_SQL_ERROR,
  SESSSTARTTIME AS out_JOB_START_DATE,
  SQLError AS SQLError,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_QUERY_LOG_UPDATE_13"""

df_14 = spark.sql(query_14)

df_14.createOrReplaceTempView("EXP_OUTPUT_14")

# COMMAND ----------
# DBTITLE 1, OUT_MPLT_GENERIC_SQL


query_15 = f"""SELECT
  MAP_NAME AS MAP_NAME1,
  out_MPLT_STATUS AS MPLT_STATUS,
  out_MPLT_SQL_ERROR AS MPLT_SQL_ERROR,
  out_JOB_START_DATE AS JOB_START_DATE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_OUTPUT_14"""

df_15 = spark.sql(query_15)

df_15.createOrReplaceTempView("OUT_MPLT_GENERIC_SQL_15")

# COMMAND ----------
# DBTITLE 1, QUERY_LOG_TXT


spark.sql("""INSERT INTO
  QUERY_LOG_TXT
SELECT
  MAP_NAME1 AS JOB_NAME,
  MAP_NAME1 AS JOB_NAME,
  MAP_NAME1 AS JOB_NAME,
  MAP_NAME1 AS JOB_NAME,
  JOB_START_DATE AS QUERY_START_TSTMP,
  JOB_START_DATE AS QUERY_START_TSTMP,
  JOB_START_DATE AS QUERY_START_TSTMP,
  JOB_START_DATE AS QUERY_START_TSTMP,
  NULL AS QUERY_END_TSTMP,
  NULL AS RESULT_NBR,
  NULL AS ERROR_NBR,
  MPLT_STATUS AS ERROR_TX,
  MPLT_STATUS AS ERROR_TX,
  MPLT_STATUS AS ERROR_TX,
  MPLT_STATUS AS ERROR_TX,
  NULL AS ROWS_AFFECTED,
  NULL AS ROWS_INSERTED,
  NULL AS ROWS_UPDATED,
  NULL AS ROWS_DELETED,
  NULL AS EXEC_USER
FROM
  OUT_MPLT_GENERIC_SQL_15""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_site_profile_plsql")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_site_profile_plsql", mainWorkflowId, parentName)
