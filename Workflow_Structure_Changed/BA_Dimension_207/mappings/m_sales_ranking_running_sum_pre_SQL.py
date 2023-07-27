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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sales_ranking_running_sum_pre_SQL")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sales_ranking_running_sum_pre_SQL", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SALES_RANKING_TOTALS_PRE_0


query_0 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  TOTAL_52WK_COMP_STORES_AMT AS TOTAL_52WK_COMP_STORES_AMT,
  MERCH_52WK_COMP_STORES_AMT AS MERCH_52WK_COMP_STORES_AMT,
  SERVICES_52WK_COMP_STORES_AMT AS SERVICES_52WK_COMP_STORES_AMT,
  SALON_52WK_COMP_STORES_AMT AS SALON_52WK_COMP_STORES_AMT,
  TRAINING_52WK_COMP_STORES_AMT AS TRAINING_52WK_COMP_STORES_AMT,
  HOTEL_DDC_52WK_COMP_STORES_AMT AS HOTEL_DDC_52WK_COMP_STORES_AMT,
  CONSUMABLES_52WK_COMP_STORES_AMT AS CONSUMABLES_52WK_COMP_STORES_AMT,
  HARDGOODS_52WK_COMP_STORES_AMT AS HARDGOODS_52WK_COMP_STORES_AMT,
  SPECIALTY_52WK_COMP_STORES_AMT AS SPECIALTY_52WK_COMP_STORES_AMT
FROM
  SALES_RANKING_TOTALS_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SALES_RANKING_TOTALS_PRE_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SALES_RANKING_SALES_PRE_1


query_1 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  TOTAL_52WK_SALES_AMT AS TOTAL_52WK_SALES_AMT,
  MERCH_52WK_SALES_AMT AS MERCH_52WK_SALES_AMT,
  SERVICES_52WK_SALES_AMT AS SERVICES_52WK_SALES_AMT,
  SALON_52WK_SALES_AMT AS SALON_52WK_SALES_AMT,
  TRAINING_52WK_SALES_AMT AS TRAINING_52WK_SALES_AMT,
  HOTEL_DDC_52WK_SALES_AMT AS HOTEL_DDC_52WK_SALES_AMT,
  CONSUMABLES_52WK_SALES_AMT AS CONSUMABLES_52WK_SALES_AMT,
  HARDGOODS_52WK_SALES_AMT AS HARDGOODS_52WK_SALES_AMT,
  SPECIALTY_52WK_SALES_AMT AS SPECIALTY_52WK_SALES_AMT,
  COMP_CURR_FLAG AS COMP_CURR_FLAG,
  SALES_CURR_FLAG AS SALES_CURR_FLAG,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID
FROM
  SALES_RANKING_SALES_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_SALES_RANKING_SALES_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SQL_TRANSFORM_DUMMY_SOURCE_2


query_2 = f"""SELECT
  START_TSTMP AS START_TSTMP,
  TABLE_NAME AS TABLE_NAME,
  BEGIN_ROW_CNT AS BEGIN_ROW_CNT
FROM
  SQL_TRANSFORM_DUMMY_SOURCE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SQL_TRANSFORM_DUMMY_SOURCE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SALES_RANKING_SALES_PRE_3


query_3 = f"""SELECT
  CURRENT_TIMESTAMP AS START_TSTMP,
  'SALES_RANKING_RUNNING_SUM_PRE' AS TABLE_NAME,
  0 AS BEGIN_ROW_CNT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SALES_RANKING_SALES_PRE_3")

# COMMAND ----------
# DBTITLE 1, SQL_INS_and_DUPS_CHECK_4


query_4 = f"""SELECT
  START_TSTMP AS null,
  TABLE_NAME AS null,
  BEGIN_ROW_CNT AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SALES_RANKING_SALES_PRE_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQL_INS_and_DUPS_CHECK_4")

# COMMAND ----------
# DBTITLE 1, EXP_GET_SESSION_INFO_5


query_5 = f"""SELECT
  TO_CHAR(START_TSTMP_output, 'MM/DD/YYYY HH24:MI:SS') AS START_TSTMP,
  TO_CHAR(now(), 'MM/DD/YYYY HH24:MI:SS') AS END_TSTMP,
  $PMWorkflowName AS WORKFLOW_NAME,
  $PMSessionName AS SESSION_NAME,
  $PMMappingName AS MAPPING_NAME,
  TABLE_NAME_output AS TABLE_NAME,
  BEGIN_ROW_CNT_output AS BEGIN_ROW_CNT,
  NumRowsAffected AS INSERT_ROW_CNT,
  DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
  IFF(
    DUPLICATE_ROW_CNT > 0,
    'There are duplicate records in the table',
    SQLError
  ) AS SQL_TRANSFORM_ERROR,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_INS_and_DUPS_CHECK_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_GET_SESSION_INFO_5")

# COMMAND ----------
# DBTITLE 1, AGG_6


query_6 = f"""SELECT
  START_TSTMP AS START_TSTMP,
  MAX(END_TSTMP) AS END_TSTMP,
  WORKFLOW_NAME AS WORKFLOW_NAME,
  SESSION_NAME AS SESSION_NAME,
  MAPPING_NAME AS MAPPING_NAME,
  TABLE_NAME AS TABLE_NAME,
  TO_CHAR(MAX(BEGIN_ROW_CNT)) AS BEGIN_ROW_CNT,
  TO_CHAR(SUM(INSERT_ROW_CNT)) AS INSERT_ROW_CNT,
  MAX(SQL_TRANSFORM_ERROR) AS SQL_TRANSFORM_ERROR,
  TO_CHAR(SUM(DUPLICATE_ROW_CNT)) AS DUPLICATE_ROW_CNT,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  EXP_GET_SESSION_INFO_5
GROUP BY
  START_TSTMP,
  WORKFLOW_NAME,
  SESSION_NAME,
  MAPPING_NAME,
  TABLE_NAME"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("AGG_6")

# COMMAND ----------
# DBTITLE 1, EXP_CREATE_INS_SQL_7


query_7 = f"""SELECT
  START_TSTMP AS START_TSTMP,
  END_TSTMP AS END_TSTMP,
  WORKFLOW_NAME AS WORKFLOW_NAME,
  SESSION_NAME AS SESSION_NAME,
  MAPPING_NAME AS MAPPING_NAME,
  TABLE_NAME AS TABLE_NAME,
  BEGIN_ROW_CNT AS BEGIN_ROW_CNT,
  INSERT_ROW_CNT AS INSERT_ROW_CNT,
  DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
  SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
  'INSERT INTO SQL_TRANSFORM_LOG VALUES (TO_DATE(' || CHR(39) || START_TSTMP || CHR(39) || ',' || CHR(39) || 'MM/DD/YYYY HH24:MI:SS' || CHR(39) || '),TO_DATE(' || CHR(39) || END_TSTMP || CHR(39) || ',' || CHR(39) || 'MM/DD/YYYY HH24:MI:SS' || CHR(39) || '), ' || CHR(39) || WORKFLOW_NAME || CHR(39) || ', ' || CHR(39) || SESSION_NAME || CHR(39) || ', ' || CHR(39) || MAPPING_NAME || CHR(39) || ', ' || CHR(39) || TABLE_NAME || CHR(39) || ', ' || CHR(39) || BEGIN_ROW_CNT || CHR(39) || ', ' || CHR(39) || INSERT_ROW_CNT || CHR(39) || ', ' || CHR(39) || DUPLICATE_ROW_CNT || CHR(39) || ',  ' || CHR(39) || SQL_TRANSFORM_ERROR || CHR(39) || ')' AS INSERT_SQL,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  AGG_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("EXP_CREATE_INS_SQL_7")

# COMMAND ----------
# DBTITLE 1, SQL_INS_to_SQL_TRANSFORM_LOG_8


query_8 = f"""SELECT
  START_TSTMP AS null,
  END_TSTMP AS null,
  WORKFLOW_NAME AS null,
  SESSION_NAME AS null,
  MAPPING_NAME AS null,
  TABLE_NAME AS null,
  BEGIN_ROW_CNT AS null,
  INSERT_ROW_CNT AS null,
  DUPLICATE_ROW_CNT AS null,
  SQL_TRANSFORM_ERROR AS null,
  INSERT_SQL AS null,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_CREATE_INS_SQL_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("SQL_INS_to_SQL_TRANSFORM_LOG_8")

# COMMAND ----------
# DBTITLE 1, EXP_ABORT_SESSION_9


query_9 = f"""SELECT
  DUPLICATE_ROW_CNT_output AS DUPLICATE_ROW_CNT,
  SQL_TRANSFORM_ERROR_output AS SQL_TRANSFORM_ERROR,
  IFF(
    TO_INTEGER(DUPLICATE_ROW_CNT_output) > 0,
    ABORT('There are duplicates rows in the table'),
    IFF(
      NOT ISNULL (SQL_TRANSFORM_ERROR_output),
      ABORT('There is an error in the INSERT statement')
    )
  ) AS ABORT_SESSION,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQL_INS_to_SQL_TRANSFORM_LOG_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("EXP_ABORT_SESSION_9")

# COMMAND ----------
# DBTITLE 1, SQL_TRANSFORM_DUMMY_TARGET


spark.sql("""INSERT INTO
  SQL_TRANSFORM_DUMMY_TARGET
SELECT
  DUPLICATE_ROW_CNT AS DUPLICATE_ROW_CNT,
  SQL_TRANSFORM_ERROR AS SQL_TRANSFORM_ERROR,
  ABORT_SESSION AS ABORT_SESSION
FROM
  EXP_ABORT_SESSION_9""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sales_ranking_running_sum_pre_SQL")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sales_ranking_running_sum_pre_SQL", mainWorkflowId, parentName)
