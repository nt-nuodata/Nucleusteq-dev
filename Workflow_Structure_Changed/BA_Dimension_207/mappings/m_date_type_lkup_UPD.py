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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_date_type_lkup_UPD")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_date_type_lkup_UPD", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, SEQ_DATE_TYPE_ID


spark.sql(
  """CREATE TABLE IF NOT EXISTS SEQ_DATE_TYPE_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int);"""
)

spark.sql(
  """INSERT INTO SEQ_DATE_TYPE_ID(NEXTVAL BIGINT,
CURRVAL BIGINT,
Increment_By Int) VALUES(60, 59, 1)"""
)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_WEEKS_1


query_1 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  CAL_WK AS CAL_WK,
  CAL_WK_NBR AS CAL_WK_NBR,
  CAL_MO AS CAL_MO,
  CAL_MO_NBR AS CAL_MO_NBR,
  CAL_MO_NAME AS CAL_MO_NAME,
  CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
  CAL_QTR AS CAL_QTR,
  CAL_QTR_NBR AS CAL_QTR_NBR,
  CAL_HALF AS CAL_HALF,
  CAL_YR AS CAL_YR,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_WK_NBR AS FISCAL_WK_NBR,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_MO_NBR AS FISCAL_MO_NBR,
  FISCAL_MO_NAME AS FISCAL_MO_NAME,
  FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
  FISCAL_HALF AS FISCAL_HALF,
  FISCAL_YR AS FISCAL_YR,
  LYR_WEEK_DT AS LYR_WEEK_DT,
  LWK_WEEK_DT AS LWK_WEEK_DT
FROM
  WEEKS"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_To_WEEKS_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_WEEKS_2


query_2 = f"""SELECT
  FISCAL_WK_NBR AS DATE_TYPE_DESC2,
  CASE
    WHEN WEEK_DT = (
      SELECT
        WEEK_DT
      FROM
        DAYS
      WHERE
        DAY_DT = CURRENT_DATE - 1
    ) THEN '1'
    WHEN WEEK_DT = (
      SELECT
        LWK_WEEK_DT
      FROM
        DAYS
      WHERE
        DAY_DT = CURRENT_DATE - 1
    ) THEN '1'
    ELSE '0'
  END AS TW_LW_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_WEEKS_1
WHERE
  FISCAL_MO = (
    SELECT
      MAX(FISCAL_MO)
    FROM
      DAYS
    WHERE
      DAY_DT < CURRENT_DATE
  )
ORDER BY
  FISCAL_WK_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_To_WEEKS_2")

# COMMAND ----------
# DBTITLE 1, EXP_WEEKS_3


query_3 = f"""SELECT
  NEXTVAL AS DATE_TYPE_ID,
  'FW' || ' ' || DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
  'Active' AS DATE_TYPE_5WK_STATUS,
  TO_INTEGER(TW_LW_FLAG) AS TW_LW_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_WEEKS_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_WEEKS_3")

spark.sql("""UPDATE SEQ_DATE_TYPE_ID SET CURRVAL = (SELECT MAX(DATE_TYPE_ID) FROM EXP_WEEKS_3) , NEXTVAL = (SELECT MAX(DATE_TYPE_ID) FROM EXP_WEEKS_3) + (SELECT Increment_By FROM EXP_WEEKS_3)""")

# COMMAND ----------
# DBTITLE 1, UPD_DATE_TYPE_ID1_4


query_4 = f"""SELECT
  DATE_TYPE_ID AS DATE_TYPE_ID,
  DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
  DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
  TW_LW_FLAG AS TW_LW_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_WEEKS_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("UPD_DATE_TYPE_ID1_4")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_DAYS_5


query_5 = f"""SELECT
  DAY_DT AS DAY_DT,
  BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
  HOLIDAY_FLAG AS HOLIDAY_FLAG,
  DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
  DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
  CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
  CAL_WK AS CAL_WK,
  CAL_WK_NBR AS CAL_WK_NBR,
  CAL_MO AS CAL_MO,
  CAL_MO_NBR AS CAL_MO_NBR,
  CAL_MO_NAME AS CAL_MO_NAME,
  CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
  CAL_QTR AS CAL_QTR,
  CAL_QTR_NBR AS CAL_QTR_NBR,
  CAL_HALF AS CAL_HALF,
  CAL_YR AS CAL_YR,
  FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
  FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_WK_NBR AS FISCAL_WK_NBR,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_MO_NBR AS FISCAL_MO_NBR,
  FISCAL_MO_NAME AS FISCAL_MO_NAME,
  FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
  FISCAL_HALF AS FISCAL_HALF,
  FISCAL_YR AS FISCAL_YR,
  LYR_WEEK_DT AS LYR_WEEK_DT,
  LWK_WEEK_DT AS LWK_WEEK_DT,
  WEEK_DT AS WEEK_DT,
  EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
  EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
  ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
  ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
  CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
  CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
  CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
  CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
  MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
  MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
  MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
  MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
  PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
  PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS
FROM
  DAYS"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Shortcut_To_DAYS_5")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_DAYS_6


query_6 = f"""SELECT
  (23 + FISCAL_DAY_OF_MO_NBR) AS DATE_TYPE_ID,
  TO_CHAR(DAY_DT, 'MM/DD') AS DATE_TYPE_DESC2,
  DAY_OF_WK_NAME_ABBR AS DATE_TYPE_DESC3,
  CASE
    WHEN WEEK_DT = (
      SELECT
        WEEK_DT
      FROM
        Shortcut_To_DAYS_5
      WHERE
        DAY_DT = CURRENT_DATE - 1
    ) THEN 1
    WHEN WEEK_DT = (
      SELECT
        LWK_WEEK_DT
      FROM
        Shortcut_To_DAYS_5
      WHERE
        DAY_DT = CURRENT_DATE - 1
    ) THEN 1
    ELSE 0
  END AS TW_LW_FLAG,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_DAYS_5
WHERE
  FISCAL_MO = (
    SELECT
      MAX(FISCAL_MO)
    FROM
      Shortcut_To_DAYS_5
    WHERE
      DAY_DT < CURRENT_DATE
  )"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("SQ_Shortcut_To_DAYS_6")

# COMMAND ----------
# DBTITLE 1, EXP_DAYS_7


query_7 = f"""SELECT
  DATE_TYPE_ID AS DATE_TYPE_ID,
  DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
  DATE_TYPE_DESC3 AS DATE_TYPE_DESC3,
  'Active' AS DATE_TYPE_5WK_STATUS,
  TW_LW_FLAG AS TW_LW_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_DAYS_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("EXP_DAYS_7")

# COMMAND ----------
# DBTITLE 1, UPD_DATE_TYPE_ID_8


query_8 = f"""SELECT
  DATE_TYPE_ID AS DATE_TYPE_ID,
  DATE_TYPE_DESC2 AS DATE_TYPE_DESC2,
  DATE_TYPE_DESC3 AS DATE_TYPE_DESC3,
  DATE_TYPE_5WK_STATUS AS DATE_TYPE_5WK_STATUS,
  TW_LW_FLAG AS TW_LW_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_DAYS_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("UPD_DATE_TYPE_ID_8")

# COMMAND ----------
# DBTITLE 1, DATE_TYPE_LKUP


spark.sql("""MERGE INTO DATE_TYPE_LKUP AS TARGET
USING
  UPD_DATE_TYPE_ID_8 AS SOURCE ON TARGET.DATE_TYPE_ID = SOURCE.DATE_TYPE_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.DATE_TYPE_ID = SOURCE.DATE_TYPE_ID,
  TARGET.DATE_TYPE_DESC2 = SOURCE.DATE_TYPE_DESC2,
  TARGET.DATE_TYPE_DESC3 = SOURCE.DATE_TYPE_DESC3,
  TARGET.DATE_TYPE_5WK_STATUS = SOURCE.DATE_TYPE_5WK_STATUS,
  TARGET.TW_LW_FLAG = SOURCE.TW_LW_FLAG""")

# COMMAND ----------
# DBTITLE 1, DATE_TYPE_LKUP


spark.sql("""MERGE INTO DATE_TYPE_LKUP AS TARGET
USING
  UPD_DATE_TYPE_ID1_4 AS SOURCE ON TARGET.DATE_TYPE_ID = SOURCE.DATE_TYPE_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.DATE_TYPE_ID = SOURCE.DATE_TYPE_ID,
  TARGET.DATE_TYPE_DESC2 = SOURCE.DATE_TYPE_DESC2,
  TARGET.DATE_TYPE_5WK_STATUS = SOURCE.DATE_TYPE_5WK_STATUS,
  TARGET.TW_LW_FLAG = SOURCE.TW_LW_FLAG""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_date_type_lkup_UPD")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_date_type_lkup_UPD", mainWorkflowId, parentName)
