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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sales_ranking_date_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sales_ranking_date_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_WEEKS_0


query_0 = f"""SELECT
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

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_WEEKS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WEEKS_1


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
  LWK_WEEK_DT AS LWK_WEEK_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WEEKS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_WEEKS_1")

# COMMAND ----------
# DBTITLE 1, FIL_SALES_RANKING_WK_PRE_2


query_2 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_WEEKS_1
WHERE
  WEEK_DT > ADD_TO_DATE(now(), 'D', -364)
  AND WEEK_DT < now()"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("FIL_SALES_RANKING_WK_PRE_2")

# COMMAND ----------
# DBTITLE 1, EXP_SET_RANKING_WK_3


query_3 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  ADD_TO_DATE(
    TRUNC(now()),
    'D',
    - (
      IFF(
        TO_INTEGER(TO_CHAR(now(), 'D')) = 1,
        0,
        TO_INTEGER(TO_CHAR(now(), 'D')) -1
      )
    )
  ) AS RANKING_WEEK_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_SALES_RANKING_WK_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_SET_RANKING_WK_3")

# COMMAND ----------
# DBTITLE 1, SALES_RANKING_DATE_PRE


spark.sql("""INSERT INTO
  SALES_RANKING_DATE_PRE
SELECT
  WEEK_DT AS WEEK_DT,
  RANKING_WEEK_DT AS RANKING_WEEK_DT
FROM
  EXP_SET_RANKING_WK_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sales_ranking_date_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sales_ranking_date_pre", mainWorkflowId, parentName)
