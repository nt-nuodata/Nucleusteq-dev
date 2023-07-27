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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_cyr_lyr_weeks_EDW")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_cyr_lyr_weeks_EDW", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CyrLyrWeeks_0


query_0 = f"""SELECT
  LyrWeekDt AS LyrWeekDt,
  WeekDt AS WeekDt,
  CyrCalHalf AS CyrCalHalf,
  CyrCalMo AS CyrCalMo,
  CyrCalMoName AS CyrCalMoName,
  CyrCalMoNameAbbr AS CyrCalMoNameAbbr,
  CyrCalMoNbr AS CyrCalMoNbr,
  CyrCalQtr AS CyrCalQtr,
  CyrCalQtrNbr AS CyrCalQtrNbr,
  CyrCalWk AS CyrCalWk,
  CyrCalYr AS CyrCalYr,
  CyrFiscalHalf AS CyrFiscalHalf,
  CyrFiscalMoName AS CyrFiscalMoName,
  CyrFiscalMoNameAbbr AS CyrFiscalMoNameAbbr,
  CyrFiscalMoNbr AS CyrFiscalMoNbr,
  CyrFiscalQtr AS CyrFiscalQtr,
  CyrFiscalQtrNbr AS CyrFiscalQtrNbr,
  CyrFiscalWk AS CyrFiscalWk,
  CyrFiscalWkNbr AS CyrFiscalWkNbr,
  CyrFiscalYr AS CyrFiscalYr,
  CyrLwkWeekDt AS CyrLwkWeekDt,
  CyrFiscalMo AS CyrFiscalMo
FROM
  CyrLyrWeeks"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_CyrLyrWeeks_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_CyrLyrWeeks_1


query_1 = f"""SELECT
  LyrWeekDt AS LwkWeekDt,
  WeekDt AS WeekDt,
  CyrCalHalf AS CyrCalHalf,
  CyrCalMo AS CyrCalMo,
  CyrCalMoName AS CyrCalMoName,
  CyrCalMoNameAbbr AS CyrCalMoNameAbbr,
  CyrCalMoNbr AS CyrCalMoNbr,
  CyrCalQtr AS CyrCalQtr,
  CyrCalQtrNbr AS CyrCalQtrNbr,
  CyrCalWk AS CyrCalWk,
  CyrCalYr AS CyrCalYr,
  CyrFiscalHalf AS CyrFiscalHalf,
  CyrFiscalMoName AS CyrFiscalMoName,
  CyrFiscalMoNameAbbr AS CyrFiscalMoNameAbbr,
  CyrFiscalMoNbr AS CyrFiscalMoNbr,
  CyrFiscalQtr AS CyrFiscalQtr,
  CyrFiscalQtrNbr AS CyrFiscalQtrNbr,
  CyrFiscalWk AS CyrFiscalWk,
  CyrFiscalWkNbr AS CyrFiscalWkNbr,
  CyrFiscalYr AS CyrFiscalYr,
  CyrLwkWeekDt AS CyrLwkWeekDt,
  CyrFiscalMo AS CyrFiscalMo,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_CyrLyrWeeks_0
WHERE
  CyrFiscalYr = DATEPART(YY, GETDATE()) + 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_CyrLyrWeeks_1")

# COMMAND ----------
# DBTITLE 1, CYR_LYR_WEEKS


spark.sql("""INSERT INTO
  CYR_LYR_WEEKS
SELECT
  LwkWeekDt AS LYR_WEEK_DT,
  WeekDt AS WEEK_DT,
  CyrCalHalf AS CYR_CAL_HALF,
  CyrCalMo AS CYR_CAL_MO,
  CyrCalMoName AS CYR_CAL_MO_NAME,
  CyrCalMoNameAbbr AS CYR_CAL_MO_NAME_ABBR,
  CyrCalMoNbr AS CYR_CAL_MO_NBR,
  CyrCalQtr AS CYR_CAL_QTR,
  CyrCalQtrNbr AS CYR_CAL_QTR_NBR,
  CyrCalWk AS CYR_CAL_WK,
  CyrCalYr AS CYR_CAL_YR,
  CyrFiscalHalf AS CYR_FISCAL_HALF,
  CyrFiscalMoName AS CYR_FISCAL_MO_NAME,
  CyrFiscalMoNameAbbr AS CYR_FISCAL_MO_NAME_ABBR,
  CyrFiscalMoNbr AS CYR_FISCAL_MO_NBR,
  CyrFiscalQtr AS CYR_FISCAL_QTR,
  CyrFiscalQtrNbr AS CYR_FISCAL_QTR_NBR,
  CyrFiscalWk AS CYR_FISCAL_WK,
  CyrFiscalWkNbr AS CYR_FISCAL_WK_NBR,
  CyrFiscalYr AS CYR_FISCAL_YR,
  CyrLwkWeekDt AS CYR_LWK_WEEK_DT,
  CyrFiscalMo AS CYR_FISCAL_MO
FROM
  SQ_Shortcut_to_CyrLyrWeeks_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_cyr_lyr_weeks_EDW")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_cyr_lyr_weeks_EDW", mainWorkflowId, parentName)
