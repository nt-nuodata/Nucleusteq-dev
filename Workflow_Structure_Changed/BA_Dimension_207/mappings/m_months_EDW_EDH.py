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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_months_EDW_EDH")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_months_EDW_EDH", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Months_0


query_0 = f"""SELECT
  FiscalMo AS FiscalMo,
  FiscalHalf AS FiscalHalf,
  FiscalMoName AS FiscalMoName,
  FiscalMoNameAbbr AS FiscalMoNameAbbr,
  FiscalMoNbr AS FiscalMoNbr,
  FiscalQtr AS FiscalQtr,
  FiscalQtrNbr AS FiscalQtrNbr,
  FiscalYr AS FiscalYr
FROM
  Months"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_Months_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Months_1


query_1 = f"""SELECT
  FiscalMo AS FiscalMo,
  FiscalHalf AS FiscalHalf,
  FiscalMoName AS FiscalMoName,
  FiscalMoNameAbbr AS FiscalMoNameAbbr,
  FiscalMoNbr AS FiscalMoNbr,
  FiscalQtr AS FiscalQtr,
  FiscalQtrNbr AS FiscalQtrNbr,
  FiscalYr AS FiscalYr,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Months_0
WHERE
  FiscalYr = DATEPART(YY, GETDATE()) + 2"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_Months_1")

# COMMAND ----------
# DBTITLE 1, MONTHS


spark.sql("""INSERT INTO
  MONTHS
SELECT
  FiscalMo AS FISCAL_MO,
  FiscalHalf AS FISCAL_HALF,
  FiscalMoName AS FISCAL_MO_NAME,
  FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
  FiscalMoNbr AS FISCAL_MO_NBR,
  FiscalQtr AS FISCAL_QTR,
  FiscalQtrNbr AS FISCAL_QTR_NBR,
  FiscalYr AS FISCAL_YR
FROM
  SQ_Shortcut_to_Months_1""")

# COMMAND ----------
# DBTITLE 1, MONTHS


spark.sql("""INSERT INTO
  MONTHS
SELECT
  FiscalMo AS FISCAL_MO,
  FiscalHalf AS FISCAL_HALF,
  FiscalMoName AS FISCAL_MO_NAME,
  FiscalMoNameAbbr AS FISCAL_MO_NAME_ABBR,
  FiscalMoNbr AS FISCAL_MO_NBR,
  FiscalQtr AS FISCAL_QTR,
  FiscalQtrNbr AS FISCAL_QTR_NBR,
  FiscalYr AS FISCAL_YR
FROM
  SQ_Shortcut_to_Months_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_months_EDW_EDH")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_months_EDW_EDH", mainWorkflowId, parentName)
