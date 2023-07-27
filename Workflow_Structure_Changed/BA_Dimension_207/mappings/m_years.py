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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_years")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_years", variablesTableName, mainWorkflowId)

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
  DISTINCT FiscalYr AS FiscalYr,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Months_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_Months_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


query_2 = f"""SELECT
  FiscalYr AS FiscalYr,
  to_char(ADD_TO_DATE (sysdate, 'YY', {year}), 'YYYY') AS O_FiscalYr,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Months_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, FILTRANS_3


query_3 = f"""SELECT
  FiscalYr AS FiscalYr,
  O_FiscalYr AS O_FiscalYr,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXPTRANS_2
WHERE
  FiscalYr = O_FiscalYr"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("FILTRANS_3")

# COMMAND ----------
# DBTITLE 1, YEARS


spark.sql("""INSERT INTO
  YEARS
SELECT
  NULL AS FISCAL_YR
FROM
  FILTRANS_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_years")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_years", mainWorkflowId, parentName)
