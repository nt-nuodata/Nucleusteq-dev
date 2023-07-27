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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Days_Pre1")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Days_Pre1", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Days_Pre1_0


query_0 = f"""SELECT
  DayDt AS DayDt
FROM
  Days_Pre1"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Days_Pre1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Days_Pre1_1


query_1 = f"""DECLARE @ StartDateTime DATETIME DECLARE @ EndDateTime DATETIME
SET
  @ StartDateTime = (
    select
      distinct dateadd(day, -6, startwkdt)
    from
      fiscalperiod
    where
      fiscalmo = CONCAT(DATEPART(YEAR, dateadd(YY, {year}, GETDATE())), '01')
  )
SET
  @ EndDateTime = (
    select
      distinct endwkdt
    from
      fiscalperiod
    where
      fiscalmo = CONCAT(
        DATEPART(YEAR, dateadd(YY, {year} + 1, GETDATE())),
        '12'
      )
  );
WITH DateRange(DateData) AS (
  SELECT
    @ StartDateTime as Date
  UNION ALL
  SELECT
    DATEADD(d, 1, DateData)
  FROM
    DateRange
  WHERE
    DateData < @ EndDateTime
)
SELECT
  DateData
FROM
  DateRange OPTION (MAXRECURSION 0)"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Days_Pre1_1")

# COMMAND ----------
# DBTITLE 1, Days_Pre1


spark.sql("""INSERT INTO
  Days_Pre1
SELECT
  DayDt AS DayDt
FROM
  SQ_Days_Pre1_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Days_Pre1")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Days_Pre1", mainWorkflowId, parentName)
