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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_UDH_TO_NZ_PS2_DAYS_TY_LY")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_UDH_TO_NZ_PS2_DAYS_TY_LY", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UDH_PS2_DAYS_TY_LY_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  TRANS_DAY_DT AS TRANS_DAY_DT,
  WEEK_DT AS WEEK_DT,
  FISCAL_YR AS FISCAL_YR,
  TY_LY_FLAG AS TY_LY_FLAG,
  COMP_IND AS COMP_IND
FROM
  UDH_PS2_DAYS_TY_LY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_UDH_PS2_DAYS_TY_LY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UDH_PS2_DAYS_TY_LY_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  TRANS_DAY_DT AS TRANS_DAY_DT,
  WEEK_DT AS WEEK_DT,
  FISCAL_YR AS FISCAL_YR,
  TY_LY_FLAG AS TY_LY_FLAG,
  COMP_IND AS COMP_IND,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_UDH_PS2_DAYS_TY_LY_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_PS2_DAYS_TY_LY_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


query_2 = f"""SELECT
  to_date(to_char(DAY_DT), 'YYYY/MM/DD') AS o_DAY_DT,
  to_date(to_char(TRANS_DAY_DT), 'YYYY/MM/DD') AS o_TRANS_DAY_DT,
  to_date(to_char(WEEK_DT), 'YYYY/MM/DD') AS o_WEEK_DT,
  FISCAL_YR AS FISCAL_YR,
  TY_LY_FLAG AS TY_LY_FLAG,
  COMP_IND AS COMP_IND,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_PS2_DAYS_TY_LY_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, PS2_DAYS_TY_LY


spark.sql("""INSERT INTO
  PS2_DAYS_TY_LY
SELECT
  o_DAY_DT AS DAY_DT,
  o_TRANS_DAY_DT AS TRANS_DAY_DT,
  o_WEEK_DT AS WEEK_DT,
  FISCAL_YR AS FISCAL_YR,
  TY_LY_FLAG AS TY_LY_FLAG,
  COMP_IND AS COMP_IND
FROM
  EXPTRANS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_UDH_TO_NZ_PS2_DAYS_TY_LY")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_UDH_TO_NZ_PS2_DAYS_TY_LY", mainWorkflowId, parentName)
