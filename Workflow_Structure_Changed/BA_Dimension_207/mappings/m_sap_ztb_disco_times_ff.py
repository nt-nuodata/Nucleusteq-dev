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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_ztb_disco_times_ff")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_ztb_disco_times_ff", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZTB_DISCO_TIMES_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  SCHED_TYPE AS SCHED_TYPE,
  SCHED_CODE AS SCHED_CODE,
  SCHED_DESCR AS SCHED_DESCR
FROM
  ZTB_DISCO_TIMES"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ZTB_DISCO_TIMES_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTB_DISCO_TIMES_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  SCHED_TYPE AS SCHED_TYPE,
  SCHED_CODE AS SCHED_CODE,
  SCHED_DESCR AS SCHED_DESCR,
  DISCO_STRT AS DISCO_STRT,
  DISCO_DC_MAIL AS DISCO_DC_MAIL,
  DISCO_DC AS DISCO_DC,
  DISCO_DC_OUT AS DISCO_DC_OUT,
  DISCO_ST AS DISCO_ST,
  DISCO_DC_OWN AS DISCO_DC_OWN,
  DISCO_ST_OWN AS DISCO_ST_OWN,
  EOL_DATE AS EOL_DATE,
  ST_WOFF_DT AS ST_WOFF_DT,
  DISCO_COFF AS DISCO_COFF,
  DISCO_MD_BT_ST AS DISCO_MD_BT_ST,
  DISCO_MD_BT_END AS DISCO_MD_BT_END,
  DISCO_MD_END_DT AS DISCO_MD_END_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ZTB_DISCO_TIMES_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_DISCO_TIMES_1")

# COMMAND ----------
# DBTITLE 1, FF_ZTB_DISCO_TIMES


spark.sql("""INSERT INTO
  FF_ZTB_DISCO_TIMES
SELECT
  MANDT AS MANDT,
  SCHED_TYPE AS SCHED_TYPE,
  SCHED_CODE AS SCHED_CODE,
  SCHED_DESCR AS SCHED_DESCR,
  DISCO_STRT AS DISCO_STRT,
  DISCO_DC_MAIL AS DISCO_DC_MAIL,
  DISCO_DC AS DISCO_DC,
  DISCO_DC_OUT AS DISCO_DC_OUT,
  DISCO_ST AS DISCO_ST,
  DISCO_DC_OWN AS DISCO_DC_OWN,
  DISCO_ST_OWN AS DISCO_ST_OWN,
  EOL_DATE AS EOL_DATE,
  ST_WOFF_DT AS ST_WOFF_DT,
  DISCO_COFF AS DISCO_COFF,
  DISCO_MD_BT_ST AS DISCO_MD_BT_ST,
  DISCO_MD_BT_END AS DISCO_MD_BT_END,
  DISCO_MD_END_DT AS DISCO_MD_END_DT
FROM
  SQ_Shortcut_to_ZTB_DISCO_TIMES_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_ztb_disco_times_ff")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_ztb_disco_times_ff", mainWorkflowId, parentName)
