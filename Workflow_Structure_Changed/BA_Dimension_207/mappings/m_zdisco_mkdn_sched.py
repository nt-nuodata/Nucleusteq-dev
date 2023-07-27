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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_zdisco_mkdn_sched")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_zdisco_mkdn_sched", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZTB_DISCO_MD_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  ZZMD_SCH_ID AS ZZMD_SCH_ID,
  MD_SCH_DESC AS MD_SCH_DESC
FROM
  ZTB_DISCO_MD"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ZTB_DISCO_MD_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTB_DISCO_MD_1


query_1 = f"""SELECT
  Shortcut_to_ZTB_DISCO_MD_0.ZZMD_SCH_ID AS ZZMD_SCH_ID,
  Shortcut_to_ZTB_DISCO_MD_0.MD_SCH_DESC AS MD_SCH_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  SAPPR3.Shortcut_to_ZTB_DISCO_MD_0
WHERE
  Shortcut_to_ZTB_DISCO_MD_0.MANDT = '100'"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_DISCO_MD_1")

# COMMAND ----------
# DBTITLE 1, ZDISCO_MKDN_SCHED


spark.sql("""INSERT INTO
  ZDISCO_MKDN_SCHED
SELECT
  ZZMD_SCH_ID AS ZDISCO_MKDN_SCHED_ID,
  MD_SCH_DESC AS ZDISCO_MKDN_SCHED_DESC
FROM
  SQ_Shortcut_to_ZTB_DISCO_MD_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_zdisco_mkdn_sched")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_zdisco_mkdn_sched", mainWorkflowId, parentName)
