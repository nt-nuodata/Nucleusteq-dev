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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_dm_dept_segments")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_dm_dept_segments", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DM_DEPT_SEGMENTS_USER_0


query_0 = f"""SELECT
  CONSUM_ID AS CONSUM_ID,
  CONSUM_DESC AS CONSUM_DESC,
  SEGMENT_ID AS SEGMENT_ID,
  SEGMENT_DESC AS SEGMENT_DESC,
  SAP_DEPT_ID AS SAP_DEPT_ID
FROM
  DM_DEPT_SEGMENTS_USER"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_DM_DEPT_SEGMENTS_USER_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DM_DEPT_SEGMENTS_USER_1


query_1 = f"""SELECT
  CONSUM_ID AS CONSUM_ID,
  CONSUM_DESC AS CONSUM_DESC,
  SEGMENT_ID AS SEGMENT_ID,
  SEGMENT_DESC AS SEGMENT_DESC,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_DM_DEPT_SEGMENTS_USER_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_DM_DEPT_SEGMENTS_USER_1")

# COMMAND ----------
# DBTITLE 1, DM_DEPT_SEGMENTS


spark.sql("""INSERT INTO
  DM_DEPT_SEGMENTS
SELECT
  SAP_DEPT_ID AS SAP_DEPT_ID,
  CONSUM_ID AS CONSUM_ID,
  CONSUM_DESC AS CONSUM_DESC,
  SEGMENT_ID AS SEGMENT_ID,
  SEGMENT_DESC AS SEGMENT_DESC
FROM
  SQ_Shortcut_to_DM_DEPT_SEGMENTS_USER_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_dm_dept_segments")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_dm_dept_segments", mainWorkflowId, parentName)
