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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_WRF_FOLUP_TYP_A_ff")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_WRF_FOLUP_TYP_A_ff", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_WRF_FOLUP_TYP_A_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  ORIGINAL_ART_NR AS ORIGINAL_ART_NR,
  FOLUP_ART_NR AS FOLUP_ART_NR,
  ASORT AS ASORT,
  FOLLOWUP_TYP_NR AS FOLLOWUP_TYP_NR,
  DATE_FROM AS DATE_FROM,
  DATE_TO AS DATE_TO,
  PRIORITY_A AS PRIORITY_A,
  FOLLOWUP_ACTION AS FOLLOWUP_ACTION,
  TIMESTAMP AS TIMESTAMP,
  CREATE_TIMESTAMP AS CREATE_TIMESTAMP,
  ERNAM AS ERNAM,
  AENAM AS AENAM,
  ORG_ART_FACTOR AS ORG_ART_FACTOR,
  SUBST_ART_FACTOR AS SUBST_ART_FACTOR
FROM
  WRF_FOLUP_TYP_A"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_WRF_FOLUP_TYP_A_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_WRF_FOLUP_TYP_A_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  ORIGINAL_ART_NR AS ORIGINAL_ART_NR,
  FOLUP_ART_NR AS FOLUP_ART_NR,
  ASORT AS ASORT,
  FOLLOWUP_TYP_NR AS FOLLOWUP_TYP_NR,
  DATE_FROM AS DATE_FROM,
  DATE_TO AS DATE_TO,
  PRIORITY_A AS PRIORITY_A,
  FOLLOWUP_ACTION AS FOLLOWUP_ACTION,
  TIMESTAMP AS TIMESTAMP,
  CREATE_TIMESTAMP AS CREATE_TIMESTAMP,
  ERNAM AS ERNAM,
  AENAM AS AENAM,
  ORG_ART_FACTOR AS ORG_ART_FACTOR,
  SUBST_ART_FACTOR AS SUBST_ART_FACTOR,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_WRF_FOLUP_TYP_A_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_WRF_FOLUP_TYP_A_1")

# COMMAND ----------
# DBTITLE 1, sap_WRF_FOLUP_TYP_A_ff


spark.sql("""INSERT INTO
  sap_WRF_FOLUP_TYP_A_ff
SELECT
  MANDT AS MANDT,
  ORIGINAL_ART_NR AS ORIGINAL_ART_NR,
  FOLUP_ART_NR AS FOLUP_ART_NR,
  ASORT AS ASORT,
  FOLLOWUP_TYP_NR AS FOLLOWUP_TYP_NR,
  DATE_FROM AS DATE_FROM,
  DATE_TO AS DATE_TO,
  PRIORITY_A AS PRIORITY_A,
  FOLLOWUP_ACTION AS FOLLOWUP_ACTION,
  TIMESTAMP AS TIMESTAMP,
  CREATE_TIMESTAMP AS CREATE_TIMESTAMP,
  ERNAM AS ERNAM,
  AENAM AS AENAM,
  ORG_ART_FACTOR AS ORG_ART_FACTOR,
  SUBST_ART_FACTOR AS SUBST_ART_FACTOR
FROM
  SQ_Shortcut_to_WRF_FOLUP_TYP_A_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_WRF_FOLUP_TYP_A_ff")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_WRF_FOLUP_TYP_A_ff", mainWorkflowId, parentName)
