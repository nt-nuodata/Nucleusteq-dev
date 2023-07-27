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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Sap_ZTPIM_Map_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Sap_ZTPIM_Map_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZTPIM_MAP_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  ARTICLE AS ARTICLE,
  DATAB AS DATAB,
  DATBI AS DATBI,
  MAP_PRICE AS MAP_PRICE,
  CREATED_BY AS CREATED_BY,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_DATE AS LAST_CHANGED_DATE
FROM
  ZTPIM_MAP"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ZTPIM_MAP_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTPIM_MAP_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  ARTICLE AS ARTICLE,
  DATAB AS DATAB,
  DATBI AS DATBI,
  MAP_PRICE AS MAP_PRICE,
  CREATED_BY AS CREATED_BY,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ZTPIM_MAP_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTPIM_MAP_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  MANDT AS MANDT,
  ARTICLE AS ARTICLE,
  MAP_PRICE AS MAP_PRICE,
  CREATED_BY AS CREATED_BY,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
  DATAB AS DATAB,
  DATBI AS DATBI,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ZTPIM_MAP_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, SAP_ZTPIM_MAP_PRE


spark.sql("""INSERT INTO
  SAP_ZTPIM_MAP_PRE
SELECT
  MANDT AS MANDT,
  ARTICLE AS ARTICLE,
  MAP_PRICE AS MAP_PRICE,
  CREATED_BY AS CREATED_BY,
  LAST_CHANGED_BY AS LAST_CHANGED_BY,
  LAST_CHANGED_DATE AS LAST_CHANGED_DATE,
  DATAB AS DATAB,
  DATBI AS DATBI,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Sap_ZTPIM_Map_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Sap_ZTPIM_Map_Pre", mainWorkflowId, parentName)
