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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_group_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_site_group_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SITEGROUP_0


query_0 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  SITE_GROUP_CD AS SITE_GROUP_CD,
  STORE_NBR AS STORE_NBR,
  SITE_GROUP_DESC AS SITE_GROUP_DESC
FROM
  SITEGROUP"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SITEGROUP_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_SITEGROUP_1


query_1 = f"""SELECT
  DELETE_IND AS DELETE_IND,
  SITE_GROUP_CD AS SITE_GROUP_CD,
  STORE_NBR AS STORE_NBR,
  SITE_GROUP_DESC AS SITE_GROUP_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SITEGROUP_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_SITEGROUP_1")

# COMMAND ----------
# DBTITLE 1, EXP_TRIM_DESC_2


query_2 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  RTRIM(SITE_GROUP_CD) AS SITE_GROUP_CD_OUT,
  DELETE_IND AS DELETE_IND,
  RTRIM(SITE_GROUP_DESC) AS SITE_GROUP_DESC_OUT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_SITEGROUP_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_TRIM_DESC_2")

# COMMAND ----------
# DBTITLE 1, SITE_GROUP_PRE


spark.sql("""INSERT INTO
  SITE_GROUP_PRE
SELECT
  STORE_NBR AS STORE_NBR,
  SITE_GROUP_CD_OUT AS SITE_GROUP_CD,
  DELETE_IND AS DELETE_IND,
  SITE_GROUP_DESC_OUT AS SITE_GROUP_DESC
FROM
  EXP_TRIM_DESC_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_group_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_site_group_pre", mainWorkflowId, parentName)
