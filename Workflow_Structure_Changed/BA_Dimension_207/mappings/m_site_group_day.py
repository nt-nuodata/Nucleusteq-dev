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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_group_day")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_site_group_day", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SITE_GROUP_PRE_0


query_0 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  SITE_GROUP_CD AS SITE_GROUP_CD,
  DELETE_IND AS DELETE_IND,
  SITE_GROUP_DESC AS SITE_GROUP_DESC
FROM
  SITE_GROUP_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SITE_GROUP_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_SITE_GROUP_PRE_1


query_1 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  SITE_GROUP_CD AS SITE_GROUP_CD,
  DELETE_IND AS DELETE_IND,
  SITE_GROUP_DESC AS SITE_GROUP_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SITE_GROUP_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SITE_GROUP_PRE_1")

# COMMAND ----------
# DBTITLE 1, AGG_SITE_GROUP_2


query_2 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  SITE_GROUP_CD AS SITE_GROUP_CD,
  first(DELETE_IND) AS first_DELETE_IND,
  first(SITE_GROUP_DESC) AS first_SITE_GROUP_DESC,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_to_SITE_GROUP_PRE_1
GROUP BY
  STORE_NBR,
  SITE_GROUP_CD"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("AGG_SITE_GROUP_2")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_DT_3


query_3 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  SITE_GROUP_CD AS SITE_GROUP_CD,
  first_DELETE_IND AS first_DELETE_IND,
  first_SITE_GROUP_DESC AS first_SITE_GROUP_DESC,
  trunc(sysdate) AS LOAD_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  AGG_SITE_GROUP_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_LOAD_DT_3")

# COMMAND ----------
# DBTITLE 1, SITE_GROUP_DAY


spark.sql("""INSERT INTO
  SITE_GROUP_DAY
SELECT
  STORE_NBR AS STORE_NBR,
  SITE_GROUP_CD AS SITE_GROUP_CD,
  first_DELETE_IND AS DELETE_IND,
  first_SITE_GROUP_DESC AS SITE_GROUP_DESC,
  LOAD_DT AS LOAD_DT
FROM
  EXP_LOAD_DT_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_site_group_day")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_site_group_day", mainWorkflowId, parentName)
