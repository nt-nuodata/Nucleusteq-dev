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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_article_category")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_article_category", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DD07V_0


query_0 = f"""SELECT
  DOMNAME AS DOMNAME,
  VALPOS AS VALPOS,
  DDLANGUAGE AS DDLANGUAGE,
  DOMVALUE_L AS DOMVALUE_L,
  DOMVALUE_H AS DOMVALUE_H,
  DDTEXT AS DDTEXT,
  DOMVAL_LD AS DOMVAL_LD,
  DOMVAL_HD AS DOMVAL_HD
FROM
  DD07V"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_DD07V_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DD07V_1


query_1 = f"""SELECT
  CAST(VALPOS AS NUMBER (4)) AS ARTICLE_CATEGORY_ID,
  DOMVALUE_L AS ARTICLE_CATEGORY_CD,
  DDTEXT AS ARTICLE_CATEGORY_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  SAPPR3.Shortcut_to_DD07V_0
WHERE
  DOMNAME = 'ATTYP'
  AND DDLANGUAGE = 'E'
  AND DOMVALUE_L <> ' '"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_DD07V_1")

# COMMAND ----------
# DBTITLE 1, ARTICLE_CATEGORY


spark.sql("""INSERT INTO
  ARTICLE_CATEGORY
SELECT
  ARTICLE_CATEGORY_ID AS ARTICLE_CATEGORY_ID,
  ARTICLE_CATEGORY_CD AS ARTICLE_CATEGORY_CD,
  ARTICLE_CATEGORY_DESC AS ARTICLE_CATEGORY_DESC
FROM
  SQ_Shortcut_to_DD07V_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_article_category")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_article_category", mainWorkflowId, parentName)
