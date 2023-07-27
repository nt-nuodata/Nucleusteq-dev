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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_gl_c003_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_gl_c003_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_C003_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  KAPPL AS KAPPL,
  KSCHL AS KSCHL,
  KTOPL AS KTOPL,
  VKORG AS VKORG,
  KTGRM AS KTGRM,
  KVSL1 AS KVSL1,
  SAKN1 AS SAKN1,
  SAKN2 AS SAKN2
FROM
  C003"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_C003_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_C003_1


query_1 = f"""SELECT
  Shortcut_to_C003_0.VKORG,
  Shortcut_to_C003_0.KTGRM,
  CAST(Shortcut_to_C003_0.SAKN1 AS NUMBER (5)) SAKN1
FROM
  SAPPR3.Shortcut_to_C003_0
WHERE
  KTOPL = 'PCOA'
  AND KVSL1 = 'ERL'"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_C003_1")

# COMMAND ----------
# DBTITLE 1, GL_C003_PRE


spark.sql("""INSERT INTO
  GL_C003_PRE
SELECT
  VKORG AS SALES_ORG,
  KTGRM AS ACCT_ASSIGNMENT_GRP,
  SAKN1 AS GL_ACCT_NBR
FROM
  SQ_Shortcut_to_C003_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_gl_c003_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_gl_c003_pre", mainWorkflowId, parentName)
