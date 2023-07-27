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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ztb_label_chgs_pre_20140308")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ztb_label_chgs_pre_20140308", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZTB_LABEL_CHGS_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  EFFECTIVE_DATE AS EFFECTIVE_DATE,
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POG_TYPE AS POG_TYPE,
  LABEL_SIZE AS LABEL_SIZE,
  LABEL_TYPE AS LABEL_TYPE,
  EXP_LABEL_TYPE AS EXP_LABEL_TYPE,
  SUPPRESS_IND AS SUPPRESS_IND,
  NUM_LABELS AS NUM_LABELS,
  CREATE_DATE AS CREATE_DATE
FROM
  ZTB_LABEL_CHGS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ZTB_LABEL_CHGS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTB_LABEL_CHGS_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  EFFECTIVE_DATE AS EFFECTIVE_DATE,
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POG_TYPE AS POG_TYPE,
  LABEL_SIZE AS LABEL_SIZE,
  LABEL_TYPE AS LABEL_TYPE,
  EXP_LABEL_TYPE AS EXP_LABEL_TYPE,
  SUPPRESS_IND AS SUPPRESS_IND,
  NUM_LABELS AS NUM_LABELS,
  CREATE_DATE AS CREATE_DATE,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ZTB_LABEL_CHGS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_LABEL_CHGS_1")

# COMMAND ----------
# DBTITLE 1, ZTB_LABEL_CHGS_PRE


spark.sql("""INSERT INTO
  ZTB_LABEL_CHGS_PRE
SELECT
  MANDT AS MANDT,
  MANDT AS MANDT,
  EFFECTIVE_DATE AS EFFECTIVE_DATE,
  EFFECTIVE_DATE AS EFFECTIVE_DATE,
  ARTICLE AS ARTICLE,
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  SITE AS SITE,
  POG_TYPE AS POG_TYPE,
  POG_TYPE AS POG_TYPE,
  LABEL_SIZE AS LABEL_SIZE,
  LABEL_SIZE AS LABEL_SIZE,
  LABEL_TYPE AS LABEL_TYPE,
  LABEL_TYPE AS LABEL_TYPE,
  EXP_LABEL_TYPE AS EXP_LABEL_TYPE,
  EXP_LABEL_TYPE AS EXP_LABEL_TYPE,
  SUPPRESS_IND AS SUPPRESS_IND,
  SUPPRESS_IND AS SUPPRESS_IND,
  NUM_LABELS AS NUM_LABELS,
  NUM_LABELS AS NUM_LABELS,
  CREATE_DATE AS CREATE_DATE,
  CREATE_DATE AS CREATE_DATE,
  ENH_LBL_ID AS ENH_LBL_ID
FROM
  SQ_Shortcut_to_ZTB_LABEL_CHGS_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ztb_label_chgs_pre_20140308")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ztb_label_chgs_pre_20140308", mainWorkflowId, parentName)
