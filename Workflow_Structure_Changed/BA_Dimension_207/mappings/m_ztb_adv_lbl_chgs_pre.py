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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ztb_adv_lbl_chgs_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ztb_adv_lbl_chgs_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZTB_ADV_LBL_CHGS_0


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
  ZTB_ADV_LBL_CHGS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ZTB_ADV_LBL_CHGS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_1


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
  ENH_LBL_ID AS ENH_LBL_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ZTB_ADV_LBL_CHGS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_1")

# COMMAND ----------
# DBTITLE 1, EXP_EFFECTIVE_DATE_2


query_2 = f"""SELECT
  MANDT AS MANDT,
  TO_CHAR(EFFECTIVE_DATE, 'YYYYMMDD') AS EFFECTIVE_DATE,
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POG_TYPE AS POG_TYPE,
  LABEL_SIZE AS LABEL_SIZE,
  LABEL_TYPE AS LABEL_TYPE,
  IFF(
    NOT ISNULL(EXP_LABEL_TYPE)
    AND UPPER(EXP_LABEL_TYPE) != 'X',
    ' ',
    UPPER(EXP_LABEL_TYPE)
  ) AS EXP_LABEL_TYPE,
  SUPPRESS_IND AS SUPPRESS_IND,
  NUM_LABELS AS NUM_LABELS,
  TO_CHAR(CREATE_DATE, 'YYYYMMDD') AS CREATE_DATE,
  ENH_LBL_ID AS ENH_LBL_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ZTB_ADV_LBL_CHGS_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_EFFECTIVE_DATE_2")

# COMMAND ----------
# DBTITLE 1, ZTB_ADV_LBL_CHGS_PRE


spark.sql("""INSERT INTO
  ZTB_ADV_LBL_CHGS_PRE
SELECT
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
  ENH_LBL_ID AS ENH_LBL_ID
FROM
  EXP_EFFECTIVE_DATE_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ztb_adv_lbl_chgs_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ztb_adv_lbl_chgs_pre", mainWorkflowId, parentName)
