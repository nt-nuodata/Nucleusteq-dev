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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ZTB_ART_LOC_SITE_PRE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ZTB_ART_LOC_SITE_PRE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ZTB_ART_LOC_SITE_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POGID AS POGID,
  POGDBKEY AS POGDBKEY,
  EFFECTIVE_DT AS EFFECTIVE_DT,
  NUMPOS AS NUMPOS,
  NOF AS NOF,
  CAPACITY AS CAPACITY,
  PRES_QTY AS PRES_QTY,
  LAST_CHG_DATE AS LAST_CHG_DATE,
  LAST_CHG_TIME AS LAST_CHG_TIME,
  EFFECTIVE_ENDT AS EFFECTIVE_ENDT,
  POS_STATUS AS POS_STATUS,
  POG_TYPE AS POG_TYPE
FROM
  ZTB_ART_LOC_SITE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ZTB_ART_LOC_SITE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ZTB_ART_LOC_SITE_1


query_1 = f"""SELECT
  MANDT AS MANDT,
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POGID AS POGID,
  POGDBKEY AS POGDBKEY,
  EFFECTIVE_DT AS EFFECTIVE_DT,
  NUMPOS AS NUMPOS,
  NOF AS NOF,
  CAPACITY AS CAPACITY,
  PRES_QTY AS PRES_QTY,
  LAST_CHG_DATE AS LAST_CHG_DATE,
  LAST_CHG_TIME AS LAST_CHG_TIME,
  EFFECTIVE_ENDT AS EFFECTIVE_ENDT,
  POS_STATUS AS POS_STATUS,
  POG_TYPE AS POG_TYPE,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ZTB_ART_LOC_SITE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ZTB_ART_LOC_SITE_1")

# COMMAND ----------
# DBTITLE 1, EXP_Load_Tstmp_2


query_2 = f"""SELECT
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POGID AS POGID,
  POGDBKEY AS POGDBKEY,
  EFFECTIVE_DT AS EFFECTIVE_DT,
  NUMPOS AS NUMPOS,
  NOF AS NOF,
  CAPACITY AS CAPACITY,
  PRES_QTY AS PRES_QTY,
  LAST_CHG_DATE AS LAST_CHG_DATE,
  LAST_CHG_TIME AS LAST_CHG_TIME,
  EFFECTIVE_ENDT AS EFFECTIVE_ENDT,
  POS_STATUS AS POS_STATUS,
  POG_TYPE AS POG_TYPE,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ZTB_ART_LOC_SITE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_Load_Tstmp_2")

# COMMAND ----------
# DBTITLE 1, ZTB_ART_LOC_SITE_PRE


spark.sql("""INSERT INTO
  ZTB_ART_LOC_SITE_PRE
SELECT
  ARTICLE AS ARTICLE,
  SITE AS SITE,
  POGID AS POG_ID,
  POGDBKEY AS POG_DBKEY,
  EFFECTIVE_DT AS EFFECTIVE_DT,
  EFFECTIVE_ENDT AS EFFECTIVE_ENDT,
  NUMPOS AS NUMBER_OF_POSITIONS,
  NOF AS NUMBER_OF_FACINGS,
  CAPACITY AS CAPACITY,
  PRES_QTY AS PRESENTATION_QUANTITY,
  POS_STATUS AS POSITION_STATUS,
  POG_TYPE AS POG_TYPE,
  LAST_CHG_DATE AS LAST_CHG_DATE,
  LAST_CHG_TIME AS LAST_CHG_TIME,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_Load_Tstmp_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ZTB_ART_LOC_SITE_PRE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ZTB_ART_LOC_SITE_PRE", mainWorkflowId, parentName)
