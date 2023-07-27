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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_label_pog_type_ADVANCE")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_label_pog_type_ADVANCE", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_LABEL_POG_TYPE_0


query_0 = f"""SELECT
  LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  LABEL_POG_TYPE_DESC AS LABEL_POG_TYPE_DESC,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  LABEL_POG_TYPE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_LABEL_POG_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LABEL_POG_TYPE_1


query_1 = f"""SELECT
  LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_LABEL_POG_TYPE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_LABEL_POG_TYPE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_LABEL_DAY_STORE_SKU_2


query_2 = f"""SELECT
  LABEL_CHANGE_DT AS LABEL_CHANGE_DT,
  LOCATION_ID AS LOCATION_ID,
  PRODUCT_ID AS PRODUCT_ID,
  ACTUAL_FLAG AS ACTUAL_FLAG,
  LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  LABEL_SIZE_ID AS LABEL_SIZE_ID,
  LABEL_TYPE_ID AS LABEL_TYPE_ID,
  EXPIRATION_FLAG AS EXPIRATION_FLAG,
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  WEEK_DT AS WEEK_DT,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_YR AS FISCAL_YR,
  SUPPRESSED_FLAG AS SUPPRESSED_FLAG,
  LABEL_CNT AS LABEL_CNT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  LABEL_DAY_STORE_SKU"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_LABEL_DAY_STORE_SKU_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_LABEL_DAY_STORE_SKU_3


query_3 = f"""SELECT
  DISTINCT LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_LABEL_DAY_STORE_SKU_2
WHERE
  Shortcut_to_LABEL_DAY_STORE_SKU_2.LOAD_TSTMP > CURRENT_DATE"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_LABEL_DAY_STORE_SKU_3")

# COMMAND ----------
# DBTITLE 1, JNR_Label_Pog_Type_4


query_4 = f"""SELECT
  DETAIL.LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  MASTER.LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD1,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_LABEL_POG_TYPE_1 MASTER
  RIGHT JOIN SQ_Shortcut_to_LABEL_DAY_STORE_SKU_3 DETAIL ON MASTER.LABEL_POG_TYPE_CD = DETAIL.LABEL_POG_TYPE_CD"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_Label_Pog_Type_4")

# COMMAND ----------
# DBTITLE 1, FIL_Label_Pog_Type_5


query_5 = f"""SELECT
  LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  LABEL_POG_TYPE_CD1 AS LABEL_POG_TYPE_CD1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_Label_Pog_Type_4
WHERE
  ISNULL(LABEL_POG_TYPE_CD)"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FIL_Label_Pog_Type_5")

# COMMAND ----------
# DBTITLE 1, EXP_Set_Label_Pog_Type_6


query_6 = f"""SELECT
  LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  'Unavailable_' || LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_DESC,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_Label_Pog_Type_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_Set_Label_Pog_Type_6")

# COMMAND ----------
# DBTITLE 1, LABEL_POG_TYPE


spark.sql("""INSERT INTO
  LABEL_POG_TYPE
SELECT
  LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  LABEL_POG_TYPE_CD AS LABEL_POG_TYPE_CD,
  LABEL_POG_TYPE_DESC AS LABEL_POG_TYPE_DESC,
  LABEL_POG_TYPE_DESC AS LABEL_POG_TYPE_DESC,
  LOAD_TSTMP AS LOAD_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_Set_Label_Pog_Type_6""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_label_pog_type_ADVANCE")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_label_pog_type_ADVANCE", mainWorkflowId, parentName)