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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_vendor_day")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_store_vendor_day", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_STORE_VENDOR_PRE_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  DELETE_IND AS DELETE_IND,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCURE_SITE_ID AS PROCURE_SITE_ID
FROM
  SKU_STORE_VENDOR_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SKU_STORE_VENDOR_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_SHORTCUT_TO_SKU_STORE_VENDOR_PRE_1


query_1 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  STORE_NBR AS STORE_NBR,
  VENDOR_ID AS VENDOR_ID,
  DELETE_IND AS DELETE_IND,
  SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  PROCURE_SITE_ID AS PROCURE_SITE_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SKU_STORE_VENDOR_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_SHORTCUT_TO_SKU_STORE_VENDOR_PRE_1")

# COMMAND ----------
# DBTITLE 1, EXP_COMMON_LOAD_DATE_TRANS_2


query_2 = f"""SELECT
  IFF(
    DELETE_DT = '00000000',
    null,
    TO_DATE(DELETE_DT, 'MMDDYYYY')
  ) AS o_MMDDYYYY_W_DEFAULT_TIME,
  IFF(
    DELETE_DT = '00000000',
    null,
    TO_DATE(DELETE_DT, 'YYYYMMDD')
  ) AS o_YYYYMMDD_W_DEFAULT_TIME,
  TO_DATE(
    ('9999-12-31.' || i_TIME_ONLY),
    'YYYY-MM-DD.HH24MISS'
  ) AS o_TIME_W_DEFAULT_DATE,
  IFF(
    DELETE_DT = '00000000',
    null,
    TO_DATE(
      (DELETE_DT || '.' || i_TIME_ONLY),
      'MMDDYYYY.HH24:MI:SS'
    )
  ) AS o_MMDDYYYY_W_TIME,
  IFF(
    DELETE_DT = '00000000',
    null,
    TO_DATE(
      (DELETE_DT || '.' || i_TIME_ONLY),
      'YYYYMMDD.HH24:MI:SS'
    )
  ) AS o_YYYYMMDD_W_TIME,
  TRUNC(SESSSTARTTIME) AS o_CURRENT_DATE,
  TRUNC(SESSSTARTTIME) AS v_CURRENT_DATE,
  ADD_TO_DATE(TRUNC(SESSSTARTTIME), 'DD', -1) AS o_CURRENT_DATE_MINUS1,
  TO_DATE('0001-01-01', 'YYYY-MM-DD') AS o_DEFAULT_EFF_DATE,
  TO_DATE('9999-12-31', 'YYYY-MM-DD') AS o_DEFAULT_END_DATE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_SHORTCUT_TO_SKU_STORE_VENDOR_PRE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_COMMON_LOAD_DATE_TRANS_2")

# COMMAND ----------
# DBTITLE 1, SKU_STORE_VENDOR_DAY


spark.sql("""INSERT INTO
  SKU_STORE_VENDOR_DAY
SELECT
  ASTSSVP1.SKU_NBR AS SKU_NBR,
  ASTSSVP1.STORE_NBR AS STORE_NBR,
  ASTSSVP1.VENDOR_ID AS VENDOR_ID,
  ASTSSVP1.DELETE_IND AS DELETE_IND,
  ASTSSVP1.SOURCE_LIST_EFF_DT AS SOURCE_LIST_EFF_DT,
  ASTSSVP1.SOURCE_LIST_END_DT AS SOURCE_LIST_END_DT,
  ASTSSVP1.FIXED_VENDOR_IND AS FIXED_VENDOR_IND,
  ASTSSVP1.PROCURE_SITE_ID AS PROCURE_STORE_NBR,
  ECLDT2.o_CURRENT_DATE AS LOAD_DT
FROM
  ASQ_SHORTCUT_TO_SKU_STORE_VENDOR_PRE_1 ASTSSVP1
  INNER JOIN EXP_COMMON_LOAD_DATE_TRANS_2 ECLDT2 ON ASTSSVP1.Monotonically_Increasing_Id = ECLDT2.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_store_vendor_day")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_store_vendor_day", mainWorkflowId, parentName)
