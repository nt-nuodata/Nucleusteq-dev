# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ../WorkflowUtility

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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_Order_SLA_Day_DEL")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Site_Order_SLA_Day_DEL", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_ORDER_SLA_DAY_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SITE_ORDER_SLA_DAY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SITE_ORDER_SLA_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_ORDER_SLA_DAY_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_ORDER_SLA_DAY_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SITE_ORDER_SLA_DAY_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_HOURS_DAY_2


query_2 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  STORE_NBR AS STORE_NBR,
  CLOSE_FLAG AS CLOSE_FLAG,
  TIME_ZONE AS TIME_ZONE,
  OPEN_TSTMP AS OPEN_TSTMP,
  CLOSE_TSTMP AS CLOSE_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SITE_HOURS_DAY"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SITE_HOURS_DAY_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_HOURS_DAY_3


query_3 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  CLOSE_FLAG AS CLOSE_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_HOURS_DAY_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SITE_HOURS_DAY_3")

# COMMAND ----------
# DBTITLE 1, Jnr_Site_Hours__Site_Order_SLA_4


query_4 = f"""SELECT
  DETAIL.DAY_DT AS DAY_DT,
  DETAIL.LOCATION_ID AS LOCATION_ID,
  DETAIL.BUSINESS_AREA AS BUSINESS_AREA,
  DETAIL.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  DETAIL.CLOSE_FLAG AS CLOSE_FLAG,
  DETAIL.UPDATE_TSTMP AS UPDATE_TSTMP,
  MASTER.DAY_DT AS DAY_DT1,
  MASTER.LOCATION_ID AS LOCATION_ID1,
  MASTER.START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  MASTER.END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  MASTER.LOCATION_TYPE_ID AS LOCATION_TYPE_ID1,
  MASTER.UPDATE_TSTMP AS UPDATE_TSTMP1,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SITE_ORDER_SLA_DAY_1 MASTER
  INNER JOIN SQ_Shortcut_to_SITE_HOURS_DAY_3 DETAIL ON MASTER.DAY_DT = DETAIL.DAY_DT
  AND MASTER.LOCATION_ID = DETAIL.LOCATION_ID
  AND MASTER.LOCATION_TYPE_ID = DETAIL.LOCATION_TYPE_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Jnr_Site_Hours__Site_Order_SLA_4")

# COMMAND ----------
# DBTITLE 1, Fil_SiteHours_SiteOrderSLA_5


query_5 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  BUSINESS_AREA AS BUSINESS_AREA,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  UPDATE_TSTMP1 AS UPDATE_TSTMP1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Jnr_Site_Hours__Site_Order_SLA_4
WHERE
  TRUNC(UPDATE_TSTMP) >= ADD_TO_DATE(TRUNC(now()), 'DD', -3)
  AND TRUNC(UPDATE_TSTMP) > TRUNC(UPDATE_TSTMP1)
  AND (
    BUSINESS_AREA = 'Store'
    OR BUSINESS_AREA = 'DC'
    OR BUSINESS_AREA = 'Vendor'
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Fil_SiteHours_SiteOrderSLA_5")

# COMMAND ----------
# DBTITLE 1, Ups_Delete_6


query_6 = f"""SELECT
  DAY_DT AS DAY_DT1,
  LOCATION_ID AS LOCATION_ID1,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_SiteHours_SiteOrderSLA_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Ups_Delete_6")

# COMMAND ----------
# DBTITLE 1, SITE_ORDER_SLA_DAY


spark.sql("""MERGE INTO SITE_ORDER_SLA_DAY AS TARGET
USING
  Ups_Delete_6 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID1
  AND TARGET.END_ORDER_CREATE_TSTMP = SOURCE.END_ORDER_CREATE_TSTMP
  AND TARGET.START_ORDER_CREATE_TSTMP = SOURCE.START_ORDER_CREATE_TSTMP
  AND TARGET.DAY_DT = SOURCE.DAY_DT1
  WHEN MATCHED THEN DELETE""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_Order_SLA_Day_DEL")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Site_Order_SLA_Day_DEL", mainWorkflowId, parentName)
