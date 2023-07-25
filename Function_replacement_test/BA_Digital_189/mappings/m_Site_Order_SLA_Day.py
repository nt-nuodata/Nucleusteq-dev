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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_Order_SLA_Day")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Site_Order_SLA_Day", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_ORDER_SLA_DAY_PRE_0


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
  SITE_ORDER_SLA_DAY_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SITE_ORDER_SLA_DAY_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_ORDER_SLA_DAY_PRE_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_ORDER_SLA_DAY_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SITE_ORDER_SLA_DAY_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_ORDER_SLA_DAY1_2


query_2 = f"""SELECT
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

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_SITE_ORDER_SLA_DAY1_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_ORDER_SLA_DAY_3


query_3 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_ORDER_SLA_DAY1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_SITE_ORDER_SLA_DAY_3")

# COMMAND ----------
# DBTITLE 1, Fil_Site_Order_SLA_Day_4


query_4 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SITE_ORDER_SLA_DAY_3
WHERE
  TRUE"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Fil_Site_Order_SLA_Day_4")

# COMMAND ----------
# DBTITLE 1, Jnr_Site_Order_SLA_Pre_5


query_5 = f"""SELECT
  DETAIL.DAY_DT AS DAY_DT,
  DETAIL.LOCATION_ID AS LOCATION_ID,
  DETAIL.START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP,
  DETAIL.END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  DETAIL.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  DETAIL.SLA_DAY_DT AS SLA_DAY_DT,
  DETAIL.SLA_TSTMP AS SLA_TSTMP,
  DETAIL.SLA_TIME_HOUR AS SLA_TIME_HOUR,
  MASTER.DAY_DT AS DAY_DT1,
  MASTER.LOCATION_ID AS LOCATION_ID1,
  MASTER.START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TSTMP1,
  MASTER.END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP1,
  MASTER.LOCATION_TYPE_ID AS LOCATION_TYPE_ID1,
  MASTER.SLA_DAY_DT AS SLA_DAY_DT1,
  MASTER.SLA_TSTMP AS SLA_TSTMP1,
  MASTER.SLA_TIME_HOUR AS SLA_TIME_HOUR1,
  MASTER.UPDATE_TSTMP AS UPDATE_TSTMP,
  MASTER.LOAD_TSTMP AS LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Fil_Site_Order_SLA_Day_4 MASTER
  RIGHT JOIN SQ_Shortcut_to_SITE_ORDER_SLA_DAY_PRE_1 DETAIL ON MASTER.DAY_DT = DETAIL.DAY_DT
  AND MASTER.LOCATION_ID = DETAIL.LOCATION_ID
  AND MASTER.START_ORDER_CREATE_TSTMP = DETAIL.START_ORDER_CREATE_TSTMP
  AND MASTER.END_ORDER_CREATE_TSTMP = DETAIL.END_ORDER_CREATE_TSTMP"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Jnr_Site_Order_SLA_Pre_5")

# COMMAND ----------
# DBTITLE 1, Exp_Site_Order_SLA_6


query_6 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TSTMP AS START_ORDER_CREATE_TS,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(LOAD_TSTMP), now(), LOAD_TSTMP) AS LOAD_TSTMP,
  IFF(
    ISNULL(DAY_DT1),
    'I',
    IFF(
      (LOCATION_TYPE_ID <> LOCATION_TYPE_ID1)
      OR IFF(
        ISNULL(SLA_DAY_DT),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        SLA_DAY_DT
      ) <> IFF(
        ISNULL(SLA_DAY_DT1),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        SLA_DAY_DT1
      )
      OR IFF(
        ISNULL(SLA_TSTMP),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        SLA_TSTMP
      ) <> IFF(
        ISNULL(SLA_TSTMP1),
        TO_DATE('1900-01-01', 'YYYY-MM-DD'),
        SLA_TSTMP1
      )
      OR IFF(ISNULL(SLA_TIME_HOUR), 9999, SLA_TIME_HOUR) <> IFF(ISNULL(SLA_TIME_HOUR1), 9999, SLA_TIME_HOUR1),
      'U',
      'R'
    )
  ) AS UPD_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Jnr_Site_Order_SLA_Pre_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Exp_Site_Order_SLA_6")

# COMMAND ----------
# DBTITLE 1, Fil_Site_Order_SLA_7


query_7 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TS AS START_ORDER_CREATE_TS,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  UPD_FLAG AS UPD_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_Site_Order_SLA_6
WHERE
  UPD_FLAG = 'I'
  OR UPD_FLAG = 'U'"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Fil_Site_Order_SLA_7")

# COMMAND ----------
# DBTITLE 1, Ups_Site_Order_SLA_Day_8


query_8 = f"""SELECT
  DAY_DT AS DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  START_ORDER_CREATE_TS AS START_ORDER_CREATE_TS,
  END_ORDER_CREATE_TSTMP AS END_ORDER_CREATE_TSTMP,
  LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SLA_DAY_DT AS SLA_DAY_DT,
  SLA_TSTMP AS SLA_TSTMP,
  SLA_TIME_HOUR AS SLA_TIME_HOUR,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  UPD_FLAG AS UPD_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    UPD_FLAG = 'I',
    'DD_INSERT',
    IFF(UPD_FLAG = 'U', 'DD_UPDATE')
  ) AS UPDATE_STRATEGY_FLAG
FROM
  Fil_Site_Order_SLA_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("Ups_Site_Order_SLA_Day_8")

# COMMAND ----------
# DBTITLE 1, SITE_ORDER_SLA_DAY


spark.sql("""MERGE INTO SITE_ORDER_SLA_DAY AS TARGET
USING
  Ups_Site_Order_SLA_Day_8 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  AND TARGET.END_ORDER_CREATE_TSTMP = SOURCE.END_ORDER_CREATE_TSTMP
  AND TARGET.START_ORDER_CREATE_TSTMP = SOURCE.START_ORDER_CREATE_TS
  AND TARGET.DAY_DT = SOURCE.DAY_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.DAY_DT = SOURCE.DAY_DT,
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID,
  TARGET.START_ORDER_CREATE_TSTMP = SOURCE.START_ORDER_CREATE_TS,
  TARGET.END_ORDER_CREATE_TSTMP = SOURCE.END_ORDER_CREATE_TSTMP,
  TARGET.LOCATION_TYPE_ID = SOURCE.LOCATION_TYPE_ID,
  TARGET.SLA_DAY_DT = SOURCE.SLA_DAY_DT,
  TARGET.SLA_TSTMP = SOURCE.SLA_TSTMP,
  TARGET.SLA_TIME_HOUR = SOURCE.SLA_TIME_HOUR,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.LOCATION_TYPE_ID = SOURCE.LOCATION_TYPE_ID
  AND TARGET.SLA_DAY_DT = SOURCE.SLA_DAY_DT
  AND TARGET.SLA_TSTMP = SOURCE.SLA_TSTMP
  AND TARGET.SLA_TIME_HOUR = SOURCE.SLA_TIME_HOUR
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.DAY_DT,
    TARGET.LOCATION_ID,
    TARGET.START_ORDER_CREATE_TSTMP,
    TARGET.END_ORDER_CREATE_TSTMP,
    TARGET.LOCATION_TYPE_ID,
    TARGET.SLA_DAY_DT,
    TARGET.SLA_TSTMP,
    TARGET.SLA_TIME_HOUR,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.DAY_DT,
    SOURCE.LOCATION_ID,
    SOURCE.START_ORDER_CREATE_TS,
    SOURCE.END_ORDER_CREATE_TSTMP,
    SOURCE.LOCATION_TYPE_ID,
    SOURCE.SLA_DAY_DT,
    SOURCE.SLA_TSTMP,
    SOURCE.SLA_TIME_HOUR,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_Order_SLA_Day")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Site_Order_SLA_Day", mainWorkflowId, parentName)
