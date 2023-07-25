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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_Dist_Order_LN_SLA_FLG_UPD")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_OMS_Dist_Order_LN_SLA_FLG_UPD", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_DIST_ORDER_LN_SLA_0


query_0 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  OMS_ORDER_ID AS OMS_ORDER_ID,
  OMS_ORDER_LN_ID AS OMS_ORDER_LN_ID,
  OMS_DO_CREATED_ORIG_TSTMP AS OMS_DO_CREATED_ORIG_TSTMP,
  EV_RELEASED_ORIG_TSTMP AS EV_RELEASED_ORIG_TSTMP,
  EV_SHIPPED_ORIG_TSTMP AS EV_SHIPPED_ORIG_TSTMP,
  OMS_DO_SLA_TSTMP AS OMS_DO_SLA_TSTMP,
  OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG,
  OMS_DO_AGE_1_TSTMP AS OMS_DO_AGE_1_TSTMP,
  OMS_DO_AGE_2_TSTMP AS OMS_DO_AGE_2_TSTMP,
  OMS_DO_AGE_3_TSTMP AS OMS_DO_AGE_3_TSTMP,
  OMS_DO_AGE_4_TSTMP AS OMS_DO_AGE_4_TSTMP,
  OMS_DO_AGE_5_TSTMP AS OMS_DO_AGE_5_TSTMP,
  TIME_ZONE AS TIME_ZONE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_DIST_ORDER_LN_SLA"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_DIST_ORDER_LN_SLA_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_DIST_ORDER_LN_SLA_1


query_1 = f"""SELECT
  SLA.OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  SLA.OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  SLA.OMS_DO_SLA_FLAG AS OMS_DO_SLA_FLAG,
  TIMEZONE(
    CURRENT_TIMESTAMP,
    'MST',
    NVL(
      NZT.TIME_ZONE_SUBS,
      (
        CASE
          WHEN LENGTH(SLA.TIME_ZONE) = 4 THEN SUBSTR(SLA.TIME_ZONE, 1, 3)
          ELSE NVL(SLA.TIME_ZONE, 'MST')
        END
      )
    )
  ):: TIMESTAMP + CAST(
    NVL(NZT.TIME_ZONE_ADJ_HOUR, 0) || ' ' || 'hour' AS INTERVAL
  ) AS CURR_TSTMP_LOCAL,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_DIST_ORDER_LN_SLA_0 SLA
  LEFT JOIN NZ_TIME_ZONE_SUBS NZT ON SLA.TIME_ZONE = NZT.TIME_ZONE
WHERE
  SLA.EV_SHIPPED_ORIG_TSTMP IS NULL
  AND (
    SLA.OMS_DO_SLA_TSTMP < (
      TIMEZONE(
        CURRENT_TIMESTAMP,
        'MST',
        NVL(
          NZT.TIME_ZONE_SUBS,
          (
            CASE
              WHEN LENGTH(SLA.TIME_ZONE) = 4 THEN SUBSTR(SLA.TIME_ZONE, 1, 3)
              ELSE NVL(SLA.TIME_ZONE, 'MST')
            END
          )
        )
      ):: TIMESTAMP + CAST(
        NVL(NZT.TIME_ZONE_ADJ_HOUR, 0) || ' ' || 'hour' AS INTERVAL
      )
    )
  )"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_DIST_ORDER_LN_SLA_1")

# COMMAND ----------
# DBTITLE 1, EXP_SHIPMNT_SLA_LOGIC_2


query_2 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  0 AS o_OMS_DO_SLA_FLAG,
  SYSTIMESTAMP() AS UPDATE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_DIST_ORDER_LN_SLA_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_SHIPMNT_SLA_LOGIC_2")

# COMMAND ----------
# DBTITLE 1, UPD_SLA_FLAG_3


query_3 = f"""SELECT
  OMS_DIST_ORDER_ID AS OMS_DIST_ORDER_ID,
  OMS_DIST_ORDER_LN_ID AS OMS_DIST_ORDER_LN_ID,
  o_OMS_DO_SLA_FLAG AS o_OMS_DO_SLA_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_SHIPMNT_SLA_LOGIC_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("UPD_SLA_FLAG_3")

# COMMAND ----------
# DBTITLE 1, OMS_DIST_ORDER_LN_SLA


spark.sql("""MERGE INTO OMS_DIST_ORDER_LN_SLA AS TARGET
USING
  UPD_SLA_FLAG_3 AS SOURCE ON TARGET.OMS_DIST_ORDER_LN_ID = SOURCE.OMS_DIST_ORDER_LN_ID
  AND TARGET.OMS_DIST_ORDER_ID = SOURCE.OMS_DIST_ORDER_ID
  WHEN MATCHED THEN
UPDATE
SET
  TARGET.OMS_DIST_ORDER_ID = SOURCE.OMS_DIST_ORDER_ID,
  TARGET.OMS_DIST_ORDER_LN_ID = SOURCE.OMS_DIST_ORDER_LN_ID,
  TARGET.OMS_DO_SLA_FLAG = SOURCE.o_OMS_DO_SLA_FLAG,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_Dist_Order_LN_SLA_FLG_UPD")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_OMS_Dist_Order_LN_SLA_FLG_UPD", mainWorkflowId, parentName)
