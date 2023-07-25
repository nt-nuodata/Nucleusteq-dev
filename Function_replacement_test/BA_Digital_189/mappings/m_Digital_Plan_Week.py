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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Digital_Plan_Week")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Digital_Plan_Week", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_DIGITAL_PLAN_WEEK1_0


query_0 = f"""SELECT
  FISCAL_YR AS FISCAL_YR,
  FISCAL_WK AS FISCAL_WK,
  DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  PLAN_SALES_AMT_USD AS PLAN_SALES_AMT_USD,
  PLAN_MARGIN_AMT_USD AS PLAN_MARGIN_AMT_USD,
  PLAN_ORDER_CNT AS PLAN_ORDER_CNT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  DIGITAL_PLAN_WEEK"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_DIGITAL_PLAN_WEEK1_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_DIGITAL_PLAN_WEEK_1


query_1 = f"""SELECT
  FISCAL_YR AS FISCAL_YR,
  FISCAL_WK AS FISCAL_WK,
  DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  PLAN_SALES_AMT_USD AS PLAN_SALES_AMT_USD,
  PLAN_MARGIN_AMT_USD AS PLAN_MARGIN_AMT_USD,
  PLAN_ORDER_CNT AS PLAN_ORDER_CNT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_DIGITAL_PLAN_WEEK1_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_DIGITAL_PLAN_WEEK_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_Digital_Plan_2


query_2 = f"""SELECT
  FISCAL_WK AS FISCAL_WK,
  DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  PLAN_SALES_AMT_USD AS PLAN_SALES_AMT_USD,
  PLAN_MARGIN_AMT_USD AS PLAN_MARGIN_AMT_USD,
  PLAN_ORDER_CNT AS PLAN_ORDER_CNT
FROM
  Digital_Plan"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_Digital_Plan_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_Digital_Plan_3


query_3 = f"""SELECT
  FISCAL_WK AS FISCAL_WK,
  DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  PLAN_SALES_AMT_USD AS PLAN_SALES_AMT_USD,
  PLAN_MARGIN_AMT_USD AS PLAN_MARGIN_AMT_USD,
  PLAN_ORDER_CNT AS PLAN_ORDER_CNT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_Digital_Plan_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_Digital_Plan_3")

# COMMAND ----------
# DBTITLE 1, EXP_SOURCE_COLUMNS_4


query_4 = f"""SELECT
  FISCAL_WK AS FISCAL_WK,
  TRUNC(FISCAL_WK / 100, 0) AS FISCAL_YR,
  IFF(
    INSTR(
      UPPER(LTRIM(RTRIM(DIGITAL_CHANNEL))),
      'MARKETPLACE'
    ) > 0,
    'MARKETPLACE',
    UPPER(LTRIM(RTRIM(DIGITAL_CHANNEL)))
  ) AS DIGITAL_CHANNEL,
  ROUND(
    TO_DECIMAL(REPLACECHR(1, PLAN_SALES_AMT_USD, ',', '')),
    8
  ) AS PLAN_SALES_AMT_USD,
  ROUND(
    TO_DECIMAL(REPLACECHR(1, PLAN_MARGIN_AMT_USD, ',', '')),
    8
  ) AS PLAN_MARGIN_AMT_USD,
  ROUND(
    TO_DECIMAL(REPLACECHR(1, PLAN_ORDER_CNT, ',', '')),
    4
  ) AS PLAN_ORDER_CNT,
  now() AS UPDATE_TSTMP,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_Digital_Plan_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_SOURCE_COLUMNS_4")

# COMMAND ----------
# DBTITLE 1, JNR_TARGET_5


query_5 = f"""SELECT
  MASTER.FISCAL_YR AS FISCAL_YR,
  MASTER.FISCAL_WK AS FISCAL_WK,
  MASTER.DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  MASTER.PLAN_SALES_AMT_USD AS PLAN_SALES_AMT_USD,
  MASTER.PLAN_MARGIN_AMT_USD AS PLAN_MARGIN_AMT_USD,
  MASTER.PLAN_ORDER_CNT AS PLAN_ORDER_CNT,
  DETAIL.FISCAL_YR AS lkp_FISCAL_YR,
  DETAIL.FISCAL_WK AS lkp_FISCAL_WK,
  DETAIL.DIGITAL_CHANNEL AS lkp_DIGITAL_CHANNEL,
  DETAIL.PLAN_SALES_AMT_USD AS lkp_PLAN_SALES_AMT_USD,
  DETAIL.PLAN_MARGIN_AMT_USD AS lkp_PLAN_MARGIN_AMT_USD,
  DETAIL.PLAN_ORDER_CNT AS lkp_PLAN_ORDER_CNT,
  DETAIL.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_SOURCE_COLUMNS_4 MASTER
  LEFT JOIN SQ_Shortcut_to_DIGITAL_PLAN_WEEK_1 DETAIL ON MASTER.FISCAL_YR = DETAIL.FISCAL_YR
  AND MASTER.FISCAL_WK = DETAIL.FISCAL_WK
  AND MASTER.DIGITAL_CHANNEL = DETAIL.DIGITAL_CHANNEL"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("JNR_TARGET_5")

# COMMAND ----------
# DBTITLE 1, EXP_Before_Filter_6


query_6 = f"""SELECT
  FISCAL_YR AS FISCAL_YR,
  FISCAL_WK AS FISCAL_WK,
  DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  PLAN_SALES_AMT_USD AS FORECAST_SALES_AMT_USD,
  PLAN_MARGIN_AMT_USD AS FORECAST_MARGIN_AMT_USD,
  PLAN_ORDER_CNT AS FORECAST_ORDER_CNT,
  lkp_FISCAL_YR AS lkp_FISCAL_YR,
  lkp_FISCAL_WK AS lkp_FISCAL_WK,
  lkp_DIGITAL_CHANNEL AS lkp_DIGITAL_CHANNEL,
  lkp_PLAN_SALES_AMT_USD AS lkp_FORECAST_SALES_AMT_USD,
  lkp_PLAN_MARGIN_AMT_USD AS lkp_FORECAST_MARGIN_AMT_USD,
  lkp_PLAN_ORDER_CNT AS lkp_FORECAST_ORDER_CNT,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  IFF (
    ISNULL(lkp_FISCAL_YR),
    1,
    IFF (
      NOT ISNULL(lkp_FISCAL_YR)
      AND (
        IFF(
          ISNULL(PLAN_SALES_AMT_USD),
          999999999.9999,
          PLAN_SALES_AMT_USD
        ) <> IFF(
          ISNULL(lkp_PLAN_SALES_AMT_USD),
          999999999.9999,
          lkp_PLAN_SALES_AMT_USD
        )
        OR IFF(
          ISNULL(PLAN_MARGIN_AMT_USD),
          999999999.9999,
          PLAN_MARGIN_AMT_USD
        ) <> IFF(
          ISNULL(lkp_PLAN_MARGIN_AMT_USD),
          999999999.9999,
          lkp_PLAN_MARGIN_AMT_USD
        )
        OR IFF(
          ISNULL(PLAN_ORDER_CNT),
          999999999.9999,
          PLAN_ORDER_CNT
        ) <> IFF(
          ISNULL(lkp_PLAN_ORDER_CNT),
          999999999.9999,
          lkp_PLAN_ORDER_CNT
        )
      ),
      2,
      0
    )
  ) AS o_Flag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_TARGET_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_Before_Filter_6")

# COMMAND ----------
# DBTITLE 1, FIL_UNCHANGED_RECORDS_7


query_7 = f"""SELECT
  FISCAL_YR AS FISCAL_YR,
  FISCAL_WK AS FISCAL_WK,
  DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  FORECAST_SALES_AMT_USD AS PLAN_SALES_AMT_USD,
  FORECAST_MARGIN_AMT_USD AS PLAN_MARGIN_AMT_USD,
  FORECAST_ORDER_CNT AS PLAN_ORDER_CNT,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  o_Flag AS o_Flag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Before_Filter_6
WHERE
  o_Flag = 1
  OR o_Flag = 2"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("FIL_UNCHANGED_RECORDS_7")

# COMMAND ----------
# DBTITLE 1, EXP_VALID_FLAG_8


query_8 = f"""SELECT
  FISCAL_YR AS FISCAL_YR,
  FISCAL_WK AS FISCAL_WK,
  DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  PLAN_SALES_AMT_USD AS PLAN_SALES_AMT_USD,
  PLAN_MARGIN_AMT_USD AS PLAN_MARGIN_AMT_USD,
  PLAN_ORDER_CNT AS PLAN_ORDER_CNT,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS LOAD_TSTMP_exp,
  o_Flag AS o_Flag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_UNCHANGED_RECORDS_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("EXP_VALID_FLAG_8")

# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD_9


query_9 = f"""SELECT
  FISCAL_YR AS FISCAL_YR,
  FISCAL_WK AS FISCAL_WK,
  DIGITAL_CHANNEL AS DIGITAL_CHANNEL,
  PLAN_SALES_AMT_USD AS PLAN_SALES_AMT_USD,
  PLAN_MARGIN_AMT_USD AS PLAN_MARGIN_AMT_USD,
  PLAN_ORDER_CNT AS PLAN_ORDER_CNT,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP_exp AS LOAD_TSTMP_exp,
  o_Flag AS o_Flag,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(o_Flag, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_VALID_FLAG_8"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("UPD_INS_UPD_9")

# COMMAND ----------
# DBTITLE 1, DIGITAL_PLAN_WEEK


spark.sql("""MERGE INTO DIGITAL_PLAN_WEEK AS TARGET
USING
  UPD_INS_UPD_9 AS SOURCE ON TARGET.DIGITAL_CHANNEL = SOURCE.DIGITAL_CHANNEL
  AND TARGET.FISCAL_YR = SOURCE.FISCAL_YR
  AND TARGET.FISCAL_WK = SOURCE.FISCAL_WK
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.FISCAL_YR = SOURCE.FISCAL_YR,
  TARGET.FISCAL_WK = SOURCE.FISCAL_WK,
  TARGET.DIGITAL_CHANNEL = SOURCE.DIGITAL_CHANNEL,
  TARGET.PLAN_SALES_AMT_USD = SOURCE.PLAN_SALES_AMT_USD,
  TARGET.PLAN_MARGIN_AMT_USD = SOURCE.PLAN_MARGIN_AMT_USD,
  TARGET.PLAN_ORDER_CNT = SOURCE.PLAN_ORDER_CNT,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PLAN_SALES_AMT_USD = SOURCE.PLAN_SALES_AMT_USD
  AND TARGET.PLAN_MARGIN_AMT_USD = SOURCE.PLAN_MARGIN_AMT_USD
  AND TARGET.PLAN_ORDER_CNT = SOURCE.PLAN_ORDER_CNT
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.FISCAL_YR,
    TARGET.FISCAL_WK,
    TARGET.DIGITAL_CHANNEL,
    TARGET.PLAN_SALES_AMT_USD,
    TARGET.PLAN_MARGIN_AMT_USD,
    TARGET.PLAN_ORDER_CNT,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.FISCAL_YR,
    SOURCE.FISCAL_WK,
    SOURCE.DIGITAL_CHANNEL,
    SOURCE.PLAN_SALES_AMT_USD,
    SOURCE.PLAN_MARGIN_AMT_USD,
    SOURCE.PLAN_ORDER_CNT,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP_exp
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Digital_Plan_Week")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Digital_Plan_Week", mainWorkflowId, parentName)
