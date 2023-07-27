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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pricing_reason")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_pricing_reason", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_TWBNT_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  SPRAS AS SPRAS,
  PV_GRUND AS PV_GRUND,
  PV_GRTXT AS PV_GRTXT
FROM
  TWBNT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_TWBNT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TWBNT_1


query_1 = f"""SELECT
  TRIM(PV_GRUND) AS PV_GRUND,
  PV_GRTXT AS PV_GRTXT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  SAPPR3.Shortcut_to_TWBNT_0
WHERE
  MANDT = '100'
  AND SPRAS = 'E'"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_TWBNT_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PRICING_REASON_2


query_2 = f"""SELECT
  PRICING_REASON_CD AS PRICING_REASON_CD,
  PRICING_REASON_DESC AS PRICING_REASON_DESC,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  PRICING_REASON"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_PRICING_REASON_2")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_PRICING_REASON_3


query_3 = f"""SELECT
  PRICING_REASON_CD AS PRICING_REASON_CD,
  PRICING_REASON_DESC AS PRICING_REASON_DESC,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PRICING_REASON_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("ASQ_Shortcut_to_PRICING_REASON_3")

# COMMAND ----------
# DBTITLE 1, JNR_PRICING_REASON_4


query_4 = f"""SELECT
  DETAIL.PV_GRUND AS PRICING_REASON_CD_sap,
  DETAIL.PV_GRTXT AS PRICING_REASON_DESC_sap,
  MASTER.PRICING_REASON_CD AS PRICING_REASON_CD_edw,
  MASTER.PRICING_REASON_DESC AS PRICING_REASON_DESC_edw,
  MASTER.LOAD_DT AS LOAD_DT_edw,
  nvl(
    MASTER.Monotonically_Increasing_Id,
    DETAIL.Monotonically_Increasing_Id
  ) AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_to_PRICING_REASON_3 MASTER
  FULL OUTER JOIN SQ_Shortcut_to_TWBNT_1 DETAIL ON MASTER.PRICING_REASON_CD = DETAIL.PV_GRUND"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_PRICING_REASON_4")

# COMMAND ----------
# DBTITLE 1, EXP_PRICING_REASON_5


query_5 = f"""SELECT
  IFF(
    IsNull(PRICING_REASON_CD_sap),
    PRICING_REASON_CD_edw,
    PRICING_REASON_CD_sap
  ) AS PRICING_REASON_CD_out,
  PRICING_REASON_DESC_sap AS PRICING_REASON_DESC_sap,
  TRUNC(now()) AS UPDATE_DT,
  IFF(IsNull(LOAD_DT_edw), TRUNC(now()), LOAD_DT_edw) AS LOAD_DT_out,
  IFF(
    IsNull(PRICING_REASON_CD_sap),
    'DD_DELETE',
    IFF(
      IsNull(PRICING_REASON_CD_edw),
      'DD_INSERT',
      IFF(
        IFF(
          IsNull(PRICING_REASON_DESC_sap),
          'NULL',
          PRICING_REASON_DESC_sap
        ) <> IFF(
          IsNull(PRICING_REASON_DESC_edw),
          'NULL',
          PRICING_REASON_DESC_edw
        ),
        'DD_UPDATE',
        'DD_REJECT'
      )
    )
  ) AS UPDATE_STRATEGY,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_PRICING_REASON_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("EXP_PRICING_REASON_5")

# COMMAND ----------
# DBTITLE 1, FTR_PRICING_REASON_6


query_6 = f"""SELECT
  PRICING_REASON_CD_out AS PRICING_REASON_CD,
  PRICING_REASON_DESC_sap AS PRICING_REASON_DESC,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT_out AS LOAD_DT,
  UPDATE_STRATEGY AS UPDATE_STRATEGY,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_PRICING_REASON_5
WHERE
  UPDATE_STRATEGY <> 'DD_REJECT'"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("FTR_PRICING_REASON_6")

# COMMAND ----------
# DBTITLE 1, UPD_PRICING_REASON_7


query_7 = f"""SELECT
  PRICING_REASON_CD AS PRICING_REASON_CD,
  PRICING_REASON_DESC AS PRICING_REASON_DESC,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT,
  UPDATE_STRATEGY AS UPDATE_STRATEGY,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  UPDATE_STRATEGY AS UPDATE_STRATEGY_FLAG
FROM
  FTR_PRICING_REASON_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_PRICING_REASON_7")

# COMMAND ----------
# DBTITLE 1, PRICING_REASON


spark.sql("""MERGE INTO PRICING_REASON AS TARGET
USING
  UPD_PRICING_REASON_7 AS SOURCE ON TARGET.PRICING_REASON_CD = SOURCE.PRICING_REASON_CD
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PRICING_REASON_CD = SOURCE.PRICING_REASON_CD,
  TARGET.PRICING_REASON_DESC = SOURCE.PRICING_REASON_DESC,
  TARGET.UPDATE_DT = SOURCE.UPDATE_DT,
  TARGET.LOAD_DT = SOURCE.LOAD_DT
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PRICING_REASON_DESC = SOURCE.PRICING_REASON_DESC
  AND TARGET.UPDATE_DT = SOURCE.UPDATE_DT
  AND TARGET.LOAD_DT = SOURCE.LOAD_DT THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PRICING_REASON_CD,
    TARGET.PRICING_REASON_DESC,
    TARGET.UPDATE_DT,
    TARGET.LOAD_DT
  )
VALUES
  (
    SOURCE.PRICING_REASON_CD,
    SOURCE.PRICING_REASON_DESC,
    SOURCE.UPDATE_DT,
    SOURCE.LOAD_DT
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_pricing_reason")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_pricing_reason", mainWorkflowId, parentName)
