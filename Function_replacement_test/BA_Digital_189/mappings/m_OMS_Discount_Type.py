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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_Discount_Type")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_OMS_Discount_Type", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_A_DISCOUNT_TYPE_0


query_0 = f"""SELECT
  OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  OMS_DISCOUNT_TYPE_NAME AS OMS_DISCOUNT_TYPE_NAME,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_A_DISCOUNT_TYPE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_A_DISCOUNT_TYPE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_1


query_1 = f"""SELECT
  OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  OMS_DISCOUNT_TYPE_NAME AS OMS_DISCOUNT_TYPE_NAME,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_A_DISCOUNT_TYPE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_1")

# COMMAND ----------
# DBTITLE 1, EXP_DATA_TYPES_2


query_2 = f"""SELECT
  TO_INTEGER(OMS_DISCOUNT_TYPE_ID) AS o_DISCOUNT_TYPE_ID1,
  OMS_DISCOUNT_TYPE_NAME AS OMS_DISCOUNT_TYPE_NAME,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_A_DISCOUNT_TYPE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_DATA_TYPES_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_DISCOUNT_TYPE_3


query_3 = f"""SELECT
  OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  OMS_DISCOUNT_TYPE_DESC AS OMS_DISCOUNT_TYPE_DESC,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_DISCOUNT_TYPE"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_OMS_DISCOUNT_TYPE_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_DISCOUNT_TYPE_4


query_4 = f"""SELECT
  OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  OMS_DISCOUNT_TYPE_DESC AS OMS_DISCOUNT_TYPE_DESC,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_DISCOUNT_TYPE_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_Shortcut_to_OMS_DISCOUNT_TYPE_4")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_EDW_5


query_5 = f"""SELECT
  MASTER.o_DISCOUNT_TYPE_ID1 AS DISCOUNT_TYPE_ID,
  MASTER.OMS_DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME,
  DETAIL.OMS_DISCOUNT_TYPE_ID AS OMS_DISCOUNT_TYPE_ID,
  DETAIL.OMS_DISCOUNT_TYPE_DESC AS OMS_DISCOUNT_TYPE_DESC,
  DETAIL.UPDATE_TSTMP AS UPDATE_TSTMP,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_DATA_TYPES_2 MASTER
  LEFT JOIN SQ_Shortcut_to_OMS_DISCOUNT_TYPE_4 DETAIL ON MASTER.o_DISCOUNT_TYPE_ID1 = DETAIL.OMS_DISCOUNT_TYPE_ID"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("JNR_OMS_EDW_5")

# COMMAND ----------
# DBTITLE 1, EXP_FLAGS_6


query_6 = f"""SELECT
  DISCOUNT_TYPE_ID AS src_DISCOUNT_TYPE_ID,
  DISCOUNT_TYPE_NAME AS src_DISCOUNT_TYPE_NAME,
  IFF(
    ISNULL(OMS_DISCOUNT_TYPE_ID),
    DISCOUNT_TYPE_ID,
    OMS_DISCOUNT_TYPE_ID
  ) AS DISCOUNT_TYPE_ID1,
  IFF(
    ISNULL(OMS_DISCOUNT_TYPE_ID),
    'INSERT',
    IFF(
      NOT ISNULL(OMS_DISCOUNT_TYPE_ID)
      AND LTRIM(
        RTRIM(
          IFF(
            ISNULL(DISCOUNT_TYPE_NAME),
            ' ',
            DISCOUNT_TYPE_NAME
          )
        )
      ) != LTRIM(
        RTRIM(
          IFF(
            ISNULL(OMS_DISCOUNT_TYPE_DESC),
            ' ',
            OMS_DISCOUNT_TYPE_DESC
          )
        )
      ),
      'UPDATE',
      'REJECT'
    )
  ) AS LOAD_FLAG,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(OMS_DISCOUNT_TYPE_ID), now(), LOAD_TSTMP) AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_EDW_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_FLAGS_6")

# COMMAND ----------
# DBTITLE 1, FIL_FLAGS_7


query_7 = f"""SELECT
  DISCOUNT_TYPE_ID1 AS DISCOUNT_TYPE_ID1,
  src_DISCOUNT_TYPE_NAME AS DISCOUNT_TYPE_NAME11,
  LOAD_FLAG AS LOAD_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_FLAGS_6
WHERE
  IN(LOAD_FLAG, 'INSERT', 'UPDATE')"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("FIL_FLAGS_7")

# COMMAND ----------
# DBTITLE 1, UPD_FLAGS_8


query_8 = f"""SELECT
  DISCOUNT_TYPE_ID1 AS DISCOUNT_TYPE_ID1,
  DISCOUNT_TYPE_NAME11 AS DISCOUNT_TYPE_NAME11,
  LOAD_FLAG AS LOAD_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    LOAD_FLAG = 'INSERT',
    'DD_INSERT',
    IFF(LOAD_FLAG = 'UPDATE', 'DD_UPDATE', 'DD_REJECT')
  ) AS UPDATE_STRATEGY_FLAG
FROM
  FIL_FLAGS_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("UPD_FLAGS_8")

# COMMAND ----------
# DBTITLE 1, OMS_DISCOUNT_TYPE


spark.sql("""MERGE INTO OMS_DISCOUNT_TYPE AS TARGET
USING
  UPD_FLAGS_8 AS SOURCE ON TARGET.OMS_DISCOUNT_TYPE_ID = SOURCE.DISCOUNT_TYPE_ID1
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_DISCOUNT_TYPE_ID = SOURCE.DISCOUNT_TYPE_ID1,
  TARGET.OMS_DISCOUNT_TYPE_DESC = SOURCE.DISCOUNT_TYPE_NAME11,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_DISCOUNT_TYPE_DESC = SOURCE.DISCOUNT_TYPE_NAME11
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_DISCOUNT_TYPE_ID,
    TARGET.OMS_DISCOUNT_TYPE_DESC,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.DISCOUNT_TYPE_ID1,
    SOURCE.DISCOUNT_TYPE_NAME11,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_OMS_Discount_Type")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_OMS_Discount_Type", mainWorkflowId, parentName)
