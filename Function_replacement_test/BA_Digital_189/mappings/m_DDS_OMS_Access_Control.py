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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Access_Control")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Access_Control", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ACCESS_CONTROL_0


query_0 = f"""SELECT
  OMS_ACCESS_CONTROL_ID AS OMS_ACCESS_CONTROL_ID,
  OMS_UCL_USER_ID AS OMS_UCL_USER_ID,
  OMS_ROLE_ID AS OMS_ROLE_ID,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_ACCESS_CONTROL"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_OMS_ACCESS_CONTROL_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ACCESS_CONTROL_1


query_1 = f"""SELECT
  OMS_ACCESS_CONTROL_ID AS OMS_ACCESS_CONTROL_ID,
  OMS_UCL_USER_ID AS OMS_UCL_USER_ID,
  OMS_ROLE_ID AS OMS_ROLE_ID,
  OMS_CREATED_TSTMP AS OMS_CREATED_TSTMP,
  OMS_LAST_UPDATED_TSTMP AS OMS_LAST_UPDATED_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ACCESS_CONTROL_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_OMS_ACCESS_CONTROL_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_OMS_ACCESS_CONTROL_PRE_2


query_2 = f"""SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  OMS_ACCESS_CONTROL_PRE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_OMS_ACCESS_CONTROL_PRE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_OMS_ACCESS_CONTROL_PRE_3


query_3 = f"""SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_OMS_ACCESS_CONTROL_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_OMS_ACCESS_CONTROL_PRE_3")

# COMMAND ----------
# DBTITLE 1, JNR_OMS_ACCESS_CONTROL_4


query_4 = f"""SELECT
  DETAIL.ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  DETAIL.UCL_USER_ID AS UCL_USER_ID,
  DETAIL.ROLE_ID AS ROLE_ID,
  DETAIL.CREATED_DTTM AS CREATED_DTTM,
  DETAIL.LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  MASTER.OMS_ACCESS_CONTROL_ID AS lkp_OMS_ACCESS_CONTROL_ID,
  MASTER.OMS_UCL_USER_ID AS lkp_OMS_UCL_USER_ID,
  MASTER.OMS_ROLE_ID AS lkp_OMS_ROLE_ID,
  MASTER.OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  MASTER.OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  MASTER.LOAD_TSTMP AS lkp_LOAD_TSTMP,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_OMS_ACCESS_CONTROL_1 MASTER
  RIGHT JOIN SQ_Shortcut_to_OMS_ACCESS_CONTROL_PRE_3 DETAIL ON MASTER.OMS_ACCESS_CONTROL_ID = DETAIL.ACCESS_CONTROL_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("JNR_OMS_ACCESS_CONTROL_4")

# COMMAND ----------
# DBTITLE 1, FTR_UNCHANGED_RECORDS_5


query_5 = f"""SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  lkp_OMS_ACCESS_CONTROL_ID AS lkp_OMS_ACCESS_CONTROL_ID,
  lkp_OMS_UCL_USER_ID AS lkp_OMS_UCL_USER_ID,
  lkp_OMS_ROLE_ID AS lkp_OMS_ROLE_ID,
  lkp_OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  lkp_OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_OMS_ACCESS_CONTROL_4
WHERE
  ISNULL(lkp_OMS_ACCESS_CONTROL_ID)
  OR (
    NOT ISNULL(lkp_OMS_ACCESS_CONTROL_ID)
    AND (
      IFF(
        ISNULL(UCL_USER_ID),
        TO_INTEGER(999999999),
        UCL_USER_ID
      ) <> IFF(
        ISNULL(lkp_OMS_UCL_USER_ID),
        TO_INTEGER(999999999),
        lkp_OMS_UCL_USER_ID
      )
      OR IFF(ISNULL(ROLE_ID), TO_INTEGER(999999999), ROLE_ID) <> IFF(
        ISNULL(lkp_OMS_ROLE_ID),
        TO_INTEGER(999999999),
        lkp_OMS_ROLE_ID
      )
      OR IFF(
        ISNULL(CREATED_DTTM),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        CREATED_DTTM
      ) <> IFF(
        ISNULL(lkp_OMS_CREATED_TSTMP),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        lkp_OMS_CREATED_TSTMP
      )
      OR IFF(
        ISNULL(LAST_UPDATED_DTTM),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        LAST_UPDATED_DTTM
      ) <> IFF(
        ISNULL(lkp_OMS_LAST_UPDATED_TSTMP),
        To_DATE('12-31-9999', 'MM-DD-YYYY'),
        lkp_OMS_LAST_UPDATED_TSTMP
      )
    )
  )"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FTR_UNCHANGED_RECORDS_5")

# COMMAND ----------
# DBTITLE 1, EXP_VALID_FLAGS_6


query_6 = f"""SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  lkp_OMS_ACCESS_CONTROL_ID AS lkp_OMS_ACCESS_CONTROL_ID,
  lkp_OMS_UCL_USER_ID AS lkp_OMS_UCL_USER_ID,
  lkp_OMS_ROLE_ID AS lkp_OMS_ROLE_ID,
  lkp_OMS_CREATED_TSTMP AS lkp_OMS_CREATED_TSTMP,
  lkp_OMS_LAST_UPDATED_TSTMP AS lkp_OMS_LAST_UPDATED_TSTMP,
  lkp_LOAD_TSTMP AS lkp_LOAD_TSTMP,
  now() AS UPDATE_TSTMP_exp,
  IFF(ISNULL(lkp_LOAD_TSTMP), now(), lkp_LOAD_TSTMP) AS LOAD_TSTMP_exp,
  IFF(ISNULL(lkp_OMS_ACCESS_CONTROL_ID), 1, 2) AS VALID_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FTR_UNCHANGED_RECORDS_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_VALID_FLAGS_6")

# COMMAND ----------
# DBTITLE 1, UPD_INS_UPD_7


query_7 = f"""SELECT
  ACCESS_CONTROL_ID AS ACCESS_CONTROL_ID,
  UCL_USER_ID AS UCL_USER_ID,
  ROLE_ID AS ROLE_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  UPDATE_TSTMP_exp AS UPDATE_TSTMP_exp,
  LOAD_TSTMP_exp AS LOAD_TSTMP_exp,
  VALID_FLAG AS VALID_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(VALID_FLAG, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_VALID_FLAGS_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_INS_UPD_7")

# COMMAND ----------
# DBTITLE 1, OMS_ACCESS_CONTROL


spark.sql("""MERGE INTO OMS_ACCESS_CONTROL AS TARGET
USING
  UPD_INS_UPD_7 AS SOURCE ON TARGET.OMS_ACCESS_CONTROL_ID = SOURCE.ACCESS_CONTROL_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.OMS_ACCESS_CONTROL_ID = SOURCE.ACCESS_CONTROL_ID,
  TARGET.OMS_UCL_USER_ID = SOURCE.UCL_USER_ID,
  TARGET.OMS_ROLE_ID = SOURCE.ROLE_ID,
  TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM,
  TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP_exp,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.OMS_UCL_USER_ID = SOURCE.UCL_USER_ID
  AND TARGET.OMS_ROLE_ID = SOURCE.ROLE_ID
  AND TARGET.OMS_CREATED_TSTMP = SOURCE.CREATED_DTTM
  AND TARGET.OMS_LAST_UPDATED_TSTMP = SOURCE.LAST_UPDATED_DTTM
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP_exp
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP_exp THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.OMS_ACCESS_CONTROL_ID,
    TARGET.OMS_UCL_USER_ID,
    TARGET.OMS_ROLE_ID,
    TARGET.OMS_CREATED_TSTMP,
    TARGET.OMS_LAST_UPDATED_TSTMP,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.ACCESS_CONTROL_ID,
    SOURCE.UCL_USER_ID,
    SOURCE.ROLE_ID,
    SOURCE.CREATED_DTTM,
    SOURCE.LAST_UPDATED_DTTM,
    SOURCE.UPDATE_TSTMP_exp,
    SOURCE.LOAD_TSTMP_exp
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Access_Control")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Access_Control", mainWorkflowId, parentName)