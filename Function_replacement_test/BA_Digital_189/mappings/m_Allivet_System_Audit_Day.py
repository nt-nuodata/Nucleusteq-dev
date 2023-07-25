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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Allivet_System_Audit_Day")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Allivet_System_Audit_Day", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_0


query_0 = f"""SELECT
  ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
  ALLIVET_ORDER_CNTR AS ALLIVET_ORDER_CNTR,
  ALLIVET_ORDER_LN_NBR AS ALLIVET_ORDER_LN_NBR,
  OPERATION AS OPERATION,
  ALLIVET_CUSTOMER_NBR AS ALLIVET_CUSTOMER_NBR,
  EXTRA_INFO AS EXTRA_INFO,
  ORDER_CHG_DT AS ORDER_CHG_DT,
  FROM_ITEM_CD AS FROM_ITEM_CD,
  TO_ITEM_CD AS TO_ITEM_CD,
  PRESCRIPTION_ID AS PRESCRIPTION_ID,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  ALLIVET_SYSTEM_AUDIT_DAY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_1


query_1 = f"""SELECT
  ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
  ALLIVET_ORDER_CNTR AS ALLIVET_ORDER_CNTR,
  ALLIVET_ORDER_LN_NBR AS ALLIVET_ORDER_LN_NBR,
  OPERATION AS OPERATION,
  ALLIVET_CUSTOMER_NBR AS ALLIVET_CUSTOMER_NBR,
  EXTRA_INFO AS EXTRA_INFO,
  ORDER_CHG_DT AS ORDER_CHG_DT,
  FROM_ITEM_CD AS FROM_ITEM_CD,
  TO_ITEM_CD AS TO_ITEM_CD,
  PRESCRIPTION_ID AS PRESCRIPTION_ID,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ALLIVET_DAILY_SYSTEM_AUDIT_PRE_2


query_2 = f"""SELECT
  ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
  ALLIVET_ORDER_COUNTER AS ALLIVET_ORDER_COUNTER,
  ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
  OPERATION AS OPERATION,
  CUSTOMER_CODE AS CUSTOMER_CODE,
  EXTRA_INFO AS EXTRA_INFO,
  CHANGE_DATE AS CHANGE_DATE,
  FROM_ITEM_CODE AS FROM_ITEM_CODE,
  TO_ITEM_CODE AS TO_ITEM_CODE,
  PRESCRIPTION_ID AS PRESCRIPTION_ID,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  ALLIVET_DAILY_SYSTEM_AUDIT_PRE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_ALLIVET_DAILY_SYSTEM_AUDIT_PRE_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_DAILY_SYSTEM_AUDIT_PRE_3


query_3 = f"""SELECT
  ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
  ALLIVET_ORDER_COUNTER AS ALLIVET_ORDER_COUNTER,
  ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
  OPERATION AS OPERATION,
  CUSTOMER_CODE AS CUSTOMER_CODE,
  EXTRA_INFO AS EXTRA_INFO,
  CHANGE_DATE AS CHANGE_DATE,
  FROM_ITEM_CODE AS FROM_ITEM_CODE,
  TO_ITEM_CODE AS TO_ITEM_CODE,
  PRESCRIPTION_ID AS PRESCRIPTION_ID,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_ALLIVET_DAILY_SYSTEM_AUDIT_PRE_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_DAILY_SYSTEM_AUDIT_PRE_3")

# COMMAND ----------
# DBTITLE 1, Jnr_Audit_Target_4


query_4 = f"""SELECT
  DETAIL.ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
  DETAIL.ALLIVET_ORDER_COUNTER AS ALLIVET_ORDER_COUNTER,
  DETAIL.ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
  DETAIL.OPERATION AS OPERATION,
  DETAIL.CUSTOMER_CODE AS CUSTOMER_CODE,
  DETAIL.EXTRA_INFO AS EXTRA_INFO,
  DETAIL.CHANGE_DATE AS CHANGE_DATE,
  DETAIL.FROM_ITEM_CODE AS FROM_ITEM_CODE,
  DETAIL.TO_ITEM_CODE AS TO_ITEM_CODE,
  DETAIL.PRESCRIPTION_ID AS PRESCRIPTION_ID,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  MASTER.ALLIVET_ORDER_NBR AS Lkp_ALLIVET_ORDER_NBR,
  MASTER.ALLIVET_ORDER_CNTR AS Lkp_ALLIVET_ORDER_CNTR,
  MASTER.ALLIVET_ORDER_LN_NBR AS Lkp_ALLIVET_ORDER_LN_NBR,
  MASTER.OPERATION AS LKP_OPERATION,
  MASTER.ALLIVET_CUSTOMER_NBR AS LKP_ALLIVET_CUSTOMER_NBR,
  MASTER.EXTRA_INFO AS LKP_EXTRA_INFO,
  MASTER.ORDER_CHG_DT AS LKP_ORDER_CHG_DT,
  MASTER.FROM_ITEM_CD AS LKP_FROM_ITEM_CD,
  MASTER.TO_ITEM_CD AS LKP_TO_ITEM_CD,
  MASTER.PRESCRIPTION_ID AS LKP_PRESCRIPTION_ID,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ALLIVET_SYSTEM_AUDIT_DAY_1 MASTER
  RIGHT JOIN SQ_Shortcut_to_ALLIVET_DAILY_SYSTEM_AUDIT_PRE_3 DETAIL ON MASTER.ALLIVET_ORDER_NBR = DETAIL.ALLIVET_ORDER_CODE
  AND MASTER.ALLIVET_ORDER_CNTR = DETAIL.ALLIVET_ORDER_COUNTER"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Jnr_Audit_Target_4")

# COMMAND ----------
# DBTITLE 1, Exp_Update_Flag_5


query_5 = f"""SELECT
  ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
  ALLIVET_ORDER_COUNTER AS ALLIVET_ORDER_COUNTER,
  ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
  OPERATION AS OPERATION,
  CUSTOMER_CODE AS CUSTOMER_CODE,
  EXTRA_INFO AS EXTRA_INFO,
  CHANGE_DATE AS CHANGE_DATE,
  FROM_ITEM_CODE AS FROM_ITEM_CODE,
  TO_ITEM_CODE AS TO_ITEM_CODE,
  PRESCRIPTION_ID AS PRESCRIPTION_ID,
  LOAD_TSTMP AS LOAD_TSTMP,
  now() AS UPDATE_TSTMP,
  IFF(
    (
      ISNULL(Lkp_ALLIVET_ORDER_NBR)
      OR ISNULL(Lkp_ALLIVET_ORDER_CNTR)
    ),
    1,
    IFF(
      NOT(
        ISNULL(Lkp_ALLIVET_ORDER_NBR)
        OR ISNULL(Lkp_ALLIVET_ORDER_CNTR)
      )
      AND (
        IFF(
          ISNULL(ALLIVET_ORDER_LINE_NUMBER),
          -1,
          ALLIVET_ORDER_LINE_NUMBER
        ) <> IFF(
          ISNULL(Lkp_ALLIVET_ORDER_LN_NBR),
          -1,
          Lkp_ALLIVET_ORDER_LN_NBR
        )
        OR IFF(ISNULL(OPERATION), '0', OPERATION) <> IFF(ISNULL(LKP_OPERATION), '0', LKP_OPERATION)
        OR IFF(ISNULL(CUSTOMER_CODE), '0', CUSTOMER_CODE) <> IFF(
          ISNULL(LKP_ALLIVET_CUSTOMER_NBR),
          '0',
          LKP_ALLIVET_CUSTOMER_NBR
        )
        OR IFF(ISNULL(EXTRA_INFO), '0', EXTRA_INFO) <> IFF(ISNULL(LKP_EXTRA_INFO), '0', LKP_EXTRA_INFO)
        OR IFF(
          ISNULL(CHANGE_DATE),
          TO_DATE('9999-12-31', 'YYYY-MM-DD'),
          CHANGE_DATE
        ) <> IFF(
          ISNULL(LKP_ORDER_CHG_DT),
          TO_DATE('9999-12-31', 'YYYY-MM-DD'),
          LKP_ORDER_CHG_DT
        )
        OR IFF(ISNULL(FROM_ITEM_CODE), '0', FROM_ITEM_CODE) <> IFF(ISNULL(LKP_FROM_ITEM_CD), '0', LKP_FROM_ITEM_CD)
        OR IFF(ISNULL(TO_ITEM_CODE), '0', TO_ITEM_CODE) <> IFF(ISNULL(LKP_TO_ITEM_CD), '0', LKP_TO_ITEM_CD)
        OR IFF(ISNULL(PRESCRIPTION_ID), -1, PRESCRIPTION_ID) <> IFF(
          ISNULL(LKP_PRESCRIPTION_ID),
          -1,
          LKP_PRESCRIPTION_ID
        )
      ),
      2,
      0
    )
  ) AS UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Jnr_Audit_Target_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Exp_Update_Flag_5")

# COMMAND ----------
# DBTITLE 1, Filter_Update_Insert_6


query_6 = f"""SELECT
  ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
  ALLIVET_ORDER_COUNTER AS ALLIVET_ORDER_COUNTER,
  ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
  OPERATION AS OPERATION,
  CUSTOMER_CODE AS CUSTOMER_CODE,
  EXTRA_INFO AS EXTRA_INFO,
  CHANGE_DATE AS CHANGE_DATE,
  FROM_ITEM_CODE AS FROM_ITEM_CODE,
  TO_ITEM_CODE AS TO_ITEM_CODE,
  PRESCRIPTION_ID AS PRESCRIPTION_ID,
  LOAD_TSTMP AS LOAD_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  UPDATE_FLAG AS UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_Update_Flag_5
WHERE
  IN(UPDATE_FLAG, 1, 2)"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Filter_Update_Insert_6")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_7


query_7 = f"""SELECT
  ALLIVET_ORDER_CODE AS ALLIVET_ORDER_CODE,
  ALLIVET_ORDER_COUNTER AS ALLIVET_ORDER_COUNTER,
  ALLIVET_ORDER_LINE_NUMBER AS ALLIVET_ORDER_LINE_NUMBER,
  OPERATION AS OPERATION,
  CUSTOMER_CODE AS CUSTOMER_CODE,
  EXTRA_INFO AS EXTRA_INFO,
  CHANGE_DATE AS CHANGE_DATE,
  FROM_ITEM_CODE AS FROM_ITEM_CODE,
  TO_ITEM_CODE AS TO_ITEM_CODE,
  PRESCRIPTION_ID AS PRESCRIPTION_ID,
  LOAD_TSTMP AS LOAD_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  UPDATE_FLAG AS UPDATE_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  DECODE(UPDATE_FLAG, 1, 'DD_INSERT', 2, 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  Filter_Update_Insert_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPDTRANS_7")

# COMMAND ----------
# DBTITLE 1, ALLIVET_SYSTEM_AUDIT_DAY


spark.sql("""MERGE INTO ALLIVET_SYSTEM_AUDIT_DAY AS TARGET
USING
  UPDTRANS_7 AS SOURCE ON TARGET.ALLIVET_ORDER_CNTR = SOURCE.ALLIVET_ORDER_COUNTER
  AND TARGET.ALLIVET_ORDER_NBR = SOURCE.ALLIVET_ORDER_CODE
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.ALLIVET_ORDER_NBR = SOURCE.ALLIVET_ORDER_CODE,
  TARGET.ALLIVET_ORDER_CNTR = SOURCE.ALLIVET_ORDER_COUNTER,
  TARGET.ALLIVET_ORDER_LN_NBR = SOURCE.ALLIVET_ORDER_LINE_NUMBER,
  TARGET.OPERATION = SOURCE.OPERATION,
  TARGET.ALLIVET_CUSTOMER_NBR = SOURCE.CUSTOMER_CODE,
  TARGET.EXTRA_INFO = SOURCE.EXTRA_INFO,
  TARGET.ORDER_CHG_DT = SOURCE.CHANGE_DATE,
  TARGET.FROM_ITEM_CD = SOURCE.FROM_ITEM_CODE,
  TARGET.TO_ITEM_CD = SOURCE.TO_ITEM_CODE,
  TARGET.PRESCRIPTION_ID = SOURCE.PRESCRIPTION_ID,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.ALLIVET_ORDER_LN_NBR = SOURCE.ALLIVET_ORDER_LINE_NUMBER
  AND TARGET.OPERATION = SOURCE.OPERATION
  AND TARGET.ALLIVET_CUSTOMER_NBR = SOURCE.CUSTOMER_CODE
  AND TARGET.EXTRA_INFO = SOURCE.EXTRA_INFO
  AND TARGET.ORDER_CHG_DT = SOURCE.CHANGE_DATE
  AND TARGET.FROM_ITEM_CD = SOURCE.FROM_ITEM_CODE
  AND TARGET.TO_ITEM_CD = SOURCE.TO_ITEM_CODE
  AND TARGET.PRESCRIPTION_ID = SOURCE.PRESCRIPTION_ID
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.ALLIVET_ORDER_NBR,
    TARGET.ALLIVET_ORDER_CNTR,
    TARGET.ALLIVET_ORDER_LN_NBR,
    TARGET.OPERATION,
    TARGET.ALLIVET_CUSTOMER_NBR,
    TARGET.EXTRA_INFO,
    TARGET.ORDER_CHG_DT,
    TARGET.FROM_ITEM_CD,
    TARGET.TO_ITEM_CD,
    TARGET.PRESCRIPTION_ID,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.ALLIVET_ORDER_CODE,
    SOURCE.ALLIVET_ORDER_COUNTER,
    SOURCE.ALLIVET_ORDER_LINE_NUMBER,
    SOURCE.OPERATION,
    SOURCE.CUSTOMER_CODE,
    SOURCE.EXTRA_INFO,
    SOURCE.CHANGE_DATE,
    SOURCE.FROM_ITEM_CODE,
    SOURCE.TO_ITEM_CODE,
    SOURCE.PRESCRIPTION_ID,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Allivet_System_Audit_Day")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Allivet_System_Audit_Day", mainWorkflowId, parentName)