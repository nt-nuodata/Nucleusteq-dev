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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_Eagle_Status")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Site_Eagle_Status", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UDH_SITE_EAGLE_STATUS_0


query_0 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  EAGLE_LEVEL AS EAGLE_LEVEL,
  LOAD_DT AS LOAD_DT
FROM
  UDH_SITE_EAGLE_STATUS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_UDH_SITE_EAGLE_STATUS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1


query_1 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  EAGLE_LEVEL AS EAGLE_LEVEL,
  LOAD_DT AS LOAD_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_UDH_SITE_EAGLE_STATUS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1")

# COMMAND ----------
# DBTITLE 1, LKP_SITE_PROFILE_2


query_2 = f"""SELECT
  SP.LOCATION_ID AS LOCATION_ID,
  SP.STORE_NBR AS STORE_NBR,
  SStUSES1.STORE_NBR AS STORE_NBR1,
  SStUSES1.EAGLE_LEVEL AS EAGLE_LEVEL,
  SStUSES1.LOAD_DT AS LOAD_DT,
  SStUSES1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_SITE_EAGLE_STATUS_1 SStUSES1
  LEFT JOIN SITE_PROFILE SP ON SP.STORE_NBR = SStUSES1.STORE_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKP_SITE_PROFILE_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SITE_EAGLE_STATUS1_3


query_3 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  EAGLE_LEVEL AS EAGLE_LEVEL,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  SITE_EAGLE_STATUS"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_SITE_EAGLE_STATUS1_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SITE_EAGLE_STATUS_4


query_4 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  EAGLE_LEVEL AS EAGLE_LEVEL,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SITE_EAGLE_STATUS1_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_Shortcut_to_SITE_EAGLE_STATUS_4")

# COMMAND ----------
# DBTITLE 1, JNR_LEFTOUTERJOIN_5


query_5 = f"""SELECT
  MASTER.LOCATION_ID AS NEW_LOCATION_ID,
  MASTER.EAGLE_LEVEL AS NEW_EAGLE_LEVEL,
  MASTER.LOAD_DT AS NEW_LOAD_DT,
  DETAIL.LOCATION_ID AS OLD_LOCATION_ID,
  DETAIL.EAGLE_LEVEL AS OLD_EAGLE_LEVEL,
  DETAIL.UPDATE_TSTMP AS UPDATE_TSTMP,
  DETAIL.LOAD_TSTMP AS LOAD_TSTMP,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_SITE_PROFILE_2 MASTER
  LEFT JOIN SQ_Shortcut_to_SITE_EAGLE_STATUS_4 DETAIL ON MASTER.LOCATION_ID = DETAIL.LOCATION_ID"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("JNR_LEFTOUTERJOIN_5")

# COMMAND ----------
# DBTITLE 1, EXP_STRATEGY_6


query_6 = f"""SELECT
  NEW_LOCATION_ID AS NEW_LOCATION_ID,
  NEW_EAGLE_LEVEL AS NEW_EAGLE_LEVEL,
  IFF(
    ISNULL(OLD_LOCATION_ID),
    'INSERT',
    IFF(
      NEW_EAGLE_LEVEL <> OLD_EAGLE_LEVEL,
      'UPDATE',
      'REJECT'
    )
  ) AS v_STRATEGY,
  IFF(
    ISNULL(OLD_LOCATION_ID),
    'INSERT',
    IFF(
      NEW_EAGLE_LEVEL <> OLD_EAGLE_LEVEL,
      'UPDATE',
      'REJECT'
    )
  ) AS STRATEGY,
  now() AS UPDATE_TSTMP,
  IFF(
    IFF(
      ISNULL(OLD_LOCATION_ID),
      'INSERT',
      IFF(
        NEW_EAGLE_LEVEL <> OLD_EAGLE_LEVEL,
        'UPDATE',
        'REJECT'
      )
    ) = 'INSERT',
    now(),
    LOAD_TSTMP
  ) AS INSERT_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_LEFTOUTERJOIN_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_STRATEGY_6")

# COMMAND ----------
# DBTITLE 1, FILT_EXISTING_RECORDS_7


query_7 = f"""SELECT
  NEW_LOCATION_ID AS NEW_LOCATION_ID,
  NEW_EAGLE_LEVEL AS NEW_EAGLE_LEVEL,
  STRATEGY AS STRATEGY,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  INSERT_TSTMP AS INSERT_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_STRATEGY_6
WHERE
  STRATEGY <> 'REJECT'"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("FILT_EXISTING_RECORDS_7")

# COMMAND ----------
# DBTITLE 1, UPDTRANS_8


query_8 = f"""SELECT
  NEW_LOCATION_ID AS NEW_LOCATION_ID,
  NEW_EAGLE_LEVEL AS NEW_EAGLE_LEVEL,
  STRATEGY AS STRATEGY,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  INSERT_TSTMP AS INSERT_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    STRATEGY = 'INSERT',
    'DD_INSERT',
    IFF(STRATEGY = 'UPDATE', 'DD_UPDATE', 'DD_REJECT')
  ) AS UPDATE_STRATEGY_FLAG
FROM
  FILT_EXISTING_RECORDS_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("UPDTRANS_8")

# COMMAND ----------
# DBTITLE 1, SITE_EAGLE_STATUS


spark.sql("""MERGE INTO SITE_EAGLE_STATUS AS TARGET
USING
  UPDTRANS_8 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.NEW_LOCATION_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.LOCATION_ID = SOURCE.NEW_LOCATION_ID,
  TARGET.EAGLE_LEVEL = SOURCE.NEW_EAGLE_LEVEL,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.INSERT_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.EAGLE_LEVEL = SOURCE.NEW_EAGLE_LEVEL
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.INSERT_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.LOCATION_ID,
    TARGET.EAGLE_LEVEL,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.NEW_LOCATION_ID,
    SOURCE.NEW_EAGLE_LEVEL,
    SOURCE.UPDATE_TSTMP,
    SOURCE.INSERT_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Site_Eagle_Status")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Site_Eagle_Status", mainWorkflowId, parentName)
