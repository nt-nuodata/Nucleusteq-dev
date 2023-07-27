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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_USR_Store_Attributes")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_USR_Store_Attributes", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_USR_STORE_ATTRIBUTES_PRE_0


query_0 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  VET_TYPE_ID AS VET_TYPE_ID,
  VET_TYPE_DESC AS VET_TYPE_DESC,
  MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  ONP_DIST_FLAG AS ONP_DIST_FLAG,
  PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  HOTEL_TIER_ID AS HOTEL_TIER_ID,
  STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
  STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
  FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
  GROOMERY_FLAG AS GROOMERY_FLAG,
  STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  MARKET_LEADER_ID AS MARKET_LEADER_ID,
  MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  USR_STORE_ATTRIBUTES_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_USR_STORE_ATTRIBUTES_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_USR_STORE_ATTRIBUTES_PRE_1


query_1 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  VET_TYPE_ID AS VET_TYPE_ID,
  VET_TYPE_DESC AS VET_TYPE_DESC,
  MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  ONP_DIST_FLAG AS ONP_DIST_FLAG,
  PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  HOTEL_TIER_ID AS HOTEL_TIER_ID,
  STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
  STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
  FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
  GROOMERY_FLAG AS GROOMERY_FLAG,
  STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  LOAD_TSTMP AS LOAD_TSTMP,
  MARKET_LEADER_ID AS MARKET_LEADER_ID,
  MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_USR_STORE_ATTRIBUTES_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_USR_STORE_ATTRIBUTES_PRE_1")

# COMMAND ----------
# DBTITLE 1, EXP_ADDING_FILLER_ZEROS_2


query_2 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  VET_TYPE_ID AS VET_TYPE_ID,
  VET_TYPE_DESC AS VET_TYPE_DESC,
  MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  ONP_DIST_FLAG AS ONP_DIST_FLAG,
  PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  HOTEL_TIER_ID AS HOTEL_TIER_ID,
  STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
  STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
  FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
  0 AS SSDW_FLAG,
  0 AS DOG_WALK_FLAG,
  GROOMERY_FLAG AS GROOMERY_FLAG,
  STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  LOAD_TSTMP AS LOAD_TSTMP,
  MARKET_LEADER_ID AS MARKET_LEADER_ID,
  MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_USR_STORE_ATTRIBUTES_PRE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_ADDING_FILLER_ZEROS_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_USR_STORE_ATTRIBUTES_3


query_3 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  VET_TYPE_ID AS VET_TYPE_ID,
  MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  ONP_DIST_FLAG AS ONP_DIST_FLAG,
  PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  HOTEL_TIER_ID AS HOTEL_TIER_ID,
  STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  MARKET_LEADER_ID AS MARKET_LEADER_ID,
  MARKET_LEADER_DESC AS MARKET_LEADER_DESC
FROM
  USR_STORE_ATTRIBUTES"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_USR_STORE_ATTRIBUTES_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_USR_STORE_ATTRIBUTES_4


query_4 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  VET_TYPE_ID AS VET_TYPE_ID,
  VET_TYPE_DESC AS VET_TYPE_DESC,
  MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  ONP_DIST_FLAG AS ONP_DIST_FLAG,
  PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  HOTEL_TIER_ID AS HOTEL_TIER_ID,
  STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
  STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
  FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
  SSDW_FLAG AS SSDW_FLAG,
  DOG_WALK_FLAG AS DOG_WALK_FLAG,
  GROOMERY_FLAG AS GROOMERY_FLAG,
  STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  MARKET_LEADER_ID AS MARKET_LEADER_ID,
  MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_USR_STORE_ATTRIBUTES_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_Shortcut_to_USR_STORE_ATTRIBUTES_4")

# COMMAND ----------
# DBTITLE 1, JNR_UPD_STRATEGY_5


query_5 = f"""SELECT
  DETAIL.LOCATION_ID AS LOCATION_ID,
  DETAIL.STORE_NBR AS STORE_NBR,
  DETAIL.BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  DETAIL.VET_TYPE_ID AS VET_TYPE_ID1,
  DETAIL.VET_TYPE_DESC AS VET_TYPE_DESC1,
  DETAIL.MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  DETAIL.ONP_DIST_FLAG AS ONP_DIST_FLAG,
  DETAIL.PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  DETAIL.HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  DETAIL.HOTEL_TIER_ID AS HOTEL_TIER_ID,
  DETAIL.STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  DETAIL.STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
  DETAIL.STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  DETAIL.STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
  DETAIL.FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  DETAIL.FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
  DETAIL.SSDW_FLAG AS SSDW_FLAG,
  DETAIL.DOG_WALK_FLAG AS DOG_WALK_FLAG,
  DETAIL.GROOMERY_FLAG AS GROOMERY_FLAG,
  DETAIL.STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  DETAIL.MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  DETAIL.NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  MASTER.LOCATION_ID AS LOCATION_ID1,
  MASTER.STORE_NBR AS STORE_NBR1,
  MASTER.BOUTIQUE_FLAG AS BOUTIQUE_FLAG1,
  MASTER.VET_TYPE_ID AS VET_TYPE_ID,
  MASTER.VET_TYPE_DESC AS VET_TYPE_DESC,
  MASTER.MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG1,
  MASTER.ONP_DIST_FLAG AS ONP_DIST_FLAG1,
  MASTER.PET_TRAINING_FLAG AS PET_TRAINING_FLAG1,
  MASTER.HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG1,
  MASTER.HOTEL_TIER_ID AS HOTEL_TIER_ID1,
  MASTER.STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID1,
  MASTER.STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC1,
  MASTER.STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID1,
  MASTER.STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC1,
  MASTER.FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID1,
  MASTER.FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC1,
  MASTER.SSDW_FLAG AS SSDW_FLAG1,
  MASTER.DOG_WALK_FLAG AS DOG_WALK_FLAG1,
  MASTER.GROOMERY_FLAG AS GROOMERY_FLAG1,
  MASTER.STORE_PROGRAM_ID AS STORE_PROGRAM_ID1,
  MASTER.MICRO_SALON_FLAG AS MICRO_SALON_FLAG1,
  MASTER.NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG1,
  MASTER.UPDATE_TSTMP AS UPDATE_TSTMP,
  MASTER.LOAD_TSTMP AS LOAD_TSTMP,
  DETAIL.MARKET_LEADER_ID AS MARKET_LEADER_ID,
  DETAIL.MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
  MASTER.MARKET_LEADER_ID AS MARKET_LEADER_ID1,
  MASTER.MARKET_LEADER_DESC AS MARKET_LEADER_DESC1,
  DETAIL.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_USR_STORE_ATTRIBUTES_4 MASTER
  RIGHT JOIN EXP_ADDING_FILLER_ZEROS_2 DETAIL ON MASTER.LOCATION_ID = DETAIL.LOCATION_ID"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("JNR_UPD_STRATEGY_5")

# COMMAND ----------
# DBTITLE 1, EXP_UPD_STRATEGY_6


query_6 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  VET_TYPE_ID AS VET_TYPE_ID,
  VET_TYPE_DESC1 AS VET_TYPE_DESC,
  MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  ONP_DIST_FLAG AS ONP_DIST_FLAG,
  PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  HOTEL_TIER_ID AS HOTEL_TIER_ID,
  STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
  STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
  FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
  IFF(ISNULL(SSDW_FLAG1), SSDW_FLAG, SSDW_FLAG1) AS o_SSDW_FLAG,
  IFF(
    ISNULL(DOG_WALK_FLAG1),
    DOG_WALK_FLAG,
    DOG_WALK_FLAG1
  ) AS o_DOG_WALK_FLAG,
  GROOMERY_FLAG AS GROOMERY_FLAG,
  STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  now() AS UPDATE_TSTMP,
  IFF(ISNULL(LOAD_TSTMP), now(), LOAD_TSTMP) AS o_LOAD_TSTMP,
  MD5(
    LOCATION_ID || STORE_NBR || BOUTIQUE_FLAG || VET_TYPE_ID || VET_TYPE_DESC || MICRO_HOTEL_FLAG || ONP_DIST_FLAG || PET_TRAINING_FLAG || HOTEL_PROTOTYPE_FLAG || HOTEL_TIER_ID || STORE_CENTER_TYPE_ID || STORE_CENTER_TYPE_DESC || STORE_SIZE_TYPE_ID || STORE_SIZE_TYPE_DESC || FISH_SYSTEM_TYPE_ID || FISH_SYSTEM_TYPE_DESC || GROOMERY_FLAG || STORE_PROGRAM_ID || MICRO_SALON_FLAG || NO_FORKLIFT_FLAG || MARKET_LEADER_ID || MARKET_LEADER_DESC
  ) = MD5(
    LOCATION_ID1 || STORE_NBR1 || BOUTIQUE_FLAG1 || VET_TYPE_ID1 || VET_TYPE_DESC1 || MICRO_HOTEL_FLAG1 || ONP_DIST_FLAG1 || PET_TRAINING_FLAG1 || HOTEL_PROTOTYPE_FLAG1 || HOTEL_TIER_ID1 || STORE_CENTER_TYPE_ID1 || STORE_CENTER_TYPE_DESC1 || STORE_SIZE_TYPE_ID1 || STORE_SIZE_TYPE_DESC1 || FISH_SYSTEM_TYPE_ID1 || FISH_SYSTEM_TYPE_DESC1 || GROOMERY_FLAG1 || STORE_PROGRAM_ID1 || MICRO_SALON_FLAG1 || NO_FORKLIFT_FLAG1 || MARKET_LEADER_ID1 || MARKET_LEADER_DESC1
  ) AS MD5_UPD,
  IFF(ISNULL(LOCATION_ID1), 1, 0) AS NEW_RECORD,
  IFF(
    IFF(ISNULL(LOCATION_ID1), 1, 0) = 1,
    'DD_INSERT',
    IFF(
      MD5(
        LOCATION_ID || STORE_NBR || BOUTIQUE_FLAG || VET_TYPE_ID || VET_TYPE_DESC1 || MICRO_HOTEL_FLAG || ONP_DIST_FLAG || PET_TRAINING_FLAG || HOTEL_PROTOTYPE_FLAG || HOTEL_TIER_ID || STORE_CENTER_TYPE_ID || STORE_CENTER_TYPE_DESC || STORE_SIZE_TYPE_ID || STORE_SIZE_TYPE_DESC || FISH_SYSTEM_TYPE_ID || FISH_SYSTEM_TYPE_DESC || GROOMERY_FLAG || STORE_PROGRAM_ID || MICRO_SALON_FLAG || NO_FORKLIFT_FLAG || MARKET_LEADER_ID || MARKET_LEADER_DESC
      ) = MD5(
        LOCATION_ID1 || STORE_NBR1 || BOUTIQUE_FLAG1 || VET_TYPE_ID || VET_TYPE_DESC1 || MICRO_HOTEL_FLAG1 || ONP_DIST_FLAG1 || PET_TRAINING_FLAG1 || HOTEL_PROTOTYPE_FLAG1 || HOTEL_TIER_ID1 || STORE_CENTER_TYPE_ID1 || STORE_CENTER_TYPE_DESC1 || STORE_SIZE_TYPE_ID1 || STORE_SIZE_TYPE_DESC1 || FISH_SYSTEM_TYPE_ID1 || FISH_SYSTEM_TYPE_DESC1 || GROOMERY_FLAG1 || STORE_PROGRAM_ID1 || MICRO_SALON_FLAG1 || NO_FORKLIFT_FLAG1 || MARKET_LEADER_ID1 || MARKET_LEADER_DESC1
      ) = 0,
      'DD_UPDATE',
      'DD_REJECT'
    )
  ) AS UPD_STRATEGY,
  MARKET_LEADER_ID AS MARKET_LEADER_ID,
  MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  JNR_UPD_STRATEGY_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_UPD_STRATEGY_6")

# COMMAND ----------
# DBTITLE 1, FIL_NO_CHANGES_7


query_7 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  VET_TYPE_ID AS VET_TYPE_ID,
  VET_TYPE_DESC AS VET_TYPE_DESC,
  MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  ONP_DIST_FLAG AS ONP_DIST_FLAG,
  PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  HOTEL_TIER_ID AS HOTEL_TIER_ID,
  STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
  STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
  FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
  o_SSDW_FLAG AS o_SSDW_FLAG,
  o_DOG_WALK_FLAG AS o_DOG_WALK_FLAG,
  GROOMERY_FLAG AS GROOMERY_FLAG,
  STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  o_LOAD_TSTMP AS o_LOAD_TSTMP,
  UPD_STRATEGY AS UPD_STRATEGY,
  MARKET_LEADER_ID AS MARKET_LEADER_ID,
  MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_UPD_STRATEGY_6
WHERE
  UPD_STRATEGY <> 'DD_REJECT'"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("FIL_NO_CHANGES_7")

# COMMAND ----------
# DBTITLE 1, UPD_STRATEGY_8


query_8 = f"""SELECT
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  BOUTIQUE_FLAG AS BOUTIQUE_FLAG,
  VET_TYPE_ID AS VET_TYPE_ID,
  VET_TYPE_DESC AS VET_TYPE_DESC,
  MICRO_HOTEL_FLAG AS MICRO_HOTEL_FLAG,
  ONP_DIST_FLAG AS ONP_DIST_FLAG,
  PET_TRAINING_FLAG AS PET_TRAINING_FLAG,
  HOTEL_PROTOTYPE_FLAG AS HOTEL_PROTOTYPE_FLAG,
  HOTEL_TIER_ID AS HOTEL_TIER_ID,
  STORE_CENTER_TYPE_ID AS STORE_CENTER_TYPE_ID,
  STORE_CENTER_TYPE_DESC AS STORE_CENTER_TYPE_DESC,
  STORE_SIZE_TYPE_ID AS STORE_SIZE_TYPE_ID,
  STORE_SIZE_TYPE_DESC AS STORE_SIZE_TYPE_DESC,
  FISH_SYSTEM_TYPE_ID AS FISH_SYSTEM_TYPE_ID,
  FISH_SYSTEM_TYPE_DESC AS FISH_SYSTEM_TYPE_DESC,
  o_SSDW_FLAG AS o_SSDW_FLAG,
  o_DOG_WALK_FLAG AS o_DOG_WALK_FLAG,
  GROOMERY_FLAG AS GROOMERY_FLAG,
  STORE_PROGRAM_ID AS STORE_PROGRAM_ID,
  MICRO_SALON_FLAG AS MICRO_SALON_FLAG,
  NO_FORKLIFT_FLAG AS NO_FORKLIFT_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  o_LOAD_TSTMP AS o_LOAD_TSTMP,
  UPD_STRATEGY AS UPD_STRATEGY,
  MARKET_LEADER_ID AS MARKET_LEADER_ID,
  MARKET_LEADER_DESC AS MARKET_LEADER_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  UPD_STRATEGY AS UPDATE_STRATEGY_FLAG
FROM
  FIL_NO_CHANGES_7"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("UPD_STRATEGY_8")

# COMMAND ----------
# DBTITLE 1, USR_STORE_ATTRIBUTES


spark.sql("""MERGE INTO USR_STORE_ATTRIBUTES AS TARGET
USING
  UPD_STRATEGY_8 AS SOURCE ON TARGET.LOCATION_ID = SOURCE.LOCATION_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.LOCATION_ID = SOURCE.LOCATION_ID,
  TARGET.STORE_NBR = SOURCE.STORE_NBR,
  TARGET.BOUTIQUE_FLAG = SOURCE.BOUTIQUE_FLAG,
  TARGET.VET_TYPE_ID = SOURCE.VET_TYPE_ID,
  TARGET.VET_TYPE_DESC = SOURCE.VET_TYPE_DESC,
  TARGET.MICRO_HOTEL_FLAG = SOURCE.MICRO_HOTEL_FLAG,
  TARGET.ONP_DIST_FLAG = SOURCE.ONP_DIST_FLAG,
  TARGET.PET_TRAINING_FLAG = SOURCE.PET_TRAINING_FLAG,
  TARGET.HOTEL_PROTOTYPE_FLAG = SOURCE.HOTEL_PROTOTYPE_FLAG,
  TARGET.HOTEL_TIER_ID = SOURCE.HOTEL_TIER_ID,
  TARGET.STORE_CENTER_TYPE_ID = SOURCE.STORE_CENTER_TYPE_ID,
  TARGET.STORE_CENTER_TYPE_DESC = SOURCE.STORE_CENTER_TYPE_DESC,
  TARGET.STORE_SIZE_TYPE_ID = SOURCE.STORE_SIZE_TYPE_ID,
  TARGET.STORE_SIZE_TYPE_DESC = SOURCE.STORE_SIZE_TYPE_DESC,
  TARGET.FISH_SYSTEM_TYPE_ID = SOURCE.FISH_SYSTEM_TYPE_ID,
  TARGET.FISH_SYSTEM_TYPE_DESC = SOURCE.FISH_SYSTEM_TYPE_DESC,
  TARGET.SSDW_FLAG = SOURCE.o_SSDW_FLAG,
  TARGET.DOG_WALK_FLAG = SOURCE.o_DOG_WALK_FLAG,
  TARGET.GROOMERY_FLAG = SOURCE.GROOMERY_FLAG,
  TARGET.STORE_PROGRAM_ID = SOURCE.STORE_PROGRAM_ID,
  TARGET.MICRO_SALON_FLAG = SOURCE.MICRO_SALON_FLAG,
  TARGET.NO_FORKLIFT_FLAG = SOURCE.NO_FORKLIFT_FLAG,
  TARGET.MARKET_LEADER_ID = SOURCE.MARKET_LEADER_ID,
  TARGET.MARKET_LEADER_DESC = SOURCE.MARKET_LEADER_DESC,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.STORE_NBR = SOURCE.STORE_NBR
  AND TARGET.BOUTIQUE_FLAG = SOURCE.BOUTIQUE_FLAG
  AND TARGET.VET_TYPE_ID = SOURCE.VET_TYPE_ID
  AND TARGET.VET_TYPE_DESC = SOURCE.VET_TYPE_DESC
  AND TARGET.MICRO_HOTEL_FLAG = SOURCE.MICRO_HOTEL_FLAG
  AND TARGET.ONP_DIST_FLAG = SOURCE.ONP_DIST_FLAG
  AND TARGET.PET_TRAINING_FLAG = SOURCE.PET_TRAINING_FLAG
  AND TARGET.HOTEL_PROTOTYPE_FLAG = SOURCE.HOTEL_PROTOTYPE_FLAG
  AND TARGET.HOTEL_TIER_ID = SOURCE.HOTEL_TIER_ID
  AND TARGET.STORE_CENTER_TYPE_ID = SOURCE.STORE_CENTER_TYPE_ID
  AND TARGET.STORE_CENTER_TYPE_DESC = SOURCE.STORE_CENTER_TYPE_DESC
  AND TARGET.STORE_SIZE_TYPE_ID = SOURCE.STORE_SIZE_TYPE_ID
  AND TARGET.STORE_SIZE_TYPE_DESC = SOURCE.STORE_SIZE_TYPE_DESC
  AND TARGET.FISH_SYSTEM_TYPE_ID = SOURCE.FISH_SYSTEM_TYPE_ID
  AND TARGET.FISH_SYSTEM_TYPE_DESC = SOURCE.FISH_SYSTEM_TYPE_DESC
  AND TARGET.SSDW_FLAG = SOURCE.o_SSDW_FLAG
  AND TARGET.DOG_WALK_FLAG = SOURCE.o_DOG_WALK_FLAG
  AND TARGET.GROOMERY_FLAG = SOURCE.GROOMERY_FLAG
  AND TARGET.STORE_PROGRAM_ID = SOURCE.STORE_PROGRAM_ID
  AND TARGET.MICRO_SALON_FLAG = SOURCE.MICRO_SALON_FLAG
  AND TARGET.NO_FORKLIFT_FLAG = SOURCE.NO_FORKLIFT_FLAG
  AND TARGET.MARKET_LEADER_ID = SOURCE.MARKET_LEADER_ID
  AND TARGET.MARKET_LEADER_DESC = SOURCE.MARKET_LEADER_DESC
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.o_LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.LOCATION_ID,
    TARGET.STORE_NBR,
    TARGET.BOUTIQUE_FLAG,
    TARGET.VET_TYPE_ID,
    TARGET.VET_TYPE_DESC,
    TARGET.MICRO_HOTEL_FLAG,
    TARGET.ONP_DIST_FLAG,
    TARGET.PET_TRAINING_FLAG,
    TARGET.HOTEL_PROTOTYPE_FLAG,
    TARGET.HOTEL_TIER_ID,
    TARGET.STORE_CENTER_TYPE_ID,
    TARGET.STORE_CENTER_TYPE_DESC,
    TARGET.STORE_SIZE_TYPE_ID,
    TARGET.STORE_SIZE_TYPE_DESC,
    TARGET.FISH_SYSTEM_TYPE_ID,
    TARGET.FISH_SYSTEM_TYPE_DESC,
    TARGET.SSDW_FLAG,
    TARGET.DOG_WALK_FLAG,
    TARGET.GROOMERY_FLAG,
    TARGET.STORE_PROGRAM_ID,
    TARGET.MICRO_SALON_FLAG,
    TARGET.NO_FORKLIFT_FLAG,
    TARGET.MARKET_LEADER_ID,
    TARGET.MARKET_LEADER_DESC,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.LOCATION_ID,
    SOURCE.STORE_NBR,
    SOURCE.BOUTIQUE_FLAG,
    SOURCE.VET_TYPE_ID,
    SOURCE.VET_TYPE_DESC,
    SOURCE.MICRO_HOTEL_FLAG,
    SOURCE.ONP_DIST_FLAG,
    SOURCE.PET_TRAINING_FLAG,
    SOURCE.HOTEL_PROTOTYPE_FLAG,
    SOURCE.HOTEL_TIER_ID,
    SOURCE.STORE_CENTER_TYPE_ID,
    SOURCE.STORE_CENTER_TYPE_DESC,
    SOURCE.STORE_SIZE_TYPE_ID,
    SOURCE.STORE_SIZE_TYPE_DESC,
    SOURCE.FISH_SYSTEM_TYPE_ID,
    SOURCE.FISH_SYSTEM_TYPE_DESC,
    SOURCE.o_SSDW_FLAG,
    SOURCE.o_DOG_WALK_FLAG,
    SOURCE.GROOMERY_FLAG,
    SOURCE.STORE_PROGRAM_ID,
    SOURCE.MICRO_SALON_FLAG,
    SOURCE.NO_FORKLIFT_FLAG,
    SOURCE.MARKET_LEADER_ID,
    SOURCE.MARKET_LEADER_DESC,
    SOURCE.UPDATE_TSTMP,
    SOURCE.o_LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_USR_Store_Attributes")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_USR_Store_Attributes", mainWorkflowId, parentName)