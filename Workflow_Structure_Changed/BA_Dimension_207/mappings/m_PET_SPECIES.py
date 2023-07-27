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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_PET_SPECIES")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_PET_SPECIES", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PET_SPECIES_PRE_0


query_0 = f"""SELECT
  SPECIES_ID AS SPECIES_ID,
  NAME AS NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  PET_SPECIES_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PET_SPECIES_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_SPECIES_PRE_1


query_1 = f"""SELECT
  SPECIES_ID AS SPECIES_ID,
  NAME AS NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PET_SPECIES_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PET_SPECIES_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PET_SPECIES1_2


query_2 = f"""SELECT
  PET_SPECIES_ID AS PET_SPECIES_ID,
  PET_SPECIES_DESC AS PET_SPECIES_DESC,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  PET_SPECIES"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_PET_SPECIES1_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_SPECIES_3


query_3 = f"""SELECT
  PET_SPECIES_ID AS PET_SPECIES_ID,
  PET_SPECIES_DESC AS PET_SPECIES_DESC,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PET_SPECIES1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PET_SPECIES_3")

# COMMAND ----------
# DBTITLE 1, jnr_Pre_DWTable_4


query_4 = f"""SELECT
  MASTER.SPECIES_ID AS SPECIES_ID,
  MASTER.NAME AS NAME,
  MASTER.IS_ACTIVE AS IS_ACTIVE,
  MASTER.IS_FIRST_PARTY AS IS_FIRST_PARTY,
  DETAIL.PET_SPECIES_ID AS PET_SPECIES_ID,
  DETAIL.PET_SPECIES_DESC AS PET_SPECIES_DESC,
  DETAIL.IS_ACTIVE AS IS_ACTIVE1,
  DETAIL.IS_FIRST_PARTY AS IS_FIRST_PARTY1,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PET_SPECIES_PRE_1 MASTER
  LEFT JOIN SQ_Shortcut_to_PET_SPECIES_3 DETAIL ON MASTER.SPECIES_ID = DETAIL.PET_SPECIES_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("jnr_Pre_DWTable_4")

# COMMAND ----------
# DBTITLE 1, exp_FLAGS_5


query_5 = f"""SELECT
  SPECIES_ID AS PET_SPECIES_ID,
  NAME AS PET_SPECIES_DESC,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  now() AS UPDATE_TSTMP,
  now() AS LOAD_TSTMP,
  MD5(
    to_char(PET_SPECIES_ID) || ltrim(rtrim(PET_SPECIES_DESC)) || to_char(IS_ACTIVE)
  ) AS pre_MD5,
  MD5(
    to_char(PET_SPECIES_ID1) || ltrim(rtrim(PET_SPECIES_DESC1)) || to_char(IS_ACTIVE1)
  ) AS final_MD5,
  IFF(
    ISNULL(PET_SPECIES_ID1),
    'INSERT',
    IFF(pre_MD5 != final_MD5, 'UPDATE', 'REJECT')
  ) AS v_LOAD_FLAG,
  IFF(
    ISNULL(SPECIES_ID),
    'INSERT',
    IFF(
      MD5(
        to_char(SPECIES_ID) || ltrim(rtrim(NAME)) || to_char(IS_ACTIVE)
      ) != MD5(
        to_char(SPECIES_ID) || ltrim(rtrim(PET_SPECIES_DESC)) || to_char(IS_ACTIVE1)
      ),
      'UPDATE',
      'REJECT'
    )
  ) AS LOAD_FLAG,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  jnr_Pre_DWTable_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("exp_FLAGS_5")

# COMMAND ----------
# DBTITLE 1, fil_FLAGS_6


query_6 = f"""SELECT
  LOAD_FLAG AS LOAD_FLAG,
  PET_SPECIES_ID AS PET_SPECIES_ID,
  PET_SPECIES_DESC AS PET_SPECIES_DESC,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  exp_FLAGS_5
WHERE
  IN(LOAD_FLAG, 'INSERT', 'UPDATE')"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("fil_FLAGS_6")

# COMMAND ----------
# DBTITLE 1, upd_FLAG_7


query_7 = f"""SELECT
  LOAD_FLAG AS LOAD_FLAG,
  PET_SPECIES_ID AS PET_SPECIES_ID,
  PET_SPECIES_DESC AS PET_SPECIES_DESC,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(
    LOAD_FLAG = 'INSERT',
    'DD_INSERT',
    IFF(LOAD_FLAG = 'UPDATE', 'DD_UPDATE', 'DD_REJECT')
  ) AS UPDATE_STRATEGY_FLAG
FROM
  fil_FLAGS_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("upd_FLAG_7")

# COMMAND ----------
# DBTITLE 1, PET_SPECIES


spark.sql("""MERGE INTO PET_SPECIES AS TARGET
USING
  upd_FLAG_7 AS SOURCE ON TARGET.PET_SPECIES_ID = SOURCE.PET_SPECIES_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PET_SPECIES_ID = SOURCE.PET_SPECIES_ID,
  TARGET.PET_SPECIES_DESC = SOURCE.PET_SPECIES_DESC,
  TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE,
  TARGET.IS_FIRST_PARTY = SOURCE.IS_FIRST_PARTY,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PET_SPECIES_DESC = SOURCE.PET_SPECIES_DESC
  AND TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE
  AND TARGET.IS_FIRST_PARTY = SOURCE.IS_FIRST_PARTY
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PET_SPECIES_ID,
    TARGET.PET_SPECIES_DESC,
    TARGET.IS_ACTIVE,
    TARGET.IS_FIRST_PARTY,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.PET_SPECIES_ID,
    SOURCE.PET_SPECIES_DESC,
    SOURCE.IS_ACTIVE,
    SOURCE.IS_FIRST_PARTY,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_PET_SPECIES")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_PET_SPECIES", mainWorkflowId, parentName)
