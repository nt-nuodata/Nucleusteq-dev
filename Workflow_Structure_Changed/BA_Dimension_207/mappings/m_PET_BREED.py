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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_PET_BREED")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_PET_BREED", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PET_BREED_PRE_0


query_0 = f"""SELECT
  BREED_ID AS BREED_ID,
  DESCRIPTION AS DESCRIPTION,
  IS_AGGRESSIVE AS IS_AGGRESSIVE,
  SPECIES_ID AS SPECIES_ID,
  SPECIES_NAME AS SPECIES_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  PET_BREED_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PET_BREED_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_BREED_PRE_1


query_1 = f"""SELECT
  BREED_ID AS BREED_ID,
  DESCRIPTION AS DESCRIPTION,
  IS_AGGRESSIVE AS IS_AGGRESSIVE,
  SPECIES_ID AS SPECIES_ID,
  SPECIES_NAME AS SPECIES_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY AS IS_FIRST_PARTY,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PET_BREED_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PET_BREED_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PET_BREED1_2


query_2 = f"""SELECT
  PET_BREED_ID AS PET_BREED_ID,
  PET_BREED_DESC AS PET_BREED_DESC,
  IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
  PET_SPECIES_ID AS PET_SPECIES_ID,
  PET_SPECIES_NAME AS PET_SPECIES_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  PET_BREED"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_PET_BREED1_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_BREED_3


query_3 = f"""SELECT
  PET_BREED_ID AS PET_BREED_ID,
  PET_BREED_DESC AS PET_BREED_DESC,
  IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
  PET_SPECIES_ID AS PET_SPECIES_ID,
  PET_SPECIES_NAME AS PET_SPECIES_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PET_BREED1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PET_BREED_3")

# COMMAND ----------
# DBTITLE 1, jnr_Pre_DWTable_4


query_4 = f"""SELECT
  MASTER.BREED_ID AS BREED_ID,
  MASTER.DESCRIPTION AS DESCRIPTION,
  MASTER.IS_AGGRESSIVE AS IS_AGGRESSIVE,
  MASTER.SPECIES_ID AS SPECIES_ID,
  MASTER.SPECIES_NAME AS SPECIES_NAME,
  MASTER.IS_ACTIVE AS IS_ACTIVE,
  MASTER.IS_FIRST_PARTY AS IS_FIRST_PARTY,
  DETAIL.PET_BREED_ID AS PET_BREED_ID,
  DETAIL.PET_BREED_DESC AS PET_BREED_DESC,
  DETAIL.IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
  DETAIL.PET_SPECIES_ID AS PET_SPECIES_ID,
  DETAIL.PET_SPECIES_NAME AS PET_SPECIES_NAME,
  DETAIL.IS_ACTIVE AS IS_ACTIVE1,
  DETAIL.IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PET_BREED_PRE_1 MASTER
  LEFT JOIN SQ_Shortcut_to_PET_BREED_3 DETAIL ON MASTER.BREED_ID = DETAIL.PET_BREED_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("jnr_Pre_DWTable_4")

# COMMAND ----------
# DBTITLE 1, exp_FLAGS_5


query_5 = f"""SELECT
  now() AS UPDATE_TSTMP,
  now() AS LOAD_TSTMP,
  BREED_ID AS BREED_ID,
  DESCRIPTION AS DESCRIPTION,
  IS_AGGRESSIVE AS IS_AGGRESSIVE,
  SPECIES_ID AS SPECIES_ID,
  SPECIES_NAME AS SPECIES_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IFF(upper(LTRIM(RTRIM(IS_FIRST_PARTY))) = 'TRUE', 1, 0) AS v_IS_FIRST_PARTY_FLAG,
  IFF(upper(LTRIM(RTRIM(IS_FIRST_PARTY))) = 'TRUE', 1, 0) AS IS_FIRST_PARTY_FLAG,
  MD5(
    to_char(BREED_ID) || ltrim(rtrim(DESCRIPTION)) || to_char(IS_AGGRESSIVE) || to_char(SPECIES_ID) || ltrim(rtrim(SPECIES_NAME)) || to_char(IS_ACTIVE) || to_char(v_IS_FIRST_PARTY_FLAG)
  ) AS pre_MD5,
  MD5(
    to_char(PET_BREED_ID) || ltrim(rtrim(PET_BREED_DESC)) || to_char(IS_AGGRESSIVE_FLAG) || to_char(PET_SPECIES_ID) || ltrim(rtrim(PET_SPECIES_NAME)) || to_char(IS_ACTIVE1) || to_char(IS_FIRST_PARTY_FLAG1)
  ) AS final_MD5,
  IFF(
    ISNULL(PET_BREED_ID),
    'INSERT',
    IFF(pre_MD5 != final_MD5, 'UPDATE', 'REJECT')
  ) AS v_LOAD_FLAG,
  IFF(
    ISNULL(PET_BREED_ID),
    'INSERT',
    IFF(
      MD5(
        to_char(BREED_ID) || ltrim(rtrim(DESCRIPTION)) || to_char(IS_AGGRESSIVE) || to_char(SPECIES_ID) || ltrim(rtrim(SPECIES_NAME)) || to_char(IS_ACTIVE) || to_char(
          IFF(upper(LTRIM(RTRIM(IS_FIRST_PARTY))) = 'TRUE', 1, 0)
        )
      ) != MD5(
        to_char(PET_BREED_ID) || ltrim(rtrim(PET_BREED_DESC)) || to_char(IS_AGGRESSIVE_FLAG) || to_char(PET_SPECIES_ID) || ltrim(rtrim(PET_SPECIES_NAME)) || to_char(IS_ACTIVE1) || to_char(IS_FIRST_PARTY_FLAG)
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
  BREED_ID AS PET_BREED_ID,
  DESCRIPTION AS PET_BREED_DESC,
  IS_AGGRESSIVE AS IS_AGGRESSIVE_FLAG,
  SPECIES_ID AS PET_SPECIES_ID,
  SPECIES_NAME AS PET_SPECIES_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
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
  PET_BREED_ID AS PET_BREED_ID,
  PET_BREED_DESC AS PET_BREED_DESC,
  IS_AGGRESSIVE_FLAG AS IS_AGGRESSIVE_FLAG,
  PET_SPECIES_ID AS PET_SPECIES_ID,
  PET_SPECIES_NAME AS PET_SPECIES_NAME,
  IS_ACTIVE AS IS_ACTIVE,
  IS_FIRST_PARTY_FLAG AS IS_FIRST_PARTY_FLAG,
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
# DBTITLE 1, PET_BREED


spark.sql("""MERGE INTO PET_BREED AS TARGET
USING
  upd_FLAG_7 AS SOURCE ON TARGET.PET_BREED_ID = SOURCE.PET_BREED_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PET_BREED_ID = SOURCE.PET_BREED_ID,
  TARGET.PET_BREED_DESC = SOURCE.PET_BREED_DESC,
  TARGET.IS_AGGRESSIVE_FLAG = SOURCE.IS_AGGRESSIVE_FLAG,
  TARGET.PET_SPECIES_ID = SOURCE.PET_SPECIES_ID,
  TARGET.PET_SPECIES_NAME = SOURCE.PET_SPECIES_NAME,
  TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE,
  TARGET.IS_FIRST_PARTY_FLAG = SOURCE.IS_FIRST_PARTY_FLAG,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PET_BREED_DESC = SOURCE.PET_BREED_DESC
  AND TARGET.IS_AGGRESSIVE_FLAG = SOURCE.IS_AGGRESSIVE_FLAG
  AND TARGET.PET_SPECIES_ID = SOURCE.PET_SPECIES_ID
  AND TARGET.PET_SPECIES_NAME = SOURCE.PET_SPECIES_NAME
  AND TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE
  AND TARGET.IS_FIRST_PARTY_FLAG = SOURCE.IS_FIRST_PARTY_FLAG
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PET_BREED_ID,
    TARGET.PET_BREED_DESC,
    TARGET.IS_AGGRESSIVE_FLAG,
    TARGET.PET_SPECIES_ID,
    TARGET.PET_SPECIES_NAME,
    TARGET.IS_ACTIVE,
    TARGET.IS_FIRST_PARTY_FLAG,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.PET_BREED_ID,
    SOURCE.PET_BREED_DESC,
    SOURCE.IS_AGGRESSIVE_FLAG,
    SOURCE.PET_SPECIES_ID,
    SOURCE.PET_SPECIES_NAME,
    SOURCE.IS_ACTIVE,
    SOURCE.IS_FIRST_PARTY_FLAG,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_PET_BREED")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_PET_BREED", mainWorkflowId, parentName)
