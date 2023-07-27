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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_PET_ALLERGIES")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_PET_ALLERGIES", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PET_ALLERGIES_PRE_0


query_0 = f"""SELECT
  ALLERGY_ID AS ALLERGY_ID,
  DESCRIPTION AS DESCRIPTION,
  IS_ACTIVE AS IS_ACTIVE,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  PET_ALLERGIES_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PET_ALLERGIES_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_ALLERGIES_PRE_1


query_1 = f"""SELECT
  ALLERGY_ID AS ALLERGY_ID,
  DESCRIPTION AS DESCRIPTION,
  IS_ACTIVE AS IS_ACTIVE,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PET_ALLERGIES_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PET_ALLERGIES_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PET_ALLERGIES1_2


query_2 = f"""SELECT
  PET_ALLERGY_ID AS PET_ALLERGY_ID,
  PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
  IS_ACTIVE AS IS_ACTIVE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  PET_ALLERGIES"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_PET_ALLERGIES1_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PET_ALLERGIES_3


query_3 = f"""SELECT
  PET_ALLERGY_ID AS PET_ALLERGY_ID,
  PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
  IS_ACTIVE AS IS_ACTIVE,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PET_ALLERGIES1_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PET_ALLERGIES_3")

# COMMAND ----------
# DBTITLE 1, jnr_Pre_DWTable_4


query_4 = f"""SELECT
  MASTER.ALLERGY_ID AS ALLERGY_ID,
  MASTER.DESCRIPTION AS DESCRIPTION,
  MASTER.IS_ACTIVE AS IS_ACTIVE,
  DETAIL.PET_ALLERGY_ID AS PET_ALLERGY_ID,
  DETAIL.PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
  DETAIL.IS_ACTIVE AS IS_ACTIVE1,
  MASTER.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PET_ALLERGIES_PRE_1 MASTER
  LEFT JOIN SQ_Shortcut_to_PET_ALLERGIES_3 DETAIL ON MASTER.ALLERGY_ID = DETAIL.PET_ALLERGY_ID"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("jnr_Pre_DWTable_4")

# COMMAND ----------
# DBTITLE 1, exp_FLAGS_5


query_5 = f"""SELECT
  MD5(
    (
      to_char(PET_ALLERGY_ID) || ltrim(rtrim(PET_ALLERGY_DESC)) || to_char(IS_ACTIVE)
    )
  ) AS pre_MD5,
  MD5(
    (
      to_char(PET_ALLERGY_ID1) || ltrim(rtrim(PET_ALLERGY_DESC1)) || to_char(IS_ACTIVE1)
    )
  ) AS final_MD5,
  IFF(
    ISNULL(PET_ALLERGY_ID1),
    'INSERT',
    IFF(pre_MD5 != final_MD5, 'UPDATE', 'REJECT')
  ) AS v_LOAD_FLAG,
  IFF(
    ISNULL(ALLERGY_ID),
    'INSERT',
    IFF(
      MD5(
        (
          to_char(ALLERGY_ID) || ltrim(rtrim(DESCRIPTION)) || to_char(IS_ACTIVE)
        )
      ) != MD5(
        (
          to_char(ALLERGY_ID) || ltrim(rtrim(PET_ALLERGY_DESC)) || to_char(IS_ACTIVE1)
        )
      ),
      'UPDATE',
      'REJECT'
    )
  ) AS LOAD_FLAG,
  ALLERGY_ID AS PET_ALLERGY_ID,
  DESCRIPTION AS PET_ALLERGY_DESC,
  IS_ACTIVE AS IS_ACTIVE,
  now() AS UPDATE_TSTMP,
  now() AS LOAD_TSTMP,
  ALLERGY_ID AS PET_ALLERGY_ID1,
  PET_ALLERGY_DESC AS PET_ALLERGY_DESC1,
  IS_ACTIVE1 AS IS_ACTIVE1,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  jnr_Pre_DWTable_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("exp_FLAGS_5")

# COMMAND ----------
# DBTITLE 1, fil_FLAGS_6


query_6 = f"""SELECT
  LOAD_FLAG AS LOAD_FLAG,
  PET_ALLERGY_ID AS PET_ALLERGY_ID,
  PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
  IS_ACTIVE AS IS_ACTIVE,
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
  PET_ALLERGY_ID AS PET_ALLERGY_ID,
  PET_ALLERGY_DESC AS PET_ALLERGY_DESC,
  IS_ACTIVE AS IS_ACTIVE,
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
# DBTITLE 1, PET_ALLERGIES


spark.sql("""MERGE INTO PET_ALLERGIES AS TARGET
USING
  upd_FLAG_7 AS SOURCE ON TARGET.PET_ALLERGY_ID = SOURCE.PET_ALLERGY_ID
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.PET_ALLERGY_ID = SOURCE.PET_ALLERGY_ID,
  TARGET.PET_ALLERGY_DESC = SOURCE.PET_ALLERGY_DESC,
  TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE,
  TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP,
  TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.PET_ALLERGY_DESC = SOURCE.PET_ALLERGY_DESC
  AND TARGET.IS_ACTIVE = SOURCE.IS_ACTIVE
  AND TARGET.UPDATE_TSTMP = SOURCE.UPDATE_TSTMP
  AND TARGET.LOAD_TSTMP = SOURCE.LOAD_TSTMP THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.PET_ALLERGY_ID,
    TARGET.PET_ALLERGY_DESC,
    TARGET.IS_ACTIVE,
    TARGET.UPDATE_TSTMP,
    TARGET.LOAD_TSTMP
  )
VALUES
  (
    SOURCE.PET_ALLERGY_ID,
    SOURCE.PET_ALLERGY_DESC,
    SOURCE.IS_ACTIVE,
    SOURCE.UPDATE_TSTMP,
    SOURCE.LOAD_TSTMP
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_PET_ALLERGIES")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_PET_ALLERGIES", mainWorkflowId, parentName)
