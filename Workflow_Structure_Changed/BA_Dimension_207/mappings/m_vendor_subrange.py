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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_subrange")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_vendor_subrange", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_VENDOR_SUBRANGE_PRE_0


query_0 = f"""SELECT
  VENDOR_NBR AS VENDOR_NBR,
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  VENDOR_SUBRANGE_DESC AS VENDOR_SUBRANGE_DESC
FROM
  VENDOR_SUBRANGE_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_VENDOR_SUBRANGE_PRE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_VENDOR_SUBRANGE_PRE_1


query_1 = f"""SELECT
  VENDOR_NBR AS VENDOR_NBR,
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  VENDOR_SUBRANGE_CD AS VENDOR_SUBRANGE_CD,
  VENDOR_SUBRANGE_DESC AS VENDOR_SUBRANGE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_VENDOR_SUBRANGE_PRE_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_VENDOR_SUBRANGE_PRE_1")

# COMMAND ----------
# DBTITLE 1, EXP_Subrange_2


query_2 = f"""SELECT
  VENDOR_SUBRANGE_CD AS SubRangeCD,
  VENDOR_NBR AS VendorNbr,
  VENDOR_SUBRANGE_DESC AS SubRangeDesc,
  VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_VENDOR_SUBRANGE_PRE_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_Subrange_2")

# COMMAND ----------
# DBTITLE 1, LKP_Vendor_Profile_3


query_3 = f"""SELECT
  VP.VENDOR_ID AS VENDOR_ID,
  VP.VENDOR_TYPE_ID AS VENDOR_TYPE_ID,
  ES2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Subrange_2 ES2
  LEFT JOIN VENDOR_PROFILE VP ON VP.VENDOR_TYPE_ID = ES2.VENDOR_TYPE_ID
  AND VP.VENDOR_NBR = ES2.VendorNbr"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("LKP_Vendor_Profile_3")

# COMMAND ----------
# DBTITLE 1, EXP_Final_4


query_4 = f"""SELECT
  LVP3.VENDOR_ID AS VENDOR_ID,
  ltrim(rtrim(ES2.SubRangeCD)) AS lkp_SubRangeCD1,
  IFF(ISNULL(ES2.SubRangeCD), ' ', ES2.SubRangeCD) AS SubRangeCD,
  ES2.SubRangeDesc AS SubRangeDesc,
  LVP3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  LKP_Vendor_Profile_3 LVP3
  INNER JOIN EXP_Subrange_2 ES2 ON LVP3.Monotonically_Increasing_Id = ES2.Monotonically_Increasing_Id"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_Final_4")

# COMMAND ----------
# DBTITLE 1, LKP_Vendor_Subrange1_5


query_5 = f"""SELECT
  VS.VENDOR_ID AS VENDOR_ID,
  EF4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Final_4 EF4
  LEFT JOIN (
    SELECT
      VENDOR_SUBRANGE.VENDOR_ID as VENDOR_ID,
      Trim(VENDOR_SUBRANGE.VENDOR_SUBRANGE_CD) as VENDOR_SUBRANGE_CD
    FROM
      VENDOR_SUBRANGE
  ) AS VS ON VS.VENDOR_ID = EF4.VENDOR_ID
  AND VS.VENDOR_SUBRANGE_CD = EF4.lkp_SubRangeCD1"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("LKP_Vendor_Subrange1_5")

# COMMAND ----------
# DBTITLE 1, EXP_UPD_6


query_6 = f"""SELECT
  LVS5.VENDOR_ID AS VENDOR_ID1,
  EF4.VENDOR_ID AS VENDOR_ID,
  EF4.SubRangeCD AS SubRangeCD,
  EF4.SubRangeDesc AS SubRangeDesc,
  EF4.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_Final_4 EF4
  INNER JOIN LKP_Vendor_Subrange1_5 LVS5 ON EF4.Monotonically_Increasing_Id = LVS5.Monotonically_Increasing_Id"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_UPD_6")

# COMMAND ----------
# DBTITLE 1, UPD_UPDATE_7


query_7 = f"""SELECT
  VENDOR_ID1 AS VENDOR_ID3,
  SubRangeCD AS SubRangeCd3,
  SubRangeDesc AS SubRangeDesc3,
  VENDOR_ID1 AS VENDOR_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  IFF(ISNULL(VENDOR_ID1), 'DD_INSERT', 'DD_UPDATE') AS UPDATE_STRATEGY_FLAG
FROM
  EXP_UPD_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("UPD_UPDATE_7")

# COMMAND ----------
# DBTITLE 1, VENDOR_SUBRANGE


spark.sql("""MERGE INTO VENDOR_SUBRANGE AS TARGET
USING
  UPD_UPDATE_7 AS SOURCE ON TARGET.VENDOR_SUBRANGE_CD = SOURCE.SubRangeCd3
  AND TARGET.VENDOR_ID = SOURCE.VENDOR_ID3
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_UPDATE" THEN
UPDATE
SET
  TARGET.VENDOR_ID = SOURCE.VENDOR_ID3,
  TARGET.VENDOR_SUBRANGE_CD = SOURCE.SubRangeCd3,
  TARGET.VENDOR_SUBRANGE_DESC = SOURCE.SubRangeDesc3
  WHEN MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_DELETE"
  AND TARGET.VENDOR_SUBRANGE_DESC = SOURCE.SubRangeDesc3 THEN DELETE
  WHEN NOT MATCHED
  AND SOURCE.UPDATE_STRATEGY_FLAG = "DD_INSERT" THEN
INSERT
  (
    TARGET.VENDOR_ID,
    TARGET.VENDOR_SUBRANGE_CD,
    TARGET.VENDOR_SUBRANGE_DESC
  )
VALUES
  (
    SOURCE.VENDOR_ID3,
    SOURCE.SubRangeCd3,
    SOURCE.SubRangeDesc3
  )""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_vendor_subrange")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_vendor_subrange", mainWorkflowId, parentName)
