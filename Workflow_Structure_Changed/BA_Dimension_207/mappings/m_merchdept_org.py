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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_merchdept_org")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_merchdept_org", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CAMerchDirector_0


query_0 = f"""SELECT
  CAMerchDirectorId AS CAMerchDirectorId,
  CAMerchVpId AS CAMerchVpId,
  CAMerchDirectorName AS CAMerchDirectorName,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  CAMerchDirector"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_CAMerchDirector_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_CAMerchBuyer_1


query_1 = f"""SELECT
  CAMerchBuyerId AS CAMerchBuyerId,
  CAMerchDirectorId AS CAMerchDirectorId,
  CAMerchBuyerName AS CAMerchBuyerName,
  CountryCd AS CountryCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  CAMerchBuyer"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_CAMerchBuyer_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchBusinessUnit_2


query_2 = f"""SELECT
  MerchBusinessUnitId AS MerchBusinessUnitId,
  MerchBusinessUnitDesc AS MerchBusinessUnitDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  MerchBusinessUnit"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_to_MerchBusinessUnit_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchSvp_3


query_3 = f"""SELECT
  MerchSvpId AS MerchSvpId,
  MerchBusinessUnitId AS MerchBusinessUnitId,
  MerchSvpDesc AS MerchSvpDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  MerchSvp"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_MerchSvp_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchSegment_4


query_4 = f"""SELECT
  MerchSegmentId AS MerchSegmentId,
  MerchGroupId AS MerchGroupId,
  MerchSegmentDesc AS MerchSegmentDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  MerchSegment"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_MerchSegment_4")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchGroup_5


query_5 = f"""SELECT
  MerchGroupId AS MerchGroupId,
  MerchGroupDesc AS MerchGroupDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  MerchGroup"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Shortcut_to_MerchGroup_5")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchVp_6


query_6 = f"""SELECT
  MerchVpId AS MerchVpId,
  MerchSvpId AS MerchSvpId,
  MerchVpName AS MerchVpName,
  MerchDesc AS MerchDesc,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  MerchVp"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Shortcut_to_MerchVp_6")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchBuyer_7


query_7 = f"""SELECT
  MerchBuyerId AS MerchBuyerId,
  MerchDirectorId AS MerchDirectorId,
  MerchBuyerName AS MerchBuyerName,
  CountryCd AS CountryCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  MerchBuyer"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Shortcut_to_MerchBuyer_7")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchDirector_8


query_8 = f"""SELECT
  MerchDirectorId AS MerchDirectorId,
  MerchVpId AS MerchVpId,
  MerchDirectorName AS MerchDirectorName,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  MerchDirector"""

df_8 = spark.sql(query_8)

df_8.createOrReplaceTempView("Shortcut_to_MerchDirector_8")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_MerchDept_9


query_9 = f"""SELECT
  MerchDeptCd AS MerchDeptCd,
  MerchDeptDesc AS MerchDeptDesc,
  MerchDivisionCd AS MerchDivisionCd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp,
  DivisionId AS DivisionId,
  DivSpeciesId AS DivSpeciesId,
  MerchBuyerId AS MerchBuyerId,
  PlannerCD AS PlannerCD,
  MerchSegmentId AS MerchSegmentId,
  CAMerchBuyerId AS CAMerchBuyerId
FROM
  MerchDept"""

df_9 = spark.sql(query_9)

df_9.createOrReplaceTempView("Shortcut_to_MerchDept_9")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_MerchDept_10


query_10 = f"""SELECT
  a17.MERCHBUSINESSUNITID AS BUS_UNIT_ID,
  a112.MERCHBUSinessUNITDESC AS BUS_UNIT_DESC,
  a12.MERCHBUYERID AS BUYER_ID,
  a14.MERCHBUYERNaMe AS BUYER_NM,
  CASE
    WHEN a12.CAMERCHBUYERID IS NULL THEN a12.MERCHBUYERID
    ELSE a12.CAMERCHBUYERID
  END AS CA_BUYER_ID,
  CASE
    WHEN a12.CAMERCHBUYERID IS NULL THEN a14.MERCHBUYERNaMe
    ELSE a18.CAMERCHBUYERNAME
  END AS CA_BUYER_NM,
  CASE
    WHEN a12.CAMERCHBUYERID IS NULL THEN a14.MERCHDIRECTORID
    ELSE a18.CAMERCHDIRECTORID
  END AS CA_DIRECTOR_ID,
  CASE
    WHEN a12.CAMERCHBUYERID IS NULL THEN a15.MERCHDIRECTORNAME
    ELSE a110.CAMERCHDIRECTORNAME
  END AS CA_DIRECTOR_NM,
  a12.MERCHDEPTCD AS SAP_DEPT_ID,
  a12.MERCHDEPTDESC AS SAP_DEPT_DESC,
  a14.MERCHDIRECTORID AS DIRECTOR_ID,
  a15.MERCHDIRECTORNAME AS DIRECTOR_NM,
  a19.MERCHGROUPID AS GROUP_ID,
  a111.MERCHGROUPDESC AS GROUP_DESC,
  a12.MERCHSEGMENTID AS SEGMENT_ID,
  a19.MERCHSEGMENTDESC AS SEGMENT_DESC,
  a16.MERCHSVPID AS SVP_ID,
  a17.MERCHSVPDESC AS SVP_NM,
  a15.MERCHVPID AS VP_ID,
  a16.MERCHDESC AS VP_DESC,
  a16.MERCHVPNAME AS VP_NM,
  a40.MERCHVPID AS CA_VP_ID,
  a40.MERCHVPNAME AS CA_VP_NM,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  MERCHDEPT a12
  LEFT JOIN MERCHBUYER a14 ON (a12.MERCHBUYERID = a14.MERCHBUYERID)
  JOIN MERCHDIRECTOR a15 ON (a14.MERCHDIRECTORID = a15.MERCHDIRECTORID)
  JOIN MERCHVP a16 ON (a15.MERCHVPID = a16.MERCHVPID)
  JOIN MERCHSVP a17 ON (a16.MERCHSVPID = a17.MERCHSVPID)
  LEFT OUTER JOIN CAMERCHBUYER a18 ON (a12.CAMERCHBUYERID = a18.CAMERCHBUYERID)
  LEFT OUTER JOIN MERCHSEGMENT a19 ON (a12.MERCHSEGMENTID = a19.MERCHSEGMENTID)
  LEFT OUTER JOIN CAMERCHDIRECTOR a110 ON (a18.CAMERCHDIRECTORID = a110.CAMERCHDIRECTORID)
  LEFT OUTER JOIN MERCHVP a40 ON (a40.MERCHVPID = a110.CAMERCHVPID)
  LEFT OUTER JOIN MERCHGROUP a111 ON (a19.MERCHGROUPID = a111.MERCHGROUPID)
  JOIN MERCHBUSINESSUNIT a112 ON (a17.MERCHBUSINESSUNITID = a112.MERCHBUSINESSUNITID)
ORDER BY
  a12.MERCHDEPTCD"""

df_10 = spark.sql(query_10)

df_10.createOrReplaceTempView("SQ_Shortcut_to_MerchDept_10")

# COMMAND ----------
# DBTITLE 1, EXP_CA_FLG_11


query_11 = f"""SELECT
  BUS_UNIT_ID AS BUS_UNIT_ID,
  BUS_UNIT_DESC AS BUS_UNIT_DESC,
  BUYER_ID AS BUYER_ID,
  BUYER_NM AS BUYER_NM,
  CA_BUYER_ID AS CA_BUYER_ID,
  CA_BUYER_NM AS CA_BUYER_NM,
  CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
  CA_DIRECTOR_NM AS CA_DIRECTOR_NM,
  IFF(BUYER_ID <> CA_BUYER_ID, 'Y', 'N') AS CA_MANAGED_FLG,
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DEPT_DESC AS SAP_DEPT_DESC,
  DIRECTOR_ID AS DIRECTOR_ID,
  DIRECTOR_NM AS DIRECTOR_NM,
  GROUP_ID AS GROUP_ID,
  GROUP_DESC AS GROUP_DESC,
  SEGMENT_ID AS SEGMENT_ID,
  SEGMENT_DESC AS SEGMENT_DESC,
  SVP_ID AS SVP_ID,
  SVP_NM AS SVP_NM,
  VP_ID AS VP_ID,
  VP_DESC AS VP_DESC,
  VP_NM AS VP_NM,
  CA_VP_ID AS CA_VP_ID,
  CA_VP_NM AS CA_VP_NM,
  now() AS LOAD_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_MerchDept_10"""

df_11 = spark.sql(query_11)

df_11.createOrReplaceTempView("EXP_CA_FLG_11")

# COMMAND ----------
# DBTITLE 1, MERCHDEPT_ORG


spark.sql("""INSERT INTO
  MERCHDEPT_ORG
SELECT
  SAP_DEPT_ID AS SAP_DEPT_ID,
  SAP_DEPT_DESC AS SAP_DEPT_DESC,
  BUS_UNIT_ID AS BUS_UNIT_ID,
  BUS_UNIT_DESC AS BUS_UNIT_DESC,
  BUYER_ID AS BUYER_ID,
  BUYER_NM AS BUYER_NM,
  CA_BUYER_ID AS CA_BUYER_ID,
  CA_BUYER_NM AS CA_BUYER_NM,
  CA_DIRECTOR_ID AS CA_DIRECTOR_ID,
  CA_DIRECTOR_NM AS CA_DIRECTOR_NM,
  CA_MANAGED_FLG AS CA_MANAGED_FLG,
  DIRECTOR_ID AS DIRECTOR_ID,
  DIRECTOR_NM AS DIRECTOR_NM,
  GROUP_ID AS GROUP_ID,
  GROUP_DESC AS GROUP_DESC,
  NULL AS PRICING_ROLE_ID,
  NULL AS PRICING_ROLE_DESC,
  SEGMENT_ID AS SEGMENT_ID,
  SEGMENT_DESC AS SEGMENT_DESC,
  SVP_ID AS SVP_ID,
  SVP_NM AS SVP_NM,
  VP_ID AS VP_ID,
  VP_DESC AS VP_DESC,
  VP_NM AS VP_NM,
  CA_VP_ID AS CA_VP_ID,
  CA_VP_NM AS CA_VP_NM,
  LOAD_DT AS LOAD_DT
FROM
  EXP_CA_FLG_11""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_merchdept_org")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_merchdept_org", mainWorkflowId, parentName)
