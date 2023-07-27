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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_PIM_Attr_Type_Values_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SKU_PIM_Attr_Type_Values_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PIMWebstyle_0


query_0 = f"""SELECT
  PIMWebstyle AS PIMWebstyle,
  PIMProductTitle AS PIMProductTitle,
  CopyRequiredFlg AS CopyRequiredFlg,
  SpecificationReqdFlg AS SpecificationReqdFlg,
  Keywords AS Keywords,
  ShortDesc AS ShortDesc,
  ImageReqFlg AS ImageReqFlg,
  ProductTitleStatus AS ProductTitleStatus,
  SliceAttrId1 AS SliceAttrId1,
  SliceAttrId2 AS SliceAttrId2,
  DelInd AS DelInd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  PIMWebstyle"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PIMWebstyle_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PIMWebstyle_1


query_1 = f"""SELECT
  PIMWebstyle AS PIMWebstyle,
  PIMProductTitle AS PIMProductTitle,
  DelInd AS DelInd,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PIMWebstyle_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PIMWebstyle_1")

# COMMAND ----------
# DBTITLE 1, Exp_WebStyle_2


query_2 = f"""SELECT
  0 AS PIM_ATTR_ID2,
  PIMWebstyle AS PIMWebstyle,
  PIMProductTitle AS PIMProductTitle,
  NULL AS ENTRY_POSITION2,
  DelInd AS DelInd,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PIMWebstyle_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Exp_WebStyle_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PIMAttrValues_3


query_3 = f"""SELECT
  PIMAttrID AS PIMAttrID,
  PIMAttrValID AS PIMAttrValID,
  PIMAttrValDesc AS PIMAttrValDesc,
  EntryPosition AS EntryPosition,
  DelInd AS DelInd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  PIMAttrValues"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_PIMAttrValues_3")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PIMAttrValues_4


query_4 = f"""SELECT
  PIMAttrID AS PIMAttrID,
  PIMAttrValID AS PIMAttrValID,
  PIMAttrValDesc AS PIMAttrValDesc,
  EntryPosition AS EntryPosition,
  DelInd AS DelInd,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PIMAttrValues_3"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("SQ_Shortcut_to_PIMAttrValues_4")

# COMMAND ----------
# DBTITLE 1, Union_5


query_5 = f"""SELECT
  PIMAttrID AS PIM_ATTR_ID,
  PIMAttrValID AS PIM_ATTR_VAL_ID,
  PIMAttrValDesc AS PIM_ATTR_VAL_DESC,
  EntryPosition AS ENTRY_POSITION,
  DelInd AS DEL_IND,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PIMAttrValues_4
UNION ALL
SELECT
  PIM_ATTR_ID2 AS PIM_ATTR_ID,
  PIMWebstyle AS PIM_ATTR_VAL_ID,
  PIMProductTitle AS PIM_ATTR_VAL_DESC,
  ENTRY_POSITION2 AS ENTRY_POSITION,
  DelInd AS DEL_IND,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_WebStyle_2"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("Union_5")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_PIM_Attr_Values_Pre_6


query_6 = f"""SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  DEL_IND AS DEL_IND,
  sysdate AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Union_5"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Exp_SKU_PIM_Attr_Values_Pre_6")

# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR_TYPE_VALUES_PRE


spark.sql("""INSERT INTO
  SKU_PIM_ATTR_TYPE_VALUES_PRE
SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_VAL_ID AS PIM_ATTR_VAL_ID,
  PIM_ATTR_VAL_DESC AS PIM_ATTR_VAL_DESC,
  ENTRY_POSITION AS ENTRY_POSITION,
  DEL_IND AS DEL_IND,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  Exp_SKU_PIM_Attr_Values_Pre_6""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_PIM_Attr_Type_Values_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SKU_PIM_Attr_Type_Values_Pre", mainWorkflowId, parentName)
