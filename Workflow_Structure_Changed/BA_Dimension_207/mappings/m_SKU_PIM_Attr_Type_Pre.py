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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_PIM_Attr_Type_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SKU_PIM_Attr_Type_Pre", variablesTableName, mainWorkflowId)

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
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PIMWebstyle_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PIMWebstyle_1")

# COMMAND ----------
# DBTITLE 1, Agg_WebStyle_2


query_2 = f"""SELECT
  MAX(PIMWebstyle) AS PIMWebstyle1,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PIMWebstyle_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Agg_WebStyle_2")

# COMMAND ----------
# DBTITLE 1, Exp_WebStyle_3


query_3 = f"""SELECT
  0 AS PIM_ATTR_ID,
  'Web Style' AS PIM_ATTR_TAG,
  'Web Style' AS PIM_ATTR_NAME,
  'Web Style' AS PIM_ATTR_DISPLAY_NAME,
  NULL AS SLICING_ATTR_IND2,
  NULL AS MULTI_VAL_ASSIGN_IND2,
  NULL AS INTERNAL_AUD_IND2,
  NULL AS SIZE_ATTR_IND2,
  NULL AS DelInd,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Agg_WebStyle_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Exp_WebStyle_3")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PIMAttribute_4


query_4 = f"""SELECT
  PIMAttrID AS PIMAttrID,
  PIMAttrTag AS PIMAttrTag,
  PIMAttrName AS PIMAttrName,
  PIMAttrDisplayName AS PIMAttrDisplayName,
  SlicingAttrInd AS SlicingAttrInd,
  MultiValAssignInd AS MultiValAssignInd,
  InternalAudInd AS InternalAudInd,
  SizeAttrInd AS SizeAttrInd,
  DelInd AS DelInd,
  LoadTstmp AS LoadTstmp,
  UpdateTstmp AS UpdateTstmp
FROM
  PIMAttribute"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("Shortcut_to_PIMAttribute_4")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PIMAttribute_5


query_5 = f"""SELECT
  PIMAttrID AS PIMAttrID,
  PIMAttrTag AS PIMAttrTag,
  PIMAttrName AS PIMAttrName,
  PIMAttrDisplayName AS PIMAttrDisplayName,
  SlicingAttrInd AS SlicingAttrInd,
  MultiValAssignInd AS MultiValAssignInd,
  InternalAudInd AS InternalAudInd,
  SizeAttrInd AS SizeAttrInd,
  DelInd AS DelInd,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PIMAttribute_4"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("SQ_Shortcut_to_PIMAttribute_5")

# COMMAND ----------
# DBTITLE 1, Union_6


query_6 = f"""SELECT
  PIMAttrID AS PIM_ATTR_ID,
  PIMAttrTag AS PIM_ATTR_TAG,
  PIMAttrName AS PIM_ATTR_NAME,
  PIMAttrDisplayName AS PIM_ATTR_DISPLAY_NAME,
  SlicingAttrInd AS SLICING_ATTR_IND,
  MultiValAssignInd AS MULTI_VAL_ASSIGN_IND,
  InternalAudInd AS INTERNAL_AUD_IND,
  SizeAttrInd AS SIZE_ATTR_IND,
  DelInd AS DEL_IND,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PIMAttribute_5
UNION ALL
SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_TAG AS PIM_ATTR_TAG,
  PIM_ATTR_NAME AS PIM_ATTR_NAME,
  PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
  SLICING_ATTR_IND2 AS SLICING_ATTR_IND,
  MULTI_VAL_ASSIGN_IND2 AS MULTI_VAL_ASSIGN_IND,
  INTERNAL_AUD_IND2 AS INTERNAL_AUD_IND,
  SIZE_ATTR_IND2 AS SIZE_ATTR_IND,
  DelInd AS DEL_IND,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Exp_WebStyle_3"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("Union_6")

# COMMAND ----------
# DBTITLE 1, Exp_SKU_PIM_Attr_Type_Pre_7


query_7 = f"""SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_TAG AS PIM_ATTR_TAG,
  PIM_ATTR_NAME AS PIM_ATTR_NAME,
  PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
  SLICING_ATTR_IND AS SLICING_ATTR_IND,
  MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
  INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
  SIZE_ATTR_IND AS SIZE_ATTR_IND,
  DEL_IND AS DEL_IND,
  sysdate AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  Union_6"""

df_7 = spark.sql(query_7)

df_7.createOrReplaceTempView("Exp_SKU_PIM_Attr_Type_Pre_7")

# COMMAND ----------
# DBTITLE 1, SKU_PIM_ATTR_TYPE_PRE


spark.sql("""INSERT INTO
  SKU_PIM_ATTR_TYPE_PRE
SELECT
  PIM_ATTR_ID AS PIM_ATTR_ID,
  PIM_ATTR_TAG AS PIM_ATTR_TAG,
  PIM_ATTR_NAME AS PIM_ATTR_NAME,
  PIM_ATTR_DISPLAY_NAME AS PIM_ATTR_DISPLAY_NAME,
  SLICING_ATTR_IND AS SLICING_ATTR_IND,
  MULTI_VAL_ASSIGN_IND AS MULTI_VAL_ASSIGN_IND,
  INTERNAL_AUD_IND AS INTERNAL_AUD_IND,
  SIZE_ATTR_IND AS SIZE_ATTR_IND,
  DEL_IND AS DEL_IND,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  Exp_SKU_PIM_Attr_Type_Pre_7""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_PIM_Attr_Type_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SKU_PIM_Attr_Type_Pre", mainWorkflowId, parentName)
