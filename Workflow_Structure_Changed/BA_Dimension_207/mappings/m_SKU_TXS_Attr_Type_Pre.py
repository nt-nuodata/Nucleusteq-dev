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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_TXS_Attr_Type_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_SKU_TXS_Attr_Type_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SKUAttrType_0


query_0 = f"""SELECT
  SKUAttrTypeID AS SKUAttrTypeID,
  SKUAttrTypeDesc AS SKUAttrTypeDesc,
  SKUAttrOwner AS SKUAttrOwner,
  DelInd AS DelInd,
  UpdateUser AS UpdateUser,
  UpdateDt AS UpdateDt,
  LoadUser AS LoadUser,
  LoadDt AS LoadDt
FROM
  SKUAttrType"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SKUAttrType_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_SKUAttrType_1


query_1 = f"""SELECT
  SKUAttrTypeID AS SKUAttrTypeID,
  SKUAttrTypeDesc AS SKUAttrTypeDesc,
  SKUAttrOwner AS SKUAttrOwner,
  DelInd AS DelInd,
  UpdateUser AS UpdateUser,
  LoadUser AS LoadUser,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SKUAttrType_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_SKUAttrType_1")

# COMMAND ----------
# DBTITLE 1, Exp_Load_Tstmp_2


query_2 = f"""SELECT
  LoadUser AS LoadUser,
  sysdate AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_SKUAttrType_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Exp_Load_Tstmp_2")

# COMMAND ----------
# DBTITLE 1, SKU_TXS_ATTR_TYPE_PRE


spark.sql("""INSERT INTO
  SKU_TXS_ATTR_TYPE_PRE
SELECT
  SStS1.SKUAttrTypeID AS SKU_TXS_ATTR_TYPE_ID,
  SStS1.SKUAttrTypeDesc AS SKU_TXS_ATTR_TYPE_DESC,
  SStS1.SKUAttrOwner AS SKU_TXS_ATTR_OWNER,
  SStS1.DelInd AS DEL_IND,
  SStS1.UpdateUser AS UPDATE_USER,
  ELT2.LoadUser AS LOAD_USER,
  ELT2.LOAD_TSTMP AS LOAD_TSTMP
FROM
  SQ_Shortcut_to_SKUAttrType_1 SStS1
  INNER JOIN Exp_Load_Tstmp_2 ELT2 ON SStS1.Monotonically_Increasing_Id = ELT2.Monotonically_Increasing_Id""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_SKU_TXS_Attr_Type_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_SKU_TXS_Attr_Type_Pre", mainWorkflowId, parentName)
