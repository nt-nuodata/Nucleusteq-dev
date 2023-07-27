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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_hierarchy")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sap_hierarchy", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_SAP_HIERARCHY_VIEW_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  HL5_CODE_ID AS HL5_CODE_ID,
  HL5_CODE_DESC AS HL5_CODE_DESC,
  HL5_VALUE_ID AS HL5_VALUE_ID,
  HL5_VALUE_DESC AS HL5_VALUE_DESC,
  HL6_CODE_ID AS HL6_CODE_ID,
  HL6_CODE_DESC AS HL6_CODE_DESC,
  HL6_VALUE_ID AS HL6_VALUE_ID,
  HL6_VALUE_DESC AS HL6_VALUE_DESC,
  HL7_CODE_ID AS HL7_CODE_ID,
  HL7_CODE_DESC AS HL7_CODE_DESC,
  HL7_VALUE_ID AS HL7_VALUE_ID,
  HL7_VALUE_DESC AS HL7_VALUE_DESC,
  HL8_CODE_ID AS HL8_CODE_ID,
  HL8_CODE_DESC AS HL8_CODE_DESC,
  HL8_VALUE_ID AS HL8_VALUE_ID,
  HL8_VALUE_DESC AS HL8_VALUE_DESC,
  PLG_CODE_ID AS PLG_CODE_ID,
  PLG_CODE_DESC AS PLG_CODE_DESC,
  PLG_VALUE_ID AS PLG_VALUE_ID,
  PLG_VALUE_DESC AS PLG_VALUE_DESC
FROM
  SAP_HIERARCHY_VIEW"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_SAP_HIERARCHY_VIEW_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_to_SAP_HIERARCHY_VIEW_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  HL5_CODE_ID AS HL5_CODE_ID,
  HL5_CODE_DESC AS HL5_CODE_DESC,
  HL5_VALUE_ID AS HL5_VALUE_ID,
  HL5_VALUE_DESC AS HL5_VALUE_DESC,
  HL6_CODE_ID AS HL6_CODE_ID,
  HL6_CODE_DESC AS HL6_CODE_DESC,
  HL6_VALUE_ID AS HL6_VALUE_ID,
  HL6_VALUE_DESC AS HL6_VALUE_DESC,
  HL7_CODE_ID AS HL7_CODE_ID,
  HL7_CODE_DESC AS HL7_CODE_DESC,
  HL7_VALUE_ID AS HL7_VALUE_ID,
  HL7_VALUE_DESC AS HL7_VALUE_DESC,
  HL8_CODE_ID AS HL8_CODE_ID,
  HL8_CODE_DESC AS HL8_CODE_DESC,
  HL8_VALUE_ID AS HL8_VALUE_ID,
  HL8_VALUE_DESC AS HL8_VALUE_DESC,
  PLG_CODE_ID AS PLG_CODE_ID,
  PLG_CODE_DESC AS PLG_CODE_DESC,
  PLG_VALUE_ID AS PLG_VALUE_ID,
  PLG_VALUE_DESC AS PLG_VALUE_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_SAP_HIERARCHY_VIEW_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_to_SAP_HIERARCHY_VIEW_1")

# COMMAND ----------
# DBTITLE 1, FIL_ZERO_PRODUCT_ID_2


query_2 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  HL5_CODE_ID AS HL5_CODE_ID,
  HL5_CODE_DESC AS HL5_CODE_DESC,
  HL5_VALUE_ID AS HL5_VALUE_ID,
  HL5_VALUE_DESC AS HL5_VALUE_DESC,
  HL6_CODE_ID AS HL6_CODE_ID,
  HL6_CODE_DESC AS HL6_CODE_DESC,
  HL6_VALUE_ID AS HL6_VALUE_ID,
  HL6_VALUE_DESC AS HL6_VALUE_DESC,
  HL7_CODE_ID AS HL7_CODE_ID,
  HL7_CODE_DESC AS HL7_CODE_DESC,
  HL7_VALUE_ID AS HL7_VALUE_ID,
  HL7_VALUE_DESC AS HL7_VALUE_DESC,
  HL8_CODE_ID AS HL8_CODE_ID,
  HL8_CODE_DESC AS HL8_CODE_DESC,
  HL8_VALUE_ID AS HL8_VALUE_ID,
  HL8_VALUE_DESC AS HL8_VALUE_DESC,
  PLG_CODE_ID AS PLG_CODE_ID,
  PLG_CODE_DESC AS PLG_CODE_DESC,
  PLG_VALUE_ID AS PLG_VALUE_ID,
  PLG_VALUE_DESC AS PLG_VALUE_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_to_SAP_HIERARCHY_VIEW_1
WHERE
  PRODUCT_ID != 0"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("FIL_ZERO_PRODUCT_ID_2")

# COMMAND ----------
# DBTITLE 1, EXP_HIERARCHY_3


query_3 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  HL5_CODE_ID AS HL5_CODE_ID,
  HL5_CODE_DESC AS HL5_CODE_DESC,
  HL5_VALUE_ID AS HL5_VALUE_ID,
  HL5_VALUE_DESC AS HL5_VALUE_DESC,
  HL6_CODE_ID AS HL6_CODE_ID,
  HL6_CODE_DESC AS HL6_CODE_DESC,
  HL6_VALUE_ID AS HL6_VALUE_ID,
  HL6_VALUE_DESC AS HL6_VALUE_DESC,
  HL7_CODE_ID AS HL7_CODE_ID,
  HL7_CODE_DESC AS HL7_CODE_DESC,
  HL7_VALUE_ID AS HL7_VALUE_ID,
  HL7_VALUE_DESC AS HL7_VALUE_DESC,
  HL8_CODE_ID AS HL8_CODE_ID,
  HL8_CODE_DESC AS HL8_CODE_DESC,
  HL8_VALUE_ID AS HL8_VALUE_ID,
  HL8_VALUE_DESC AS HL8_VALUE_DESC,
  PLG_CODE_ID AS PLG_CODE_ID,
  PLG_CODE_DESC AS PLG_CODE_DESC,
  PLG_VALUE_ID AS PLG_VALUE_ID,
  PLG_VALUE_DESC AS PLG_VALUE_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  FIL_ZERO_PRODUCT_ID_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_HIERARCHY_3")

# COMMAND ----------
# DBTITLE 1, SAP_HIERARCHY


spark.sql("""INSERT INTO
  SAP_HIERARCHY
SELECT
  PRODUCT_ID AS PRODUCT_ID,
  HL5_CODE_ID AS HL5_CODE_ID,
  HL5_CODE_DESC AS HL5_CODE_DESC,
  HL5_VALUE_ID AS HL5_VALUE_ID,
  HL5_VALUE_DESC AS HL5_VALUE_DESC,
  HL6_CODE_ID AS HL6_CODE_ID,
  HL6_CODE_DESC AS HL6_CODE_DESC,
  HL6_VALUE_ID AS HL6_VALUE_ID,
  HL6_VALUE_DESC AS HL6_VALUE_DESC,
  HL7_CODE_ID AS HL7_CODE_ID,
  HL7_CODE_DESC AS HL7_CODE_DESC,
  HL7_VALUE_ID AS HL7_VALUE_ID,
  HL7_VALUE_DESC AS HL7_VALUE_DESC,
  HL8_CODE_ID AS HL8_CODE_ID,
  HL8_CODE_DESC AS HL8_CODE_DESC,
  HL8_VALUE_ID AS HL8_VALUE_ID,
  HL8_VALUE_DESC AS HL8_VALUE_DESC,
  PLG_CODE_ID AS PLAN_GROUP_CODE_ID,
  PLG_CODE_DESC AS PLAN_GROUP_CODE_DESC,
  PLG_VALUE_ID AS PLAN_GROUP_VALUE_ID,
  PLG_VALUE_DESC AS PLAN_GROUP_VALUE_DESC
FROM
  EXP_HIERARCHY_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sap_hierarchy")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sap_hierarchy", mainWorkflowId, parentName)
