# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ../WorkflowUtility

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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Facility_Alias_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_DDS_OMS_Facility_Alias_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_FACILITY_ALIAS_0


query_0 = f"""SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ALIAS_ID_U AS FACILITY_ALIAS_ID_U,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  IS_PRIMARY AS IS_PRIMARY,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_SOURCE_TYPE AS CREATED_SOURCE_TYPE,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE AS LAST_UPDATED_SOURCE_TYPE,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  TC_SHIPMENT_ID_PREFIX AS TC_SHIPMENT_ID_PREFIX,
  FACILITY_SEQ AS FACILITY_SEQ,
  ADDRESS_1 AS ADDRESS_1,
  CITY AS CITY,
  STATE_PROV AS STATE_PROV,
  POSTAL_CODE AS POSTAL_CODE,
  COUNTY AS COUNTY,
  COUNTRY_CODE AS COUNTRY_CODE,
  CARRIER_ID AS CARRIER_ID,
  MOT_ID AS MOT_ID,
  ADDRESS_2 AS ADDRESS_2,
  ADDRESS_3 AS ADDRESS_3,
  AUDIT_TRANSACTION AS AUDIT_TRANSACTION,
  AUDIT_PARTY_ID AS AUDIT_PARTY_ID,
  HIBERNATE_VERSION AS HIBERNATE_VERSION
FROM
  FACILITY_ALIAS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_FACILITY_ALIAS_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_FACILITY_ALIAS_1


query_1 = f"""SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_FACILITY_ALIAS_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_FACILITY_ALIAS_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  now() AS o_LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_FACILITY_ALIAS_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, OMS_FACILITY_ALIAS_PRE


spark.sql("""INSERT INTO
  OMS_FACILITY_ALIAS_PRE
SELECT
  FACILITY_ALIAS_ID AS FACILITY_ALIAS_ID,
  FACILITY_ID AS FACILITY_ID,
  TC_COMPANY_ID AS TC_COMPANY_ID,
  FACILITY_NAME AS FACILITY_NAME,
  MARK_FOR_DELETION AS MARK_FOR_DELETION,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  o_LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_DDS_OMS_Facility_Alias_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_DDS_OMS_Facility_Alias_Pre", mainWorkflowId, parentName)
