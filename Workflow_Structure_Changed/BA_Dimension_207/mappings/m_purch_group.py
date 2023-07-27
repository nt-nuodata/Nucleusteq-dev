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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_purch_group")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_purch_group", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_T024_0


query_0 = f"""SELECT
  MANDT AS MANDT,
  EKGRP AS EKGRP,
  EKNAM AS EKNAM,
  EKTEL AS EKTEL,
  LDEST AS LDEST,
  TELFX AS TELFX,
  ZZBNAME AS ZZBNAME
FROM
  T024"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_T024_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_T024_1


query_1 = f"""SELECT
  DISTINCT EKGRP,
  EKNAM
FROM
  SAPPR3.Shortcut_to_T024_0
WHERE
  MANDT = 100"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_T024_1")

# COMMAND ----------
# DBTITLE 1, FIL_PURCH_GROUP_ID_2


query_2 = f"""SELECT
  EKGRP AS EKGRP,
  EKNAM AS EKNAM,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_T024_1
WHERE
  IS_NUMBER(EKGRP)"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("FIL_PURCH_GROUP_ID_2")

# COMMAND ----------
# DBTITLE 1, PURCH_GROUP


spark.sql("""INSERT INTO
  PURCH_GROUP
SELECT
  EKGRP AS PURCH_GROUP_ID,
  EKNAM AS PURCH_GROUP_NAME
FROM
  FIL_PURCH_GROUP_ID_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_purch_group")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_purch_group", mainWorkflowId, parentName)
