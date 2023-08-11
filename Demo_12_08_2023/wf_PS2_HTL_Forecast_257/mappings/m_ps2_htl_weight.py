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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_weight")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_htl_weight", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UDH_PS2_HTL_WEIGHT_0


query_0 = f"""SELECT
  WEEK_NBR AS WEEK_NBR,
  WEIGHT_QTY AS WEIGHT_QTY,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  UDH_PS2_HTL_WEIGHT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_UDH_PS2_HTL_WEIGHT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UDH_PS2_HTL_WEIGHT_1


query_1 = f"""SELECT
  WEEK_NBR AS WEEK_NBR,
  WEIGHT_QTY AS WEIGHT_QTY,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_UDH_PS2_HTL_WEIGHT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_PS2_HTL_WEIGHT_1")

# COMMAND ----------
# DBTITLE 1, EXP_Weight_2


query_2 = f"""SELECT
  WEEK_NBR AS WEEK_NBR,
  WEIGHT_QTY AS WEIGHT_QTY,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_PS2_HTL_WEIGHT_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_Weight_2")

# COMMAND ----------
# DBTITLE 1, PS2_HTL_WEIGHT


spark.sql("""INSERT INTO
  PS2_HTL_WEIGHT
SELECT
  WEEK_NBR AS WEEK_NBR,
  WEIGHT_QTY AS WEIGHT_QTY,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_Weight_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_weight")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_htl_weight", mainWorkflowId, parentName)
