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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_blue_law_store_lkup")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_blue_law_store_lkup", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UDH_PS2_BLUE_LAW_STORE_LKUP_0


query_0 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  EFF_DT AS EFF_DT,
  END_DT AS END_DT
FROM
  UDH_PS2_BLUE_LAW_STORE_LKUP"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_UDH_PS2_BLUE_LAW_STORE_LKUP_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UDH_PS2_BLUE_LAW_STORE_LKUP_1


query_1 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  EFF_DT AS EFF_DT,
  END_DT AS END_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_UDH_PS2_BLUE_LAW_STORE_LKUP_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_PS2_BLUE_LAW_STORE_LKUP_1")

# COMMAND ----------
# DBTITLE 1, LKP_SITE_PROFILE_PS2_BLUE_LAW_2


query_2 = f"""SELECT
  SP.LOCATION_ID AS LOCATION_ID,
  SP.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SP.STORE_NBR AS STORE_NBR,
  SStUPBLSL1.STORE_NBR AS in_STORE_NBR,
  SStUPBLSL1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_PS2_BLUE_LAW_STORE_LKUP_1 SStUPBLSL1
  LEFT JOIN SITE_PROFILE SP ON SP.STORE_NBR = SStUPBLSL1.STORE_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKP_SITE_PROFILE_PS2_BLUE_LAW_2")

# COMMAND ----------
# DBTITLE 1, FIL_NULL_LOCATION_ID_3


query_3 = f"""SELECT
  LSPPBL2.LOCATION_ID AS LOCATION_ID,
  SStUPBLSL1.EFF_DT AS EFF_DT,
  SStUPBLSL1.END_DT AS END_DT,
  SStUPBLSL1.STORE_NBR AS STORE_NBR,
  SStUPBLSL1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_PS2_BLUE_LAW_STORE_LKUP_1 SStUPBLSL1
  INNER JOIN LKP_SITE_PROFILE_PS2_BLUE_LAW_2 LSPPBL2 ON SStUPBLSL1.Monotonically_Increasing_Id = LSPPBL2.Monotonically_Increasing_Id
WHERE
  NOT ISNULL(LSPPBL2.LOCATION_ID)"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("FIL_NULL_LOCATION_ID_3")

# COMMAND ----------
# DBTITLE 1, PS2_BLUE_LAW_STORE_LKUP


spark.sql("""INSERT INTO
  PS2_BLUE_LAW_STORE_LKUP
SELECT
  LOCATION_ID AS LOCATION_ID,
  EFF_DT AS EFF_DT,
  END_DT AS END_DT,
  STORE_NBR AS STORE_NBR
FROM
  FIL_NULL_LOCATION_ID_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_blue_law_store_lkup")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_blue_law_store_lkup", mainWorkflowId, parentName)
