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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_occupancy_data_default")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_htl_occupancy_data_default", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UDH_PS2_HTL_OCCUPANCY_DATA_DEFAULT_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  OVERNIGHT_KITTY_GUEST AS OVERNIGHT_KITTY_GUEST,
  OVERNIGHT_DOG_GUEST AS OVERNIGHT_DOG_GUEST,
  OVERNIGHT_WITH_DDC AS OVERNIGHT_WITH_DDC,
  DAY_CAMP_CNT AS DAY_CAMP_CNT,
  DAY_CARE_CNT AS DAY_CARE_CNT
FROM
  UDH_PS2_HTL_OCCUPANCY_DATA_DEFAULT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_UDH_PS2_HTL_OCCUPANCY_DATA_DEFAULT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UDH_PS2_HTL_OCCUPANCY_DATA_DEFAULT_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  OVERNIGHT_KITTY_GUEST AS OVERNIGHT_KITTY_GUEST,
  OVERNIGHT_DOG_GUEST AS OVERNIGHT_DOG_GUEST,
  OVERNIGHT_WITH_DDC AS OVERNIGHT_WITH_DDC,
  DAY_CAMP_CNT AS DAY_CAMP_CNT,
  DAY_CARE_CNT AS DAY_CARE_CNT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_UDH_PS2_HTL_OCCUPANCY_DATA_DEFAULT_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_PS2_HTL_OCCUPANCY_DATA_DEFAULT_1")

# COMMAND ----------
# DBTITLE 1, EXP_DEFAULT_2


query_2 = f"""SELECT
  TO_DATE(DAY_DT, 'YYYY-MM-DD') AS DAY_DT,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  OVERNIGHT_KITTY_GUEST AS OVERNIGHT_KITTY_GUEST,
  OVERNIGHT_DOG_GUEST AS OVERNIGHT_DOG_GUEST,
  OVERNIGHT_WITH_DDC AS OVERNIGHT_WITH_DDC,
  DAY_CAMP_CNT AS DAY_CAMP_CNT,
  DAY_CARE_CNT AS DAY_CARE_CNT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_PS2_HTL_OCCUPANCY_DATA_DEFAULT_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_DEFAULT_2")

# COMMAND ----------
# DBTITLE 1, PS2_HTL_OCCUPANCY_DATA_DEFAULT


spark.sql("""INSERT INTO
  PS2_HTL_OCCUPANCY_DATA_DEFAULT
SELECT
  DAY_DT AS DAY_DT,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  OVERNIGHT_KITTY_GUEST AS OVERNIGHT_KITTY_GUEST,
  OVERNIGHT_DOG_GUEST AS OVERNIGHT_DOG_GUEST,
  OVERNIGHT_WITH_DDC AS OVERNIGHT_WITH_DDC,
  DAY_CAMP_CNT AS DAY_CAMP_CNT,
  DAY_CARE_CNT AS DAY_CARE_CNT
FROM
  EXP_DEFAULT_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_occupancy_data_default")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_htl_occupancy_data_default", mainWorkflowId, parentName)
