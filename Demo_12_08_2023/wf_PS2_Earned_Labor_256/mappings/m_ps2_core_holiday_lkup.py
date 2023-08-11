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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_core_holiday_lkup")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_core_holiday_lkup", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_UDH_PS2_CORE_HOLIDAY_LKUP_0


query_0 = f"""SELECT
  DAY_DT AS DAY_DT,
  DEFAULT_STORE_FLAG AS DEFAULT_STORE_FLAG,
  STORE_NBR AS STORE_NBR,
  OT_TYPE AS OT_TYPE,
  COUNTRY_CD AS COUNTRY_CD
FROM
  UDH_PS2_CORE_HOLIDAY_LKUP"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_UDH_PS2_CORE_HOLIDAY_LKUP_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_UDH_PS2_CORE_HOLIDAY_LKUP_1


query_1 = f"""SELECT
  DAY_DT AS DAY_DT,
  DEFAULT_STORE_FLAG AS DEFAULT_STORE_FLAG,
  STORE_NBR AS STORE_NBR,
  OT_TYPE AS OT_TYPE,
  COUNTRY_CD AS COUNTRY_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_UDH_PS2_CORE_HOLIDAY_LKUP_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_UDH_PS2_CORE_HOLIDAY_LKUP_1")

# COMMAND ----------
# DBTITLE 1, LKP_SITE_PROFILE_PS2_CORE_HOLIDAY_2


query_2 = f"""SELECT
  SP.LOCATION_ID AS LOCATION_ID,
  SP.LOCATION_TYPE_ID AS LOCATION_TYPE_ID,
  SP.STORE_NBR AS STORE_NBR,
  SP.COUNTRY_CD AS COUNTRY_CD,
  SStUPCHL1.STORE_NBR AS in_STORE_NBR,
  SStUPCHL1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_PS2_CORE_HOLIDAY_LKUP_1 SStUPCHL1
  LEFT JOIN SITE_PROFILE SP ON SP.STORE_NBR = SStUPCHL1.STORE_NBR"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("LKP_SITE_PROFILE_PS2_CORE_HOLIDAY_2")

# COMMAND ----------
# DBTITLE 1, EXP_DEFAULT_LOCATION_ID_3


query_3 = f"""SELECT
  SStUPCHL1.DAY_DT AS DAY_DT,
  SStUPCHL1.DEFAULT_STORE_FLAG AS DEFAULT_STORE_FLAG,
  IFF(
    ISNULL(LSPPCH2.LOCATION_ID),
    0,
    LSPPCH2.LOCATION_ID
  ) AS LOCATION_ID,
  SStUPCHL1.OT_TYPE AS OT_TYPE,
  SStUPCHL1.COUNTRY_CD AS COUNTRY_CD,
  SStUPCHL1.STORE_NBR AS STORE_NBR,
  SStUPCHL1.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_UDH_PS2_CORE_HOLIDAY_LKUP_1 SStUPCHL1
  INNER JOIN LKP_SITE_PROFILE_PS2_CORE_HOLIDAY_2 LSPPCH2 ON SStUPCHL1.Monotonically_Increasing_Id = LSPPCH2.Monotonically_Increasing_Id"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_DEFAULT_LOCATION_ID_3")

# COMMAND ----------
# DBTITLE 1, PS2_CORE_HOLIDAY_LKUP


spark.sql("""INSERT INTO
  PS2_CORE_HOLIDAY_LKUP
SELECT
  DAY_DT AS DAY_DT,
  DEFAULT_STORE_FLAG AS DEFAULT_STORE_FLAG,
  LOCATION_ID AS LOCATION_ID,
  OT_TYPE AS OT_TYPE,
  COUNTRY_CD AS COUNTRY_CD,
  STORE_NBR AS STORE_NBR
FROM
  EXP_DEFAULT_LOCATION_ID_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_core_holiday_lkup")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_core_holiday_lkup", mainWorkflowId, parentName)
