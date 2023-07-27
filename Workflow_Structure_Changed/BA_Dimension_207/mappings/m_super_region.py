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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_super_region")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_super_region", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_REGION_0


query_0 = f"""SELECT
  REGION_ID AS REGION_ID,
  REGION_DESC AS REGION_DESC
FROM
  REGION"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_REGION_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_REGION_1


query_1 = f"""SELECT
  DISTINCT REGION_ID AS REGION_ID,
  REGION_DESC AS REGION_DESC,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_REGION_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_REGION_1")

# COMMAND ----------
# DBTITLE 1, EXP_SUPER_REGION_2


query_2 = f"""SELECT
  IFF(
    IN(REGION_ID, 200, 300, 850, 400),
    1,
    IFF(
      IN(REGION_ID, 775, 800, 950, 500),
      4,
      IFF(IN(REGION_ID, 650), 5, REGION_ID)
    )
  ) AS SUPER_REGION_ID,
  IFF(
    IN(REGION_ID, 200, 300, 850, 400),
    'West',
    IFF(
      IN(REGION_ID, 775, 800, 950, 500),
      'East',
      IFF(
        IN(REGION_ID, 700, 750, 600, 825),
        'NA',
        IFF(IN(REGION_ID, 650), 'Canada', REGION_DESC)
      )
    )
  ) AS SUPER_REGION_DESC,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_REGION_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_SUPER_REGION_2")

# COMMAND ----------
# DBTITLE 1, AGG_SUPER_REGION_3


query_3 = f"""SELECT
  SUPER_REGION_ID AS SUPER_REGION_ID,
  SUPER_REGION_DESC AS SUPER_REGION_DESC,
  last(Monotonically_Increasing_Id) AS Monotonically_Increasing_Id
FROM
  EXP_SUPER_REGION_2
GROUP BY
  SUPER_REGION_ID,
  SUPER_REGION_DESC"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("AGG_SUPER_REGION_3")

# COMMAND ----------
# DBTITLE 1, SUPER_REGION


spark.sql("""INSERT INTO
  SUPER_REGION
SELECT
  SUPER_REGION_ID AS SUPER_REGION_ID,
  SUPER_REGION_DESC AS SUPER_REGION_DESC
FROM
  AGG_SUPER_REGION_3""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_super_region")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_super_region", mainWorkflowId, parentName)
