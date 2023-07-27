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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_uom_pre_uom_cd")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_sku_uom_pre_uom_cd", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_SKU_UOM_PRE_0


query_0 = f"""SELECT
  SKU_NBR AS SKU_NBR,
  UOM_CD AS UOM_CD,
  DELETE_IND AS DELETE_IND,
  UOM_NUMERATOR AS UOM_NUMERATOR,
  UOM_DENOMINATOR AS UOM_DENOMINATOR,
  LENGTH_AMT AS LENGTH_AMT,
  WIDTH_AMT AS WIDTH_AMT,
  HEIGHT_AMT AS HEIGHT_AMT,
  DIMENSION_UNIT_DESC AS DIMENSION_UNIT_DESC,
  VOLUME_AMT AS VOLUME_AMT,
  VOLUME_UNIT_DESC AS VOLUME_UNIT_DESC,
  WEIGHT_GROSS_AMT AS WEIGHT_GROSS_AMT,
  WEIGHT_UNIT_DESC AS WEIGHT_UNIT_DESC
FROM
  SKU_UOM_PRE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_SKU_UOM_PRE_0")

# COMMAND ----------
# DBTITLE 1, ASQ_SHORTCUT_TO_UOM_CD_NOT_EXIST_1


query_1 = f"""SELECT
  DISTINCT SU.UOM_CD AS UOM_CD,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_SKU_UOM_PRE_0 SU
WHERE
  NOT EXISTS (
    SELECT
      UOM_CD
    FROM
      UOM
    WHERE
      UOM.UOM_CD = SU.UOM_CD
  )"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_SHORTCUT_TO_UOM_CD_NOT_EXIST_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2


query_2 = f"""SELECT
  UOM_CD AS UOM_CD,
  NULL AS NULL_STRING,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_SHORTCUT_TO_UOM_CD_NOT_EXIST_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, UOM


spark.sql("""INSERT INTO
  UOM
SELECT
  UOM_CD AS UOM_CD,
  UOM_CD AS UOM_CD,
  UOM_CD AS UOM_DESC,
  NULL_STRING AS UOM_DESC
FROM
  EXPTRANS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_sku_uom_pre_uom_cd")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_sku_uom_pre_uom_cd", mainWorkflowId, parentName)
