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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Allivet_Order_Day_UPD")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Allivet_Order_Day_UPD", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_ALLIVET_ORDER_DAY_0


query_0 = f"""SELECT
  ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
  ALLIVET_ORDER_LN_NBR AS ALLIVET_ORDER_LN_NBR,
  ALLIVET_ORDER_DT AS ALLIVET_ORDER_DT,
  PETSMART_ORDER_DT AS PETSMART_ORDER_DT,
  ORDER_STATUS AS ORDER_STATUS,
  PRODUCT_ID AS PRODUCT_ID,
  PETSMART_ORDER_NBR AS PETSMART_ORDER_NBR,
  PETSMART_SKU_NBR AS PETSMART_SKU_NBR,
  ALLIVET_SKU_NBR AS ALLIVET_SKU_NBR,
  SUB_TOTAL_AMT AS SUB_TOTAL_AMT,
  FREIGHT_COST AS FREIGHT_COST,
  TOTAL_AMT AS TOTAL_AMT,
  SHIP_METHOD_CD AS SHIP_METHOD_CD,
  ORDER_VOIDED_FLAG AS ORDER_VOIDED_FLAG,
  ORDER_ONHOLD_FLAG AS ORDER_ONHOLD_FLAG,
  ORDER_CREATED_DT AS ORDER_CREATED_DT,
  ORDER_MODIFIED_DT AS ORDER_MODIFIED_DT,
  SHIPPED_DT AS SHIPPED_DT,
  ORDER_SHIPPED_FLAG AS ORDER_SHIPPED_FLAG,
  INTERNAL_NOTES AS INTERNAL_NOTES,
  PUBLIC_NOTES AS PUBLIC_NOTES,
  AUTOSHIP_DISCOUNT_AMT AS AUTOSHIP_DISCOUNT_AMT,
  ORDER_MERCHANT_NOTES AS ORDER_MERCHANT_NOTES,
  RISKORDER_FLAG AS RISKORDER_FLAG,
  RISK_REASON AS RISK_REASON,
  ORIG_SHIP_METHOD_CD AS ORIG_SHIP_METHOD_CD,
  SHIP_HOLD_FLAG AS SHIP_HOLD_FLAG,
  SHIP_HOLD_DT AS SHIP_HOLD_DT,
  SHIP_RELEASE_DT AS SHIP_RELEASE_DT,
  ORDER_QTY AS ORDER_QTY,
  ITEM_DESC AS ITEM_DESC,
  EXT_PRICE AS EXT_PRICE,
  ORDER_DETAIL_CREATED_DT AS ORDER_DETAIL_CREATED_DT,
  ORDER_DETAIL_MODIFIED_DT AS ORDER_DETAIL_MODIFIED_DT,
  HOW_TO_GET_RX AS HOW_TO_GET_RX,
  VET_CD AS VET_CD,
  PET_CD AS PET_CD,
  ORDER_DETAIL_ONHOLD_FLAG AS ORDER_DETAIL_ONHOLD_FLAG,
  ONHOLD_TO_FILL_FLAG AS ONHOLD_TO_FILL_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  ALLIVET_ORDER_DAY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_ALLIVET_ORDER_DAY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_ALLIVET_ORDER_DAY_1


query_1 = f"""SELECT
  ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
  ORDER_STATUS AS ORDER_STATUS,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      ALLIVET_ORDER_NBR,
      ORDER_STATUS,
      ROW_NUMBER() OVER (
        PARTITION BY
          ALLIVET_ORDER_NBR
        ORDER BY
          UPDATE_TSTMP DESC
      ) RN
    FROM
      Shortcut_to_ALLIVET_ORDER_DAY_0
    WHERE
      ALLIVET_ORDER_NBR IN (
        SELECT
          ALLIVET_ORDER_NBR
        FROM
          (
            SELECT
              DISTINCT ALLIVET_ORDER_NBR,
              ORDER_STATUS
            FROM
              Shortcut_to_ALLIVET_ORDER_DAY_0
          ) A
        GROUP BY
          ALLIVET_ORDER_NBR
        HAVING
          COUNT(*) > 1
      )
  ) B
WHERE
  RN = 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_ALLIVET_ORDER_DAY_1")

# COMMAND ----------
# DBTITLE 1, EXP_UPDATE_TSTMP_2


query_2 = f"""SELECT
  ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
  ORDER_STATUS AS ORDER_STATUS,
  now() AS UPDATE_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_ALLIVET_ORDER_DAY_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_UPDATE_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, ALLIVET_ORDER_DAY


spark.sql("""INSERT INTO
  ALLIVET_ORDER_DAY
SELECT
  ALLIVET_ORDER_NBR AS ALLIVET_ORDER_NBR,
  NULL AS ALLIVET_ORDER_LN_NBR,
  NULL AS ALLIVET_ORDER_DT,
  NULL AS PETSMART_ORDER_DT,
  ORDER_STATUS AS ORDER_STATUS,
  NULL AS PRODUCT_ID,
  NULL AS PETSMART_ORDER_NBR,
  NULL AS PETSMART_SKU_NBR,
  NULL AS ALLIVET_SKU_NBR,
  NULL AS SUB_TOTAL_AMT,
  NULL AS FREIGHT_COST,
  NULL AS TOTAL_AMT,
  NULL AS SHIP_METHOD_CD,
  NULL AS ORDER_VOIDED_FLAG,
  NULL AS ORDER_ONHOLD_FLAG,
  NULL AS ORDER_CREATED_DT,
  NULL AS ORDER_MODIFIED_DT,
  NULL AS SHIPPED_DT,
  NULL AS ORDER_SHIPPED_FLAG,
  NULL AS INTERNAL_NOTES,
  NULL AS PUBLIC_NOTES,
  NULL AS AUTOSHIP_DISCOUNT_AMT,
  NULL AS ORDER_MERCHANT_NOTES,
  NULL AS RISKORDER_FLAG,
  NULL AS RISK_REASON,
  NULL AS ORIG_SHIP_METHOD_CD,
  NULL AS SHIP_HOLD_FLAG,
  NULL AS SHIP_HOLD_DT,
  NULL AS SHIP_RELEASE_DT,
  NULL AS ORDER_QTY,
  NULL AS ITEM_DESC,
  NULL AS EXT_PRICE,
  NULL AS ORDER_DETAIL_CREATED_DT,
  NULL AS ORDER_DETAIL_MODIFIED_DT,
  NULL AS HOW_TO_GET_RX,
  NULL AS VET_CD,
  NULL AS PET_CD,
  NULL AS ORDER_DETAIL_ONHOLD_FLAG,
  NULL AS ONHOLD_TO_FILL_FLAG,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  NULL AS LOAD_TSTMP
FROM
  EXP_UPDATE_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Allivet_Order_Day_UPD")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Allivet_Order_Day_UPD", mainWorkflowId, parentName)
