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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_POG_SKU_STORE_UPD")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_POG_SKU_STORE_UPD", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_POG_SKU_STORE_0


query_0 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  POG_NBR AS POG_NBR,
  POG_DBKEY AS POG_DBKEY,
  LISTING_START_DT AS LISTING_START_DT,
  LISTING_END_DT AS LISTING_END_DT,
  POSITIONS_CNT AS POSITIONS_CNT,
  FACINGS_CNT AS FACINGS_CNT,
  CAPACITY_CNT AS CAPACITY_CNT,
  PRESENTATION_QTY AS PRESENTATION_QTY,
  POG_TYPE_CD AS POG_TYPE_CD,
  POG_SKU_POSITION_STATUS_ID AS POG_SKU_POSITION_STATUS_ID,
  DELETE_FLAG AS DELETE_FLAG,
  SAP_LAST_CHANGE_TSTMP AS SAP_LAST_CHANGE_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  POG_SKU_STORE"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_POG_SKU_STORE_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_POG_SKU_STORE_1


query_1 = f"""SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  POG_NBR AS POG_NBR,
  POG_DBKEY AS POG_DBKEY,
  LISTING_START_DT AS LISTING_START_DT,
  MIN(POG_SKU_POSITION_STATUS_ID) AS POG_SKU_POSITION_STATUS_ID,
  MAX(DELETE_FLAG) AS DELETE_FLAG,
  MAX(UPDATE_TSTMP) AS UPDATE_TSTMP,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      PRODUCT_ID,
      LOCATION_ID,
      POG_NBR,
      POG_DBKEY,
      LISTING_START_DT,
      4 AS POG_SKU_POSITION_STATUS_ID,
      DELETE_FLAG,
      CURRENT_TIMESTAMP AS UPDATE_TSTMP
    FROM
      Shortcut_to_POG_SKU_STORE_0
    WHERE
      (
        Shortcut_to_POG_SKU_STORE_0.POG_SKU_POSITION_STATUS_ID = 1
        AND Shortcut_to_POG_SKU_STORE_0.LISTING_END_DT < CURRENT_DATE
        AND Shortcut_to_POG_SKU_STORE_0.DELETE_FLAG = 0
      )
    UNION
    SELECT
      PRODUCT_ID,
      LOCATION_ID,
      POG_NBR,
      POG_DBKEY,
      LISTING_START_DT,
      3 AS POG_SKU_POSITION_STATUS_ID,
      1 AS DELETE_FLAG,
      CURRENT_TIMESTAMP AS UPDATE_TSTMP
    FROM
      (
        SELECT
          P.PRODUCT_ID,
          P.LOCATION_ID,
          P.POG_NBR,
          P.POG_DBKEY,
          P.LISTING_START_DT,
          ROW_NUMBER() OVER (
            PARTITION BY
              PRODUCT_ID,
              LOCATION_ID,
              POG_DBKEY
            ORDER BY
              SAP_LAST_CHANGE_TSTMP DESC
          ) AS RK
        FROM
          Shortcut_to_POG_SKU_STORE_0 P
        WHERE
          DELETE_FLAG = 0
      ) A
    WHERE
      RK <> 1
    UNION
    SELECT
      PRODUCT_ID,
      LOCATION_ID,
      POG_NBR,
      POG_DBKEY,
      LISTING_START_DT,
      3 AS POG_SKU_POSITION_STATUS_ID,
      1 AS DELETE_FLAG,
      CURRENT_TIMESTAMP AS UPDATE_TSTMP
    FROM
      Shortcut_to_POG_SKU_STORE_0
    WHERE
      (
        Shortcut_to_POG_SKU_STORE_0.POG_SKU_POSITION_STATUS_ID = 2
        AND Shortcut_to_POG_SKU_STORE_0.LISTING_START_DT <= CURRENT_DATE
        AND Shortcut_to_POG_SKU_STORE_0.DELETE_FLAG = 0
      )
    UNION
    (
      SELECT
        P.PRODUCT_ID,
        P.LOCATION_ID,
        P.POG_NBR,
        P.POG_DBKEY,
        P.LISTING_START_DT,
        3 AS POG_SKU_POSITION_STATUS_ID,
        1 AS DELETE_FLAG,
        CURRENT_TIMESTAMP AS UPDATE_TSTMP
      FROM
        Shortcut_to_POG_SKU_STORE_0 P,
        (
          SELECT
            SP.CKB_DB_PLANOGRAM_KEY,
            PRD.PRODUCT_ID,
            PLN.DB_DATE_EFFECTIVE_FROM
          FROM
            CKB_SPC_PERFORMANCE SP
            JOIN CKB_SPC_PRODUCT PRD ON SP.CKB_DB_PRODUCT_KEY = PRD.CKB_DB_PRODUCT_KEY
            JOIN CKB_SPC_PLANOGRAM PLN ON PLN.CKB_DB_PLANOGRAM_KEY = SP.CKB_DB_PLANOGRAM_KEY
            LEFT OUTER JOIN (
              SELECT
                DISTINCT SP.CKB_DB_PLANOGRAM_KEY AS ACTIVE_KEY,
                PRD.SKU_NBR AS ACTIVE_SKU,
                PLN.DB_DATE_EFFECTIVE_FROM AS ACTIVE_DB_DATE_EFFECTIVE_FROM
              FROM
                CKB_SPC_PERFORMANCE SP
                JOIN CKB_SPC_PRODUCT PRD ON SP.CKB_DB_PRODUCT_KEY = PRD.CKB_DB_PRODUCT_KEY
                JOIN CKB_SPC_PLANOGRAM PLN ON PLN.CKB_DB_PLANOGRAM_KEY = SP.CKB_DB_PLANOGRAM_KEY
              WHERE
                PLN.DB_STATUS IN (1, 2, 4)
                AND SP.DEL_FLAG = 0
            ) LV ON LV.ACTIVE_KEY = SP.CKB_DB_PLANOGRAM_KEY
            AND LV.ACTIVE_SKU = PRD.SKU_NBR
            AND LV.ACTIVE_DB_DATE_EFFECTIVE_FROM = PLN.DB_DATE_EFFECTIVE_FROM
          WHERE
            PLN.DB_STATUS IN (1, 2, 4)
            AND SP.DEL_FLAG = 1
            AND LV.ACTIVE_KEY IS NULL
        ) X
      WHERE
        P.POG_DBKEY = X.CKB_DB_PLANOGRAM_KEY
        AND P.PRODUCT_ID = X.PRODUCT_ID
        AND P.LISTING_START_DT = X.DB_DATE_EFFECTIVE_FROM
        AND P.DELETE_FLAG = 0
      UNION
      SELECT
        P.PRODUCT_ID,
        P.LOCATION_ID,
        P.POG_NBR,
        P.POG_DBKEY,
        P.LISTING_START_DT,
        3 AS POG_SKU_POSITION_STATUS_ID,
        1 AS DELETE_FLAG,
        CURRENT_TIMESTAMP AS UPDATE_TSTMP
      FROM
        Shortcut_to_POG_SKU_STORE_0 P,
        (
          SELECT
            SP.CKB_DB_PLANOGRAM_KEY,
            PLN.DB_DATE_EFFECTIVE_FROM,
            PRD.SKU_NBR,
            PRD.PRODUCT_ID,
            SP.DEL_FLAG,
            SP.LOAD_DT,
            SP.UPDATE_DT,
            CASE
              WHEN LV.ACTIVE_KEY IS NULL THEN 'Y'
              ELSE 'N'
            END AS FULL_DEL_FLAG
          FROM
            CKB_SPC_PERFORMANCE SP
            JOIN CKB_SPC_PRODUCT PRD ON SP.CKB_DB_PRODUCT_KEY = PRD.CKB_DB_PRODUCT_KEY
            JOIN CKB_SPC_PLANOGRAM PLN ON PLN.CKB_DB_PLANOGRAM_KEY = SP.CKB_DB_PLANOGRAM_KEY
            LEFT OUTER JOIN (
              SELECT
                DISTINCT SP.CKB_DB_PLANOGRAM_KEY AS ACTIVE_KEY,
                PRD.SKU_NBR AS ACTIVE_SKU
              FROM
                CKB_SPC_PERFORMANCE SP
                JOIN CKB_SPC_PRODUCT PRD ON SP.CKB_DB_PRODUCT_KEY = PRD.CKB_DB_PRODUCT_KEY
                JOIN CKB_SPC_PLANOGRAM PLN ON PLN.CKB_DB_PLANOGRAM_KEY = SP.CKB_DB_PLANOGRAM_KEY
              WHERE
                PLN.DB_STATUS IN (1, 2, 4)
                AND SP.DEL_FLAG = 0
            ) LV ON LV.ACTIVE_KEY = SP.CKB_DB_PLANOGRAM_KEY
            AND LV.ACTIVE_SKU = PRD.SKU_NBR
          WHERE
            PLN.DB_STATUS IN (1, 2, 4)
            AND SP.DEL_FLAG = 1
        ) X
      WHERE
        P.POG_DBKEY = X.CKB_DB_PLANOGRAM_KEY
        AND P.PRODUCT_ID = X.PRODUCT_ID
        AND P.DELETE_FLAG = 0
        AND X.FULL_DEL_FLAG = 'Y'
      UNION
      SELECT
        P.PRODUCT_ID,
        P.LOCATION_ID,
        P.POG_NBR,
        P.POG_DBKEY,
        P.LISTING_START_DT,
        3 AS POG_SKU_POSITION_STATUS_ID,
        1 AS DELETE_FLAG,
        CURRENT_TIMESTAMP AS UPDATE_TSTMP
      FROM
        Shortcut_to_POG_SKU_STORE_0 P,
        (
          SELECT
            *
          FROM
            (
              SELECT
                FF.LOCATION_ID,
                FP.CKB_DB_PLANOGRAM_KEY,
                PLN.DB_DATE_EFFECTIVE_FROM,
                MIN(FP.DEL_FLAG) DEL_FLAG
              FROM
                CKB_FLR_PERFORMANCE FP
                JOIN CKB_FLR_FLOORPLAN FF ON FP.CKB_DB_FLOOR_PLAN_KEY = FF.CKB_DB_FLOOR_PLAN_KEY
                JOIN CKB_SPC_PLANOGRAM PLN ON PLN.CKB_DB_PLANOGRAM_KEY = FP.CKB_DB_PLANOGRAM_KEY
                JOIN CKB_FLR_FLOORPLAN FLR ON (
                  FLR.CKB_DB_FLOOR_PLAN_KEY = FP.CKB_DB_FLOOR_PLAN_KEY
                  OR FLR.DB_VERSION_KEY = FP.CKB_DB_FLOOR_PLAN_KEY
                )
              WHERE
                PLN.DB_STATUS IN (2)
                AND FLR.DB_STATUS IN (2)
              GROUP BY
                FF.LOCATION_ID,
                FP.CKB_DB_PLANOGRAM_KEY,
                PLN.DB_DATE_EFFECTIVE_FROM
            ) A
          WHERE
            A.DEL_FLAG = 1
        ) X
      WHERE
        P.POG_DBKEY = X.CKB_DB_PLANOGRAM_KEY
        AND P.LOCATION_ID = X.LOCATION_ID
        AND P.LISTING_START_DT = X.DB_DATE_EFFECTIVE_FROM
        AND P.DELETE_FLAG = 0
    )
  ) A
GROUP BY
  PRODUCT_ID,
  LOCATION_ID,
  POG_NBR,
  POG_DBKEY,
  LISTING_START_DT"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_POG_SKU_STORE_1")

# COMMAND ----------
# DBTITLE 1, POG_SKU_STORE


spark.sql("""INSERT INTO
  POG_SKU_STORE
SELECT
  PRODUCT_ID AS PRODUCT_ID,
  LOCATION_ID AS LOCATION_ID,
  POG_NBR AS POG_NBR,
  POG_DBKEY AS POG_DBKEY,
  LISTING_START_DT AS LISTING_START_DT,
  NULL AS LISTING_END_DT,
  NULL AS POSITIONS_CNT,
  NULL AS FACINGS_CNT,
  NULL AS CAPACITY_CNT,
  NULL AS PRESENTATION_QTY,
  NULL AS POG_TYPE_CD,
  POG_SKU_POSITION_STATUS_ID AS POG_SKU_POSITION_STATUS_ID,
  DELETE_FLAG AS DELETE_FLAG,
  NULL AS SAP_LAST_CHANGE_TSTMP,
  UPDATE_TSTMP AS UPDATE_TSTMP,
  NULL AS LOAD_TSTMP
FROM
  SQ_Shortcut_to_POG_SKU_STORE_1""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_POG_SKU_STORE_UPD")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_POG_SKU_STORE_UPD", mainWorkflowId, parentName)
