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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_date_type_week")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_date_type_week", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_WEEKS_0


query_0 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  CAL_WK AS CAL_WK,
  CAL_WK_NBR AS CAL_WK_NBR,
  CAL_MO AS CAL_MO,
  CAL_MO_NBR AS CAL_MO_NBR,
  CAL_MO_NAME AS CAL_MO_NAME,
  CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
  CAL_QTR AS CAL_QTR,
  CAL_QTR_NBR AS CAL_QTR_NBR,
  CAL_HALF AS CAL_HALF,
  CAL_YR AS CAL_YR,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_WK_NBR AS FISCAL_WK_NBR,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_MO_NBR AS FISCAL_MO_NBR,
  FISCAL_MO_NAME AS FISCAL_MO_NAME,
  FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
  FISCAL_HALF AS FISCAL_HALF,
  FISCAL_YR AS FISCAL_YR,
  LYR_WEEK_DT AS LYR_WEEK_DT,
  LWK_WEEK_DT AS LWK_WEEK_DT
FROM
  WEEKS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_WEEKS_0")

# COMMAND ----------
# DBTITLE 1, ASQ_Shortcut_To_WEEKS_1


query_1 = f"""SELECT
  DATE_TYPE_ID AS DATE_TYPE_ID,
  WEEK_DT AS WEEK_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  (
    SELECT
      DISTINCT CASE
        WHEN DAY_OF_WK_NBR = 7 THEN (
          SELECT
            (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1)) - 7
        )
        WHEN DAY_OF_WK_NBR <> 7 THEN (
          SELECT
            (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1))
        )
      END AS WEEK_DT,
      3 AS DATE_TYPE_ID
    FROM
      DAYS
    WHERE
      DAY_DT = CURRENT_DATE
    UNION ALL
    SELECT
      WK.WEEK_DT,
      4 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 WK,
      (
        SELECT
          DAY_DT,
          FISCAL_WK,
          FISCAL_MO,
          CASE
            WHEN LENGTH(LAST_PERIOD) = 6 THEN LAST_PERIOD
            ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)
          END AS LAST_PERIOD,
          FISCAL_QTR,
          CASE
            WHEN LENGTH(LAST_FISCAL_QTR) = 6 THEN LAST_FISCAL_QTR
            ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)
          END AS LAST_FISCAL_QTR,
          FISCAL_YR
        FROM
          (
            SELECT
              CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,
              DAY_DT,
              FISCAL_MO,
              FISCAL_QTR,
              FISCAL_YR,
              CASE
                WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_FISCAL_QTR,
              CASE
                WHEN SUBSTR(FISCAL_MO, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_PERIOD,
              FISCAL_WK
            FROM
              DAYS
            WHERE
              DAY_DT = (
                SELECT
                  CASE
                    WHEN DAY_OF_WK_NBR = 7 THEN (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1)) - 7
                    )
                    ELSE (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1))
                    )
                  END AS DAY_DT
                FROM
                  DAYS
                WHERE
                  DAY_DT = CURRENT_DATE
              )
          ) ALIAS1
      ) DATES
    WHERE
      WK.FISCAL_MO = DATES.FISCAL_MO
      AND WK.WEEK_DT <= DATES.DAY_DT
    UNION ALL
    SELECT
      WK.WEEK_DT,
      5 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 WK,
      (
        SELECT
          DAY_DT,
          FISCAL_WK,
          FISCAL_MO,
          CASE
            WHEN LENGTH(LAST_PERIOD) = 6 THEN LAST_PERIOD
            ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)
          END AS LAST_PERIOD,
          FISCAL_QTR,
          CASE
            WHEN LENGTH(LAST_FISCAL_QTR) = 6 THEN LAST_FISCAL_QTR
            ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)
          END AS LAST_FISCAL_QTR,
          FISCAL_YR
        FROM
          (
            SELECT
              CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,
              DAY_DT,
              FISCAL_MO,
              FISCAL_QTR,
              FISCAL_YR,
              CASE
                WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_FISCAL_QTR,
              CASE
                WHEN SUBSTR(FISCAL_MO, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_PERIOD,
              FISCAL_WK
            FROM
              DAYS
            WHERE
              DAY_DT = (
                SELECT
                  CASE
                    WHEN DAY_OF_WK_NBR = 7 THEN (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1)) - 7
                    )
                    ELSE (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1))
                    )
                  END AS DAY_DT
                FROM
                  DAYS
                WHERE
                  DAY_DT = CURRENT_DATE
              )
          ) ALIAS1
      ) DATES
    WHERE
      WK.FISCAL_MO = DATES.LAST_PERIOD
      AND WK.WEEK_DT <= DATES.DAY_DT
    UNION ALL
    SELECT
      WK.WEEK_DT,
      6 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 WK,
      (
        SELECT
          DAY_DT,
          FISCAL_WK,
          FISCAL_MO,
          CASE
            WHEN LENGTH(LAST_PERIOD) = 6 THEN LAST_PERIOD
            ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)
          END AS LAST_PERIOD,
          FISCAL_QTR,
          CASE
            WHEN LENGTH(LAST_FISCAL_QTR) = 6 THEN LAST_FISCAL_QTR
            ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)
          END AS LAST_FISCAL_QTR,
          FISCAL_YR
        FROM
          (
            SELECT
              CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,
              DAY_DT,
              FISCAL_MO,
              FISCAL_QTR,
              FISCAL_YR,
              CASE
                WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_FISCAL_QTR,
              CASE
                WHEN SUBSTR(FISCAL_MO, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_PERIOD,
              FISCAL_WK
            FROM
              DAYS
            WHERE
              DAY_DT = (
                SELECT
                  CASE
                    WHEN DAY_OF_WK_NBR = 7 THEN (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1)) - 7
                    )
                    ELSE (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1))
                    )
                  END AS DAY_DT
                FROM
                  DAYS
                WHERE
                  DAY_DT = CURRENT_DATE
              )
          ) ALIAS1
      ) DATES
    WHERE
      WK.FISCAL_QTR = DATES.FISCAL_QTR
      AND WK.WEEK_DT <= DATES.DAY_DT
    UNION ALL
    SELECT
      WK.WEEK_DT,
      7 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 WK,
      (
        SELECT
          DAY_DT,
          FISCAL_WK,
          FISCAL_MO,
          CASE
            WHEN LENGTH(LAST_PERIOD) = 6 THEN LAST_PERIOD
            ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)
          END AS LAST_PERIOD,
          FISCAL_QTR,
          CASE
            WHEN LENGTH(LAST_FISCAL_QTR) = 6 THEN LAST_FISCAL_QTR
            ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)
          END AS LAST_FISCAL_QTR,
          FISCAL_YR
        FROM
          (
            SELECT
              CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,
              DAY_DT,
              FISCAL_MO,
              FISCAL_QTR,
              FISCAL_YR,
              CASE
                WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_FISCAL_QTR,
              CASE
                WHEN SUBSTR(FISCAL_MO, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_PERIOD,
              FISCAL_WK
            FROM
              DAYS
            WHERE
              DAY_DT = (
                SELECT
                  CASE
                    WHEN DAY_OF_WK_NBR = 7 THEN (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1)) - 7
                    )
                    ELSE (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1))
                    )
                  END AS DAY_DT
                FROM
                  DAYS
                WHERE
                  DAY_DT = CURRENT_DATE
              )
          ) ALIAS1
      ) DATES
    WHERE
      WK.FISCAL_QTR = LAST_FISCAL_QTR
      AND WK.WEEK_DT <= DATES.DAY_DT
    UNION ALL
    SELECT
      WK.WEEK_DT,
      8 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 WK,
      (
        SELECT
          DAY_DT,
          FISCAL_WK,
          FISCAL_MO,
          CASE
            WHEN LENGTH(LAST_PERIOD) = 6 THEN LAST_PERIOD
            ELSE SUBSTR(LAST_PERIOD, 1, 4) || '0' || SUBSTR(LAST_PERIOD, 5, 1)
          END AS LAST_PERIOD,
          FISCAL_QTR,
          CASE
            WHEN LENGTH(LAST_FISCAL_QTR) = 6 THEN LAST_FISCAL_QTR
            ELSE SUBSTR(LAST_FISCAL_QTR, 1, 4) || '0' || SUBSTR(LAST_FISCAL_QTR, 5, 1)
          END AS LAST_FISCAL_QTR,
          FISCAL_YR
        FROM
          (
            SELECT
              CAST(FISCAL_YR || FISCAL_DAY_OF_YR_NBR AS BIGINT) FISCAL_DAY,
              DAY_DT,
              FISCAL_MO,
              FISCAL_QTR,
              FISCAL_YR,
              CASE
                WHEN SUBSTR(FISCAL_QTR, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '4' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_QTR, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_FISCAL_QTR,
              CASE
                WHEN SUBSTR(FISCAL_MO, 5, 2) = '01' THEN CAST(CAST(FISCAL_YR AS BIGINT) - 1 || '12' AS VARCHAR (4000))
                ELSE CAST(
                  FISCAL_YR || CAST(SUBSTR(FISCAL_MO, 5, 2) AS BIGINT) - 1 AS VARCHAR (4000)
                )
              END AS LAST_PERIOD,
              FISCAL_WK
            FROM
              DAYS
            WHERE
              DAY_DT = (
                SELECT
                  CASE
                    WHEN DAY_OF_WK_NBR = 7 THEN (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1)) - 7
                    )
                    ELSE (
                      SELECT
                        (CURRENT_DATE - (DATE_PART('dow', CURRENT_DATE) - 1))
                    )
                  END AS DAY_DT
                FROM
                  DAYS
                WHERE
                  DAY_DT = CURRENT_DATE
              )
          ) ALIAS1
      ) DATES
    WHERE
      WK.FISCAL_YR = DATES.FISCAL_YR
      AND WK.WEEK_DT <= DATES.DAY_DT
    UNION ALL
    SELECT
      DISTINCT WEEK_DT,
      9 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      WEEK_DT >= CURRENT_DATE - 28
      AND WEEK_DT < CURRENT_DATE
    UNION ALL
    SELECT
      DISTINCT WEEK_DT,
      10 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      WEEK_DT >= CURRENT_DATE - 56
      AND WEEK_DT < CURRENT_DATE
    UNION ALL
    SELECT
      DISTINCT WEEK_DT,
      11 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      WEEK_DT >= CURRENT_DATE - 91
      AND WEEK_DT < CURRENT_DATE
    UNION ALL
    SELECT
      WEEK_DT,
      12 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      FISCAL_YR = (
        SELECT
          DISTINCT FISCAL_YR - 1
        FROM
          DAYS
        WHERE
          DAY_DT = (
            CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1)
          )
      )
      AND FISCAL_WK_NBR <= (
        SELECT
          FISCAL_WK_NBR
        FROM
          Shortcut_To_WEEKS_0
        WHERE
          WEEK_DT = (
            CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1)
          )
      )
    UNION ALL
    SELECT
      WEEK_DT,
      13 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      FISCAL_YR = (
        SELECT
          DISTINCT FISCAL_YR - 1
        FROM
          DAYS
        WHERE
          DAY_DT = (
            CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1)
          )
      )
    UNION ALL
    SELECT
      DISTINCT WEEK_DT,
      14 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      WEEK_DT >= CURRENT_DATE - 112
      AND WEEK_DT < CURRENT_DATE
    UNION ALL
    SELECT
      WK.WEEK_DT,
      15 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 WK,
      (
        SELECT
          WEEK_DT,
          FISCAL_WK,
          FISCAL_QTR_NBR,
          FISCAL_WK_NBR,
          (FISCAL_YR - 1) LYR_FISCAL_YR
        FROM
          Shortcut_To_WEEKS_0 W
        WHERE
          WEEK_DT = (
            CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1)
          )
      ) CW
    WHERE
      WK.FISCAL_YR = CW.LYR_FISCAL_YR
      AND WK.FISCAL_QTR_NBR = CW.FISCAL_QTR_NBR
      AND WK.FISCAL_WK_NBR <= CW.FISCAL_WK_NBR
    UNION ALL
    SELECT
      (WEEK_DT - 14) AS WEEK_DT,
      16 AS DATE_TYPE_ID
    FROM
      DAYS
    WHERE
      DAY_DT = CURRENT_DATE
    UNION ALL
    SELECT
      DISTINCT WEEK_DT,
      17 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      WEEK_DT >= CURRENT_DATE - 182
      AND WEEK_DT < CURRENT_DATE
    UNION ALL
    SELECT
      DISTINCT WEEK_DT,
      18 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      WEEK_DT >= CURRENT_DATE - 364
      AND WEEK_DT < CURRENT_DATE
    UNION ALL
    SELECT
      LY.WEEK_DT,
      19 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 CR,
      Shortcut_To_WEEKS_0 LY
    WHERE
      CR.WEEK_DT = (
        CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1)
      )
      AND LY.FISCAL_YR = CR.FISCAL_YR - 1
      AND LY.FISCAL_MO_NBR = CR.FISCAL_MO_NBR
      AND LY.FISCAL_WK_NBR <= CR.FISCAL_WK_NBR
    UNION ALL
    SELECT
      LY.WEEK_DT,
      20 AS DATE_TYPE_ID
    FROM
      (
        SELECT
          WEEK_DT,
          FISCAL_MO_NBR,
          FISCAL_YR
        FROM
          Shortcut_To_WEEKS_0
        WHERE
          WEEK_DT = (
            CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1)
          )
      ) CR,
      Shortcut_To_WEEKS_0 LY
    WHERE
      LY.FISCAL_YR = CASE
        WHEN CR.FISCAL_MO_NBR = 1 THEN CR.FISCAL_YR - 2
        ELSE CR.FISCAL_YR - 1
      END
      AND LY.FISCAL_MO_NBR = CASE
        WHEN CR.FISCAL_MO_NBR = 1 THEN 12
        ELSE CR.FISCAL_MO_NBR - 1
      END
    UNION ALL
    SELECT
      DISTINCT WEEK_DT,
      21 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      WEEK_DT >= CURRENT_DATE - 14
      AND WEEK_DT < CURRENT_DATE
    UNION ALL
    SELECT
      WEEK_DT,
      22 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0
    WHERE
      substr(FISCAL_QTR, 5, 2) IN (03, 04)
      AND substr(FISCAL_QTR, 1, 4) = to_char(current_date, 'YYYY')
      AND week_Dt < current_Date
    UNION ALL
    SELECT
      w1.WEEK_DT,
      23 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 w1
    WHERE
      WEEK_DT < CURRENT_DATE
      AND FISCAL_HALF = (
        SELECT
          FISCAL_HALF
        FROM
          Shortcut_To_WEEKS_0 w2
        WHERE
          w2.WEEK_DT = (
            SELECT
              MAX(w3.WEEK_DT)
            FROM
              Shortcut_To_WEEKS_0 w3
            WHERE
              w3.WEEK_DT < CURRENT_DATE
          )
      )
    UNION ALL
    SELECT
      LY.WEEK_DT,
      72 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 LY
      JOIN (
        SELECT
          FISCAL_YR,
          MAX(FISCAL_WK_NBR) AS TOTAL_FISCAL_WK_AMT
        FROM
          Shortcut_To_WEEKS_0
        GROUP BY
          FISCAL_YR
      ) WKN ON LY.FISCAL_YR = WKN.FISCAL_YR
      JOIN Shortcut_To_WEEKS_0 CY ON LY.FISCAL_WK_NBR = (1 - CY.FISCAL_WK_NBR / 53) * (WKN.TOTAL_FISCAL_WK_AMT - 52) + CY.FISCAL_WK_NBR - (CY.FISCAL_WK_NBR / 53) * 52
      AND LY.FISCAL_YR = CY.FISCAL_YR - 1 + (CY.FISCAL_WK_NBR / 53)
    WHERE
      CY.WEEK_DT = (
        CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1)
      )
    UNION ALL
    SELECT
      WK.WEEK_DT,
      73 AS DATE_TYPE_ID
    FROM
      Shortcut_To_WEEKS_0 WK,
      (
        SELECT
          WEEK_DT,
          FISCAL_WK,
          FISCAL_HALF,
          FISCAL_WK_NBR,
          FISCAL_YR,
          (FISCAL_YR - 1) LYR_FISCAL_YR
        FROM
          Shortcut_To_WEEKS_0 W
        WHERE
          WEEK_DT = (
            CURRENT_DATE - 1 - (DATE_PART('dow', CURRENT_DATE - 1) - 1)
          )
      ) CW
    WHERE
      WK.FISCAL_YR = CW.LYR_FISCAL_YR
      AND WK.FISCAL_HALF - WK.FISCAL_YR * 100 = CW.FISCAL_HALF - CW.FISCAL_YR * 100
      AND WK.FISCAL_WK_NBR <= CW.FISCAL_WK_NBR
  ) ALIAS1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("ASQ_Shortcut_To_WEEKS_1")

# COMMAND ----------
# DBTITLE 1, EXP_DATE_TYPE_ID_2


query_2 = f"""SELECT
  TO_BIGINT(DATE_TYPE_ID) AS DATE_TYPE_ID,
  WEEK_DT AS WEEK_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  ASQ_Shortcut_To_WEEKS_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_DATE_TYPE_ID_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_DAYS_3


query_3 = f"""SELECT
  DAY_DT AS DAY_DT,
  BUSINESS_DAY_FLAG AS BUSINESS_DAY_FLAG,
  HOLIDAY_FLAG AS HOLIDAY_FLAG,
  DAY_OF_WK_NAME AS DAY_OF_WK_NAME,
  DAY_OF_WK_NAME_ABBR AS DAY_OF_WK_NAME_ABBR,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  CAL_DAY_OF_MO_NBR AS CAL_DAY_OF_MO_NBR,
  CAL_DAY_OF_YR_NBR AS CAL_DAY_OF_YR_NBR,
  CAL_WK AS CAL_WK,
  CAL_WK_NBR AS CAL_WK_NBR,
  CAL_MO AS CAL_MO,
  CAL_MO_NBR AS CAL_MO_NBR,
  CAL_MO_NAME AS CAL_MO_NAME,
  CAL_MO_NAME_ABBR AS CAL_MO_NAME_ABBR,
  CAL_QTR AS CAL_QTR,
  CAL_QTR_NBR AS CAL_QTR_NBR,
  CAL_HALF AS CAL_HALF,
  CAL_YR AS CAL_YR,
  FISCAL_DAY_OF_MO_NBR AS FISCAL_DAY_OF_MO_NBR,
  FISCAL_DAY_OF_YR_NBR AS FISCAL_DAY_OF_YR_NBR,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_WK_NBR AS FISCAL_WK_NBR,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_MO_NBR AS FISCAL_MO_NBR,
  FISCAL_MO_NAME AS FISCAL_MO_NAME,
  FISCAL_MO_NAME_ABBR AS FISCAL_MO_NAME_ABBR,
  FISCAL_QTR AS FISCAL_QTR,
  FISCAL_QTR_NBR AS FISCAL_QTR_NBR,
  FISCAL_HALF AS FISCAL_HALF,
  FISCAL_YR AS FISCAL_YR,
  LYR_WEEK_DT AS LYR_WEEK_DT,
  LWK_WEEK_DT AS LWK_WEEK_DT,
  WEEK_DT AS WEEK_DT,
  EST_TIME_CONV_AMT AS EST_TIME_CONV_AMT,
  EST_TIME_CONV_HRS AS EST_TIME_CONV_HRS,
  ES0_TIME_CONV_AMT AS ES0_TIME_CONV_AMT,
  ES0_TIME_CONV_HRS AS ES0_TIME_CONV_HRS,
  CST_TIME_CONV_AMT AS CST_TIME_CONV_AMT,
  CST_TIME_CONV_HRS AS CST_TIME_CONV_HRS,
  CS0_TIME_CONV_AMT AS CS0_TIME_CONV_AMT,
  CS0_TIME_CONV_HRS AS CS0_TIME_CONV_HRS,
  MST_TIME_CONV_AMT AS MST_TIME_CONV_AMT,
  MST_TIME_CONV_HRS AS MST_TIME_CONV_HRS,
  MS0_TIME_CONV_AMT AS MS0_TIME_CONV_AMT,
  MS0_TIME_CONV_HRS AS MS0_TIME_CONV_HRS,
  PST_TIME_CONV_AMT AS PST_TIME_CONV_AMT,
  PST_TIME_CONV_HRS AS PST_TIME_CONV_HRS
FROM
  DAYS"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_To_DAYS_3")

# COMMAND ----------
# DBTITLE 1, DATE_TYPE_WEEK951


spark.sql("""INSERT INTO
  DATE_TYPE_WEEK951
SELECT
  DATE_TYPE_ID AS DATE_TYPE_ID,
  WEEK_DT AS WEEK_DT
FROM
  EXP_DATE_TYPE_ID_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_date_type_week")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_date_type_week", mainWorkflowId, parentName)
