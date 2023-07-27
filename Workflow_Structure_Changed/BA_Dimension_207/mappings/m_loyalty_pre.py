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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_loyalty_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_loyalty_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_To_LOYALTY_0


query_0 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  STORE_DESC AS STORE_DESC,
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
  LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
  LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
  LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT
FROM
  LOYALTY"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_To_LOYALTY_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_To_LOYALTY_1


query_1 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  STORE_DESC AS STORE_DESC,
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
  LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
  LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
  LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_To_LOYALTY_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_To_LOYALTY_1")

# COMMAND ----------
# DBTITLE 1, EXP_NULL_2


query_2 = f"""SELECT
  ISNULL(STORE_NBR) AS IS_NULL_FLAG,
  STORE_NBR AS STORE_NBR,
  STORE_DESC AS STORE_DESC,
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
  LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
  SUBSTR(LOYALTY_PGM_START_DT, 0, 2) AS V_LOYALTY_PGM_START_MON,
  SUBSTR(LOYALTY_PGM_START_DT, 4, 2) AS V_LOYALTY_PGM_START_DAY,
  SUBSTR(LOYALTY_PGM_START_DT, 7, 4) AS V_LOYALTY_PGM_START_YR,
  LOYALTY_PGM_CHANGE_DT AS IN_LOYALTY_PGM_CHANGE_DT,
  SUBSTR(LOYALTY_PGM_CHANGE_DT, 0, 2) AS V_LOYALTY_PGM_CHANGE_MON,
  SUBSTR(LOYALTY_PGM_CHANGE_DT, 4, 2) AS V_LOYALTY_PGM_CHANGE_DAY,
  SUBSTR(LOYALTY_PGM_CHANGE_DT, 7, 4) AS V_LOYALTY_PGM_CHANGE_YR,
  trunc(sysdate) AS LOAD_DT,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_To_LOYALTY_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_NULL_2")

# COMMAND ----------
# DBTITLE 1, EXP_CHANGE_3


query_3 = f"""SELECT
  IFF(
    LENGTH(TO_CHAR(IN_MONTH)) = 1,
    concat('0', TO_CHAR(IN_MONTH)),
    TO_CHAR(IN_MONTH)
  ) AS v_mm,
  IFF(LENGTH(TO_CHAR(IN_YEAR)) = 4, IN_YEAR, 0) AS v_year,
  IFF(
    LENGTH(TO_CHAR(IN_DAY)) = 1,
    concat('0', TO_CHAR(IN_DAY)),
    TO_CHAR(IN_DAY)
  ) AS v_dd,
  IFF(
    TO_DECIMAL(v_year) > 0
    and TO_DECIMAL(v_mm) > 0
    and TO_DECIMAL(v_dd) > 0,
    TO_DATE(
      TO_CHAR(IN_YEAR) || '/' || v_mm || '/' || v_dd,
      'YYYY/MM/DD'
    ),
    null
  ) AS OUT_DATE_VAR,
  IFF(
    TO_DECIMAL(
      IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_YR)) = 4,
        V_LOYALTY_PGM_START_YR,
        0
      )
    ) > 0
    and TO_DECIMAL(
      IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_MON)) = 1,
        concat('0', TO_CHAR(V_LOYALTY_PGM_START_MON)),
        TO_CHAR(V_LOYALTY_PGM_START_MON)
      )
    ) > 0
    and TO_DECIMAL(
      IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_DAY)) = 1,
        concat('0', TO_CHAR(V_LOYALTY_PGM_START_DAY)),
        TO_CHAR(V_LOYALTY_PGM_START_DAY)
      )
    ) > 0,
    TO_DATE(
      TO_CHAR(V_LOYALTY_PGM_START_YR) || '/' || IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_MON)) = 1,
        concat('0', TO_CHAR(V_LOYALTY_PGM_START_MON)),
        TO_CHAR(V_LOYALTY_PGM_START_MON)
      ) || '/' || IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_DAY)) = 1,
        concat('0', TO_CHAR(V_LOYALTY_PGM_START_DAY)),
        TO_CHAR(V_LOYALTY_PGM_START_DAY)
      ),
      'YYYY/MM/DD'
    ),
    null
  ) AS OUT_DATE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_NULL_2"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("EXP_CHANGE_3")

# COMMAND ----------
# DBTITLE 1, EXP_START_DATE_4


query_4 = f"""SELECT
  IFF(
    LENGTH(TO_CHAR(IN_MONTH)) = 1,
    concat('0', TO_CHAR(IN_MONTH)),
    TO_CHAR(IN_MONTH)
  ) AS v_mm,
  IFF(LENGTH(TO_CHAR(IN_YEAR)) = 4, IN_YEAR, 0) AS v_year,
  IFF(
    LENGTH(TO_CHAR(IN_DAY)) = 1,
    concat('0', TO_CHAR(IN_DAY)),
    TO_CHAR(IN_DAY)
  ) AS v_dd,
  IFF(
    TO_DECIMAL(v_year) > 0
    and TO_DECIMAL(v_mm) > 0
    and TO_DECIMAL(v_dd) > 0,
    TO_DATE(
      TO_CHAR(IN_YEAR) || '/' || v_mm || '/' || v_dd,
      'YYYY/MM/DD'
    ),
    null
  ) AS OUT_DATE_VAR,
  IFF(
    TO_DECIMAL(
      IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_YR)) = 4,
        V_LOYALTY_PGM_START_YR,
        0
      )
    ) > 0
    and TO_DECIMAL(
      IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_MON)) = 1,
        concat('0', TO_CHAR(V_LOYALTY_PGM_START_MON)),
        TO_CHAR(V_LOYALTY_PGM_START_MON)
      )
    ) > 0
    and TO_DECIMAL(
      IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_DAY)) = 1,
        concat('0', TO_CHAR(V_LOYALTY_PGM_START_DAY)),
        TO_CHAR(V_LOYALTY_PGM_START_DAY)
      )
    ) > 0,
    TO_DATE(
      TO_CHAR(V_LOYALTY_PGM_START_YR) || '/' || IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_MON)) = 1,
        concat('0', TO_CHAR(V_LOYALTY_PGM_START_MON)),
        TO_CHAR(V_LOYALTY_PGM_START_MON)
      ) || '/' || IFF(
        LENGTH(TO_CHAR(V_LOYALTY_PGM_START_DAY)) = 1,
        concat('0', TO_CHAR(V_LOYALTY_PGM_START_DAY)),
        TO_CHAR(V_LOYALTY_PGM_START_DAY)
      ),
      'YYYY/MM/DD'
    ),
    null
  ) AS OUT_DATE,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_NULL_2"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("EXP_START_DATE_4")

# COMMAND ----------
# DBTITLE 1, FLT_NULLS_5


query_5 = f"""SELECT
  EN2.IS_NULL_FLAG AS IS_NULL_FLAG,
  EN2.STORE_NBR AS STORE_NBR,
  EN2.STORE_DESC AS STORE_DESC,
  EN2.PETSMART_DMA_CD AS PETSMART_DMA_CD,
  EN2.PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  EN2.LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  EN2.LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
  EN2.LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  EN2.LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
  ESD4.OUT_DATE AS LOYALTY_PGM_START_DT,
  EC3.OUT_DATE AS LOYALTY_PGM_CHANGE_DT,
  EN2.LOAD_DT AS LOAD_DT,
  EN2.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  EXP_NULL_2 EN2
  INNER JOIN EXP_START_DATE_4 ESD4 ON EN2.Monotonically_Increasing_Id = ESD4.Monotonically_Increasing_Id
  INNER JOIN EXP_CHANGE_3 EC3 ON ESD4.Monotonically_Increasing_Id = EC3.Monotonically_Increasing_Id
WHERE
  EN2.IS_NULL_FLAG = 0"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("FLT_NULLS_5")

# COMMAND ----------
# DBTITLE 1, LOYALTY_PRE


spark.sql("""INSERT INTO
  LOYALTY_PRE
SELECT
  STORE_NBR AS STORE_NBR,
  STORE_DESC AS STORE_DESC,
  PETSMART_DMA_CD AS PETSMART_DMA_CD,
  PETSMART_DMA_DESC AS PETSMART_DMA_DESC,
  LOYALTY_PGM_TYPE_ID AS LOYALTY_PGM_TYPE_ID,
  LOYALTY_PGM_TYPE_DESC AS LOYALTY_PGM_TYPE_DESC,
  LOYALTY_PGM_STATUS_ID AS LOYALTY_PGM_STATUS_ID,
  LOYALTY_PGM_STATUS_DESC AS LOYALTY_PGM_STATUS_DESC,
  LOYALTY_PGM_START_DT AS LOYALTY_PGM_START_DT,
  LOYALTY_PGM_CHANGE_DT AS LOYALTY_PGM_CHANGE_DT,
  LOAD_DT AS LOAD_DT
FROM
  FLT_NULLS_5""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_loyalty_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_loyalty_pre", mainWorkflowId, parentName)
