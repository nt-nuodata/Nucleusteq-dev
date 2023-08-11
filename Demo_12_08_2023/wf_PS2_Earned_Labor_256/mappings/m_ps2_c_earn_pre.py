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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_c_earn_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_c_earn_pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_FACTORS_0


query_0 = f"""SELECT
  ROW_NO AS ROW_NO,
  FACTOR_TYPE AS FACTOR_TYPE,
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  START_DT AS START_DT,
  END_DT AS END_DT,
  FACTOR_DESC AS FACTOR_DESC,
  OTHER AS OTHER,
  FACTOR_VALUE AS FACTOR_VALUE,
  UPDATE_DT AS UPDATE_DT,
  LOAD_DT AS LOAD_DT
FROM
  PS2_FACTORS"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PS2_FACTORS_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_C_EARN_WAGE_PRE_1


query_1 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  TTL_EARNED_HRS AS TTL_EARNED_HRS,
  WFA_SAL_MGR_HRS AS WFA_SAL_MGR_HRS,
  ACT_SAL_MGR_HRS AS ACT_SAL_MGR_HRS,
  ACT_SAL_MGR_AMT AS ACT_SAL_MGR_AMT,
  CNT_OF_HRLY_MGRS AS CNT_OF_HRLY_MGRS,
  AVG_ACT_WAGE_RATE AS AVG_ACT_WAGE_RATE,
  HOLIDAY_OT_HRS AS HOLIDAY_OT_HRS,
  STR_HRLY_MGR_AVG_WG_RT AS STR_HRLY_MGR_AVG_WG_RT
FROM
  PS2_C_EARN_WAGE_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PS2_C_EARN_WAGE_PRE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PS2_C_EARN_PRE_2


query_2 = f"""SELECT
  pre.week_dt AS WEEK_DT,
  pre.location_id AS LOCATION_ID,
  ROUND(
    NVL(
      (
        (
          (
            pre.ttl_earned_hrs - (
              pre.act_sal_mgr_hrs * NVL(
                smo.factor_value,
                CASE
                  WHEN COUNTRY_CD = 'US' THEN smo_dflt.factor_value
                  WHEN COUNTRY_CD = 'CA' THEN smo_cadflt.factor_value
                END
              )
            ) - (
              pre.cnt_of_hrly_mgrs * NVL(stdot.factor_value, stdot_dflt.factor_value)
            ) - NVL(pcop.over_time_hrs, 0)
          ) * pre.avg_act_wage_rate
        ) + CASE
          WHEN str_hrly_mgr_avg_wg_rt = 0 THEN (
            (
              pre.cnt_of_hrly_mgrs * NVL(stdot.factor_value, stdot_dflt.factor_value)
            ) * pre.avg_act_wage_rate
          )
          ELSE (
            1.5 * (
              pre.cnt_of_hrly_mgrs * NVL(stdot.factor_value, stdot_dflt.factor_value)
            ) * str_hrly_mgr_avg_wg_rt
          )
        END + (
          NVL(pcop.over_time_hrs, 0) * 1.5 * pre.avg_act_wage_rate
        )
      ),
      0
    ) / CASE
      WHEN (pre.ttl_earned_hrs - pre.wfa_sal_mgr_hrs) = 0 THEN 1
      ELSE (pre.ttl_earned_hrs - pre.wfa_sal_mgr_hrs)
    END,
    4
  ) AS AVG_ACT_WAGE_RATE,
  pre.act_sal_mgr_amt AS ACT_SAL_MGR_AMT,
  ROUND(
    (
      (
        pre.cnt_of_hrly_mgrs * NVL(stdot.factor_value, stdot_dflt.factor_value)
      ) + (NVL(pcop.over_time_hrs, 0))
    ),
    2
  ) AS EARNED_OT_HRS,
  ROUND(
    (
      CASE
        WHEN str_hrly_mgr_avg_wg_rt = 0 THEN (
          (
            pre.cnt_of_hrly_mgrs * NVL(stdot.factor_value, stdot_dflt.factor_value)
          ) * pre.avg_act_wage_rate
        )
        ELSE (
          1.5 * (
            pre.cnt_of_hrly_mgrs * NVL(stdot.factor_value, stdot_dflt.factor_value)
          ) * str_hrly_mgr_avg_wg_rt
        )
      END + (
        (NVL(pcop.over_time_hrs, 0)) * 1.5 * pre.avg_act_wage_rate
      )
    ),
    2
  ) AS EARNED_OT_AMT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PS2_C_EARN_WAGE_PRE_1 pre
  JOIN SITE_PROFILE SP ON PRE.LOCATION_ID = SP.LOCATION_ID
  LEFT OUTER JOIN ps2_factors smo ON (
    pre.week_dt BETWEEN smo.start_dt
    AND NVL(smo.end_dt, '31-dec-3000')
    AND CAST(pre.location_id AS VARCHAR (5)) = smo.location_id
    AND smo.factor_desc = 'SALMGROT'
    AND smo.factor_type = 'MERCH'
    AND smo.location_id <> 'DFLT'
  )
  LEFT OUTER JOIN ps2_factors smo_dflt ON (
    pre.week_dt BETWEEN smo_dflt.start_dt
    AND NVL(smo_dflt.end_dt, '31-dec-3000')
    AND smo_dflt.factor_desc = 'SALMGROT'
    AND smo_dflt.factor_type = 'MERCH'
    AND smo_dflt.location_id = 'DFLT'
  )
  LEFT OUTER JOIN ps2_factors smo_cadflt ON (
    pre.week_dt BETWEEN smo_cadflt.start_dt
    AND NVL(smo_cadflt.end_dt, '31-dec-3000')
    AND smo_cadflt.factor_desc = 'CANSALMGROT'
    AND smo_cadflt.factor_type = 'MERCH'
    AND smo_cadflt.location_id = 'DFLT'
  )
  LEFT OUTER JOIN ps2_factors stdot ON (
    pre.week_dt BETWEEN stdot.start_dt
    AND NVL(stdot.end_dt, '31-dec-3000')
    AND CAST(pre.location_id AS VARCHAR (5)) = stdot.location_id
    AND stdot.factor_type = 'MERCH'
    AND stdot.factor_desc = 'STDOTHRS'
    AND stdot.location_id <> 'DFLT'
  )
  LEFT OUTER JOIN ps2_factors stdot_dflt ON (
    pre.week_dt BETWEEN stdot_dflt.start_dt
    AND NVL(stdot_dflt.end_dt, '31-dec-3000')
    AND stdot_dflt.factor_type = 'MERCH'
    AND stdot_dflt.factor_desc = 'STDOTHRS'
    AND stdot_dflt.location_id = 'DFLT'
  )
  LEFT OUTER JOIN ps2_core_ot_pre pcop ON pre.week_dt = pcop.week_dt
  AND pre.location_id = pcop.location_id
WHERE
  pre.week_dt > '01-jan-2011'
  AND pre.week_dt < CURRENT_DATE
ORDER BY
  pre.week_dt,
  pre.location_id"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_PS2_C_EARN_PRE_2")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_CORE_OT_PRE_3


query_3 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  OVER_TIME_HRS AS OVER_TIME_HRS
FROM
  PS2_CORE_OT_PRE"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("Shortcut_to_PS2_CORE_OT_PRE_3")

# COMMAND ----------
# DBTITLE 1, PS2_C_EARN_PRE


spark.sql("""INSERT INTO
  PS2_C_EARN_PRE
SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  AVG_ACT_WAGE_RATE AS WEIGHTED_AVG_WAGE_RATE,
  ACT_SAL_MGR_AMT AS ACT_SAL_MGR_AMT,
  EARNED_OT_HRS AS EARNED_OT_HRS,
  EARNED_OT_AMT AS EARNED_OT_AMT
FROM
  SQ_Shortcut_to_PS2_C_EARN_PRE_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_c_earn_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_c_earn_pre", mainWorkflowId, parentName)
