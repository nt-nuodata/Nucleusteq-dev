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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_ca_c_earn_pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_ca_c_earn_pre", variablesTableName, mainWorkflowId)

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
# DBTITLE 1, Shortcut_to_PS2_CA_C_EARN_WAGE_PRE_1


query_1 = f"""SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  TTL_EARNED_HRS AS TTL_EARNED_HRS,
  WFA_SAL_MGR_HRS AS WFA_SAL_MGR_HRS,
  ACT_SAL_MGR_HRS AS ACT_SAL_MGR_HRS,
  ACT_SAL_MGR_AMT AS ACT_SAL_MGR_AMT,
  CNT_OF_HRLY_MGRS AS CNT_OF_HRLY_MGRS,
  AVG_ACT_WAGE_RATE AS AVG_ACT_WAGE_RATE,
  STR_HRLY_MGR_AVG_WG_RT AS STR_HRLY_MGR_AVG_WG_RT
FROM
  PS2_CA_C_EARN_WAGE_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PS2_CA_C_EARN_WAGE_PRE_1")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PS2_CA_C_EARN_WAGE_PRE_2


query_2 = f"""SELECT
  pre.week_dt AS WEEK_DT,
  pre.location_id AS LOCATION_ID,
  ROUND(
    NVL(
      (
        (
          (
            pre.ttl_earned_hrs - (pre.act_sal_mgr_hrs * NVL(smo_dflt.factor_value, 0)) - (
              pre.cnt_of_hrly_mgrs * NVL(stdot_dflt.factor_value, 0)
            ) - (
              (
                pre.ttl_earned_hrs - pre.act_sal_mgr_hrs * NVL(smo_dflt.factor_value, 0)
              ) * (NVL(hday.factor_value, 0))
            )
          ) * pre.avg_act_wage_rate
        ) + (
          1.5 * (
            pre.cnt_of_hrly_mgrs * NVL(stdot_dflt.factor_value, 0)
          ) * str_hrly_mgr_avg_wg_rt
        ) + (
          (
            (
              (
                pre.ttl_earned_hrs - pre.act_sal_mgr_hrs * NVL(smo_dflt.factor_value, 0)
              ) * NVL(hday.factor_value, 0)
            )
          ) * 1.5 * pre.avg_act_wage_rate
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
        pre.cnt_of_hrly_mgrs * NVL(stdot_dflt.factor_value, 0)
      ) + (
        (
          pre.ttl_earned_hrs - pre.act_sal_mgr_hrs * NVL(smo_dflt.factor_value, 0)
        ) * NVL(hday.factor_value, 0)
      )
    ),
    2
  ) AS EARNED_OT_HRS,
  ROUND(
    (
      (
        1.5 * (
          pre.cnt_of_hrly_mgrs * NVL(stdot_dflt.factor_value, 0)
        ) * str_hrly_mgr_avg_wg_rt
      ) + (
        (
          (
            (
              pre.ttl_earned_hrs - pre.act_sal_mgr_hrs * NVL(smo_dflt.factor_value, 0)
            ) * NVL(hday.factor_value, 0)
          )
        ) * 1.5 * pre.avg_act_wage_rate
      )
    ),
    2
  ) AS EARNED_OT_AMT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PS2_CA_C_EARN_WAGE_PRE_1 pre
  LEFT OUTER JOIN ps2_factors smo_dflt ON (
    pre.week_dt BETWEEN smo_dflt.start_dt
    AND NVL(smo_dflt.end_dt, '31-dec-3000')
    AND smo_dflt.factor_desc = 'CANSALMGROT'
    AND smo_dflt.factor_type = 'MERCH'
    AND smo_dflt.location_id = 'DFLT'
  )
  LEFT OUTER JOIN ps2_factors stdot_dflt ON (
    pre.week_dt BETWEEN stdot_dflt.start_dt
    AND NVL(stdot_dflt.end_dt, '31-dec-3000')
    AND stdot_dflt.factor_type = 'MERCH'
    AND stdot_dflt.factor_desc = 'CANSTDOTHRS'
    AND stdot_dflt.location_id = 'DFLT'
  )
  LEFT OUTER JOIN ps2_factors hday ON (
    pre.week_dt BETWEEN hday.start_dt
    AND NVL(hday.end_dt, '31-dec-3000')
    AND hday.factor_desc = 'CANHOLIDAYOT'
    AND hday.factor_type = 'STR'
    AND hday.location_id = 'DFLT'
  )
WHERE
  pre.week_dt > '01-jan-2011'
  AND pre.week_dt < CURRENT_DATE"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("SQ_Shortcut_to_PS2_CA_C_EARN_WAGE_PRE_2")

# COMMAND ----------
# DBTITLE 1, PS2_CA_C_EARN_PRE


spark.sql("""INSERT INTO
  PS2_CA_C_EARN_PRE
SELECT
  WEEK_DT AS WEEK_DT,
  LOCATION_ID AS LOCATION_ID,
  AVG_ACT_WAGE_RATE AS WEIGHTED_AVG_WAGE_RATE,
  ACT_SAL_MGR_AMT AS ACT_SAL_MGR_AMT,
  EARNED_OT_HRS AS EARNED_OT_HRS,
  EARNED_OT_AMT AS EARNED_OT_AMT
FROM
  SQ_Shortcut_to_PS2_CA_C_EARN_WAGE_PRE_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_ca_c_earn_pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_ca_c_earn_pre", mainWorkflowId, parentName)
