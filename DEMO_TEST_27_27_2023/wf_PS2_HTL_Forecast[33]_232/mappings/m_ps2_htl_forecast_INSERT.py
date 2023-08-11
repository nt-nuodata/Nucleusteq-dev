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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_forecast_INSERT")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_ps2_htl_forecast_INSERT", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_HTL_VARIABLES_0


query_0 = f"""SELECT
  STORE_NBR AS STORE_NBR,
  DEFAULT_STORE_FLAG AS DEFAULT_STORE_FLAG,
  PS2_HTL_VAR_REC_EFF_DT AS PS2_HTL_VAR_REC_EFF_DT,
  PS2_HTL_VAR_REC_END_DT AS PS2_HTL_VAR_REC_END_DT,
  TY_WEIGHT_PCNT AS TY_WEIGHT_PCNT,
  LY_WEIGHT_PCNT AS LY_WEIGHT_PCNT,
  FRONT_DESK_FIXED_HRS AS FRONT_DESK_FIXED_HRS,
  FRONT_DESK_VAR_HRS AS FRONT_DESK_VAR_HRS,
  PLAYROOM_VAR_HRS AS PLAYROOM_VAR_HRS,
  BACK_OF_HOUSE_FIXED_HRS AS BACK_OF_HOUSE_FIXED_HRS,
  BACK_OF_HOUSE_VAR_HRS AS BACK_OF_HOUSE_VAR_HRS,
  OVERNIGHT_TRESHHOLD AS OVERNIGHT_TRESHHOLD,
  OVERNIGHT_UPPER_VALUE AS OVERNIGHT_UPPER_VALUE,
  OVERNIGHT_LOWER_VALUE AS OVERNIGHT_LOWER_VALUE,
  SUPERVISOR_HRS AS SUPERVISOR_HRS,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  PS2_HTL_VARIABLES"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PS2_HTL_VARIABLES_0")

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PS2_HTL_FORECAST_PRE_1


query_1 = f"""SELECT
  FORECAST_DAY_DT AS FORECAST_DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  WEEK_DT AS WEEK_DT,
  STORE_NBR AS STORE_NBR,
  DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  OVERNIGHT_GUEST_CNT AS OVERNIGHT_GUEST_CNT,
  OVERNIGHT_WITH_DDC_CNT AS OVERNIGHT_WITH_DDC_CNT,
  DAY_GUEST_CNT AS DAY_GUEST_CNT,
  DAY_CARE_CNT AS DAY_CARE_CNT,
  DAY_CAMP_CNT AS DAY_CAMP_CNT,
  TOTAL_DDC_GUEST_CNT AS TOTAL_DDC_GUEST_CNT,
  TOTAL_GUEST_CNT AS TOTAL_GUEST_CNT,
  REQUIRED_PLAYROOM_CNT AS REQUIRED_PLAYROOM_CNT
FROM
  PS2_HTL_FORECAST_PRE"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("Shortcut_to_PS2_HTL_FORECAST_PRE_1")

# COMMAND ----------
# DBTITLE 1, Shortcut_To_DAYS_2


query_2 = f"""SELECT
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

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("Shortcut_To_DAYS_2")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PS2_HTL_FORECAST_PRE_3


query_3 = f"""SELECT
  DISTINCT Shortcut_to_PS2_HTL_FORECAST_PRE_1.FORECAST_DAY_DT AS FORECAST_DAY_DT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.LOCATION_ID AS LOCATION_ID,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.WEEK_DT AS WEEK_DT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.STORE_NBR AS STORE_NBR,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.OVERNIGHT_GUEST_CNT AS OVERNIGHT_GUEST_CNT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.OVERNIGHT_WITH_DDC_CNT AS OVERNIGHT_WITH_DDC_CNT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.DAY_GUEST_CNT AS DAY_GUEST_CNT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.DAY_CARE_CNT AS DAY_CARE_CNT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.DAY_CAMP_CNT AS DAY_CAMP_CNT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.TOTAL_DDC_GUEST_CNT AS TOTAL_DDC_GUEST_CNT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.TOTAL_GUEST_CNT AS TOTAL_GUEST_CNT,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.REQUIRED_PLAYROOM_CNT AS REQUIRED_PLAYROOM_CNT,
  Shortcut_To_DAYS_2.FISCAL_WK AS FISCAL_WK,
  Shortcut_To_DAYS_2.FISCAL_MO AS FISCAL_MO,
  Shortcut_To_DAYS_2.FISCAL_YR AS FISCAL_YR,
  Shortcut_To_DAYS_2.DAY_DT AS DAY_DT,
  Shortcut_to_PS2_HTL_VARIABLES_0.PS2_HTL_VAR_REC_EFF_DT AS PS2_HTL_VAR_REC_EFF_DT,
  Shortcut_to_PS2_HTL_VARIABLES_0.PS2_HTL_VAR_REC_END_DT AS PS2_HTL_VAR_REC_END_DT,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PS2_HTL_VARIABLES_0,
  Shortcut_To_DAYS_2,
  Shortcut_to_PS2_HTL_FORECAST_PRE_1
WHERE
  Shortcut_to_PS2_HTL_FORECAST_PRE_1.FORECAST_DAY_DT = Shortcut_To_DAYS_2.DAY_DT
  AND Shortcut_to_PS2_HTL_FORECAST_PRE_1.FORECAST_DAY_DT >= Shortcut_to_PS2_HTL_VARIABLES_0.PS2_HTL_VAR_REC_EFF_DT
  AND Shortcut_to_PS2_HTL_FORECAST_PRE_1.FORECAST_DAY_DT <= Shortcut_to_PS2_HTL_VARIABLES_0.PS2_HTL_VAR_REC_END_DT"""

df_3 = spark.sql(query_3)

df_3.createOrReplaceTempView("SQ_Shortcut_to_PS2_HTL_FORECAST_PRE_3")

# COMMAND ----------
# DBTITLE 1, LKP_PS2_HTL_VARIABLES_FLAG_Y_4


query_4 = f"""SELECT
  PHV.STORE_NBR AS STORE_NBR,
  PHV.DEFAULT_STORE_FLAG AS DEFAULT_STORE_FLAG,
  PHV.PS2_HTL_VAR_REC_EFF_DT AS PS2_HTL_VAR_REC_EFF_DT,
  PHV.PS2_HTL_VAR_REC_END_DT AS PS2_HTL_VAR_REC_END_DT,
  PHV.TY_WEIGHT_PCNT AS TY_WEIGHT_PCNT,
  PHV.LY_WEIGHT_PCNT AS LY_WEIGHT_PCNT,
  PHV.FRONT_DESK_FIXED_HRS AS FRONT_DESK_FIXED_HRS,
  PHV.FRONT_DESK_VAR_HRS AS FRONT_DESK_VAR_HRS,
  PHV.PLAYROOM_VAR_HRS AS PLAYROOM_VAR_HRS,
  PHV.BACK_OF_HOUSE_FIXED_HRS AS BACK_OF_HOUSE_FIXED_HRS,
  PHV.BACK_OF_HOUSE_VAR_HRS AS BACK_OF_HOUSE_VAR_HRS,
  PHV.OVERNIGHT_TRESHHOLD AS OVERNIGHT_TRESHHOLD,
  PHV.OVERNIGHT_UPPER_VALUE AS OVERNIGHT_UPPER_VALUE,
  PHV.OVERNIGHT_LOWER_VALUE AS OVERNIGHT_LOWER_VALUE,
  PHV.SUPERVISOR_HRS AS SUPERVISOR_HRS,
  PHV.LOAD_TSTMP AS LOAD_TSTMP,
  SStPHFP3.DAY_DT AS DAY_DT,
  SStPHFP3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PS2_HTL_FORECAST_PRE_3 SStPHFP3
  LEFT JOIN PS2_HTL_VARIABLES PHV ON PHV.PS2_HTL_VAR_REC_EFF_DT <= SStPHFP3.DAY_DT
  AND PHV.PS2_HTL_VAR_REC_END_DT >= SStPHFP3.DAY_DT"""

df_4 = spark.sql(query_4)

df_4.createOrReplaceTempView("LKP_PS2_HTL_VARIABLES_FLAG_Y_4")

# COMMAND ----------
# DBTITLE 1, LKP_PS2_HTL_VARIABLES_FLAG_N_5


query_5 = f"""SELECT
  PHV.STORE_NBR AS STORE_NBR,
  PHV.DEFAULT_STORE_FLAG AS DEFAULT_STORE_FLAG,
  PHV.PS2_HTL_VAR_REC_EFF_DT AS PS2_HTL_VAR_REC_EFF_DT,
  PHV.PS2_HTL_VAR_REC_END_DT AS PS2_HTL_VAR_REC_END_DT,
  PHV.TY_WEIGHT_PCNT AS TY_WEIGHT_PCNT,
  PHV.LY_WEIGHT_PCNT AS LY_WEIGHT_PCNT,
  PHV.FRONT_DESK_FIXED_HRS AS FRONT_DESK_FIXED_HRS,
  PHV.FRONT_DESK_VAR_HRS AS FRONT_DESK_VAR_HRS,
  PHV.PLAYROOM_VAR_HRS AS PLAYROOM_VAR_HRS,
  PHV.BACK_OF_HOUSE_FIXED_HRS AS BACK_OF_HOUSE_FIXED_HRS,
  PHV.BACK_OF_HOUSE_VAR_HRS AS BACK_OF_HOUSE_VAR_HRS,
  PHV.OVERNIGHT_TRESHHOLD AS OVERNIGHT_TRESHHOLD,
  PHV.OVERNIGHT_UPPER_VALUE AS OVERNIGHT_UPPER_VALUE,
  PHV.OVERNIGHT_LOWER_VALUE AS OVERNIGHT_LOWER_VALUE,
  PHV.SUPERVISOR_HRS AS SUPERVISOR_HRS,
  PHV.LOAD_TSTMP AS LOAD_TSTMP,
  SStPHFP3.DAY_DT AS IN_DAY_DT,
  SStPHFP3.STORE_NBR AS IN_STORE_NBR1,
  SStPHFP3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PS2_HTL_FORECAST_PRE_3 SStPHFP3
  LEFT JOIN PS2_HTL_VARIABLES PHV ON PHV.STORE_NBR = SStPHFP3.STORE_NBR
  AND PHV.PS2_HTL_VAR_REC_EFF_DT <= SStPHFP3.DAY_DT
  AND PHV.PS2_HTL_VAR_REC_END_DT >= SStPHFP3.DAY_DT"""

df_5 = spark.sql(query_5)

df_5.createOrReplaceTempView("LKP_PS2_HTL_VARIABLES_FLAG_N_5")

# COMMAND ----------
# DBTITLE 1, EXP_DeriveColumns_6


query_6 = f"""SELECT
  SStPHFP3.FORECAST_DAY_DT AS FORECAST_DAY_DT,
  SStPHFP3.LOCATION_ID AS LOCATION_ID,
  SStPHFP3.WEEK_DT AS WEEK_DT,
  SStPHFP3.STORE_NBR AS STORE_NBR,
  SStPHFP3.DAY_OF_WK_NBR AS DAY_OF_WK_NBR,
  SStPHFP3.OVERNIGHT_GUEST_CNT AS OVERNIGHT_GUEST_CNT,
  SStPHFP3.OVERNIGHT_WITH_DDC_CNT AS OVERNIGHT_WITH_DDC_CNT,
  SStPHFP3.DAY_GUEST_CNT AS DAY_GUEST_CNT,
  SStPHFP3.DAY_CARE_CNT AS DAY_CARE_CNT,
  SStPHFP3.DAY_CAMP_CNT AS DAY_CAMP_CNT,
  SStPHFP3.TOTAL_DDC_GUEST_CNT AS TOTAL_DDC_GUEST_CNT,
  SStPHFP3.TOTAL_GUEST_CNT AS TOTAL_GUEST_CNT,
  SStPHFP3.REQUIRED_PLAYROOM_CNT AS REQUIRED_PLAYROOM_CNT,
  SStPHFP3.FISCAL_WK AS FISCAL_WK,
  SStPHFP3.FISCAL_MO AS FISCAL_MO,
  SStPHFP3.FISCAL_YR AS FISCAL_YR,
  LPHVFN5.STORE_NBR AS STORE_NBR1,
  LPHVFN5.FRONT_DESK_FIXED_HRS AS GSA_FIXED_HRS,
  LPHVFN5.FRONT_DESK_VAR_HRS AS GSA_VAR_HRS,
  LPHVFN5.PLAYROOM_VAR_HRS AS PLAYROOM_VAR_HRS,
  LPHVFN5.BACK_OF_HOUSE_FIXED_HRS AS BACK_OF_HOUSE_FIXED_HRS,
  LPHVFN5.BACK_OF_HOUSE_VAR_HRS AS BACK_OF_HOUSE_VAR_HRS,
  LPHVFN5.OVERNIGHT_TRESHHOLD AS OVERNIGHT_TRESHHOLD,
  LPHVFN5.OVERNIGHT_UPPER_VALUE AS OVERNIGHT_UPPER_VALUE,
  LPHVFN5.OVERNIGHT_LOWER_VALUE AS OVERNIGHT_LOWER_VALUE,
  LPHVFN5.SUPERVISOR_HRS AS SUPERVISOR_HRS,
  IFF(
    ISNULL(LPHVFN5.STORE_NBR),
    LPHVFY4.FRONT_DESK_FIXED_HRS,
    LPHVFN5.FRONT_DESK_FIXED_HRS
  ) + (
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.FRONT_DESK_VAR_HRS,
      LPHVFN5.FRONT_DESK_VAR_HRS
    ) * SStPHFP3.TOTAL_GUEST_CNT
  ) AS GSA_HRS,
  IFF(
    ISNULL(LPHVFN5.STORE_NBR),
    LPHVFY4.PLAYROOM_VAR_HRS,
    LPHVFN5.PLAYROOM_VAR_HRS
  ) * SStPHFP3.REQUIRED_PLAYROOM_CNT AS PLAYROOM_HRS,
  IFF(
    ISNULL(LPHVFN5.STORE_NBR),
    LPHVFY4.BACK_OF_HOUSE_FIXED_HRS,
    LPHVFN5.BACK_OF_HOUSE_FIXED_HRS
  ) + (
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.BACK_OF_HOUSE_VAR_HRS,
      LPHVFN5.BACK_OF_HOUSE_VAR_HRS
    ) * SStPHFP3.OVERNIGHT_GUEST_CNT
  ) AS BACK_OF_HOUSE_HRS,
  IFF(
    SStPHFP3.OVERNIGHT_GUEST_CNT > IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.OVERNIGHT_TRESHHOLD,
      LPHVFN5.OVERNIGHT_TRESHHOLD
    ),
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.OVERNIGHT_UPPER_VALUE,
      LPHVFN5.OVERNIGHT_UPPER_VALUE
    ),
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.OVERNIGHT_LOWER_VALUE,
      LPHVFN5.OVERNIGHT_LOWER_VALUE
    )
  ) AS OVERNIGHT_HRS,
  IFF(
    ISNULL(LPHVFN5.STORE_NBR),
    LPHVFY4.FRONT_DESK_FIXED_HRS,
    LPHVFN5.FRONT_DESK_FIXED_HRS
  ) + (
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.FRONT_DESK_VAR_HRS,
      LPHVFN5.FRONT_DESK_VAR_HRS
    ) * SStPHFP3.TOTAL_GUEST_CNT
  ) + (
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.PLAYROOM_VAR_HRS,
      LPHVFN5.PLAYROOM_VAR_HRS
    ) * SStPHFP3.REQUIRED_PLAYROOM_CNT
  ) + IFF(
    ISNULL(LPHVFN5.STORE_NBR),
    LPHVFY4.BACK_OF_HOUSE_FIXED_HRS,
    LPHVFN5.BACK_OF_HOUSE_FIXED_HRS
  ) + (
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.BACK_OF_HOUSE_VAR_HRS,
      LPHVFN5.BACK_OF_HOUSE_VAR_HRS
    ) * SStPHFP3.OVERNIGHT_GUEST_CNT
  ) + IFF(
    SStPHFP3.OVERNIGHT_GUEST_CNT > IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.OVERNIGHT_TRESHHOLD,
      LPHVFN5.OVERNIGHT_TRESHHOLD
    ),
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.OVERNIGHT_UPPER_VALUE,
      LPHVFN5.OVERNIGHT_UPPER_VALUE
    ),
    IFF(
      ISNULL(LPHVFN5.STORE_NBR),
      LPHVFY4.OVERNIGHT_LOWER_VALUE,
      LPHVFN5.OVERNIGHT_LOWER_VALUE
    )
  ) + IFF(
    ISNULL(LPHVFN5.STORE_NBR),
    LPHVFY4.SUPERVISOR_HRS,
    LPHVFN5.SUPERVISOR_HRS
  ) AS FORECAST_HRS,
  now() AS LOAD_TSTMP,
  LPHVFY4.FRONT_DESK_FIXED_HRS AS GSA_FIXED_HRS1,
  LPHVFY4.FRONT_DESK_VAR_HRS AS GSA_VAR_HRS1,
  LPHVFY4.PLAYROOM_VAR_HRS AS PLAYROOM_VAR_HRS1,
  LPHVFY4.BACK_OF_HOUSE_FIXED_HRS AS BACK_OF_HOUSE_FIXED_HRS1,
  LPHVFY4.BACK_OF_HOUSE_VAR_HRS AS BACK_OF_HOUSE_VAR_HRS1,
  LPHVFY4.OVERNIGHT_TRESHHOLD AS OVERNIGHT_TRESHHOLD1,
  LPHVFY4.OVERNIGHT_UPPER_VALUE AS OVERNIGHT_UPPER_VALUE1,
  LPHVFY4.OVERNIGHT_LOWER_VALUE AS OVERNIGHT_LOWER_VALUE1,
  LPHVFY4.SUPERVISOR_HRS AS SUPERVISOR_HRS1,
  IFF(ISNULL(STORE_NBR1), GSA_FIXED_HRS1, GSA_FIXED_HRS) AS v_GSA_FIXED_HRS,
  IFF(ISNULL(STORE_NBR1), GSA_VAR_HRS1, GSA_VAR_HRS) AS v_GSA_VAR_HRS,
  IFF(
    ISNULL(STORE_NBR1),
    PLAYROOM_VAR_HRS1,
    PLAYROOM_VAR_HRS
  ) AS v_PLAYROOM_VAR_HRS,
  IFF(
    ISNULL(STORE_NBR1),
    BACK_OF_HOUSE_FIXED_HRS1,
    BACK_OF_HOUSE_FIXED_HRS
  ) AS v_BACK_OF_HOUSE_FIXED_HRS,
  IFF(
    ISNULL(STORE_NBR1),
    BACK_OF_HOUSE_VAR_HRS1,
    BACK_OF_HOUSE_VAR_HRS
  ) AS v_BACK_OF_HOUSE_VAR_HRS,
  IFF(
    ISNULL(STORE_NBR1),
    OVERNIGHT_TRESHHOLD1,
    OVERNIGHT_TRESHHOLD
  ) AS v_OVERNIGHT_TRESHHOLD,
  IFF(
    ISNULL(STORE_NBR1),
    OVERNIGHT_UPPER_VALUE1,
    OVERNIGHT_UPPER_VALUE
  ) AS v_OVERNIGHT_UPPER_VALUE,
  IFF(
    ISNULL(STORE_NBR1),
    OVERNIGHT_LOWER_VALUE1,
    OVERNIGHT_LOWER_VALUE
  ) AS v_OVERNIGHT_LOWER_VALUE,
  IFF(
    ISNULL(STORE_NBR1),
    SUPERVISOR_HRS1,
    SUPERVISOR_HRS
  ) AS v_SUPERVISOR_HRS,
  IFF(
    ISNULL(LPHVFN5.STORE_NBR),
    LPHVFY4.SUPERVISOR_HRS,
    LPHVFN5.SUPERVISOR_HRS
  ) AS o_SUPERVISOR_HRS,
  SStPHFP3.Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PS2_HTL_FORECAST_PRE_3 SStPHFP3
  INNER JOIN LKP_PS2_HTL_VARIABLES_FLAG_N_5 LPHVFN5 ON SStPHFP3.Monotonically_Increasing_Id = LPHVFN5.Monotonically_Increasing_Id
  INNER JOIN LKP_PS2_HTL_VARIABLES_FLAG_Y_4 LPHVFY4 ON LPHVFN5.Monotonically_Increasing_Id = LPHVFY4.Monotonically_Increasing_Id"""

df_6 = spark.sql(query_6)

df_6.createOrReplaceTempView("EXP_DeriveColumns_6")

# COMMAND ----------
# DBTITLE 1, PS2_HTL_FORECAST


spark.sql("""INSERT INTO
  PS2_HTL_FORECAST
SELECT
  FORECAST_DAY_DT AS FORECAST_DAY_DT,
  FORECAST_DAY_DT AS FORECAST_DAY_DT,
  LOCATION_ID AS LOCATION_ID,
  LOCATION_ID AS LOCATION_ID,
  STORE_NBR AS STORE_NBR,
  WEEK_DT AS WEEK_DT,
  FISCAL_WK AS FISCAL_WK,
  FISCAL_MO AS FISCAL_MO,
  FISCAL_YR AS FISCAL_YR,
  OVERNIGHT_GUEST_CNT AS OVERNIGHT_GUEST_CNT,
  OVERNIGHT_WITH_DDC_CNT AS OVERNIGHT_WITH_DDC_CNT,
  DAY_GUEST_CNT AS DAY_GUEST_CNT,
  DAY_CARE_CNT AS DAY_CARE_CNT,
  DAY_CAMP_CNT AS DAY_CAMP_CNT,
  TOTAL_DDC_GUEST_CNT AS TOTAL_DDC_GUEST_CNT,
  TOTAL_GUEST_CNT AS TOTAL_GUEST_CNT,
  REQUIRED_PLAYROOM_CNT AS REQUIRED_PLAYROOM_CNT,
  GSA_HRS AS FRONT_DESK_HRS,
  PLAYROOM_HRS AS PLAYROOM_HRS,
  BACK_OF_HOUSE_HRS AS BACK_OF_HOUSE_HRS,
  OVERNIGHT_HRS AS OVERNIGHT_HRS,
  o_SUPERVISOR_HRS AS SUPERVISOR_HRS,
  FORECAST_HRS AS FORECAST_HRS,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_DeriveColumns_6""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_ps2_htl_forecast_INSERT")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_ps2_htl_forecast_INSERT", mainWorkflowId, parentName)
