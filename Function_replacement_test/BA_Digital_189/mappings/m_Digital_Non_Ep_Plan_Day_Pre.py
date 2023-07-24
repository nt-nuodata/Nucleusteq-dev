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
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Digital_Non_Ep_Plan_Day_Pre")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_Digital_Non_Ep_Plan_Day_Pre", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_PlanbyDayNonEP_0


query_0 = f"""SELECT
  Fiscal_Week AS Fiscal_Week,
  Day_Dt AS Day_Dt,
  Day_of_Week AS Day_of_Week,
  US_STH_Sales_amt AS US_STH_Sales_amt,
  US_STH_Margin_amt AS US_STH_Margin_amt,
  US_Rx_Sales_amt AS US_Rx_Sales_amt,
  US_Rx_Margin_amt AS US_Rx_Margin_amt,
  US_Autoship_Sales_amt AS US_Autoship_Sales_amt,
  US_Autoship_Margin_amt AS US_Autoship_Margin_amt
FROM
  PlanbyDayNonEP"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_PlanbyDayNonEP_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_PlanbyDayNonEP_1


query_1 = f"""SELECT
  Fiscal_Week AS Fiscal_Week,
  Day_Dt AS Day_Dt,
  Day_of_Week AS Day_of_Week,
  US_STH_Sales_amt AS US_STH_Sales_amt,
  US_STH_Margin_amt AS US_STH_Margin_amt,
  US_Rx_Sales_amt AS US_Rx_Sales_amt,
  US_Rx_Margin_amt AS US_Rx_Margin_amt,
  US_Autoship_Sales_amt AS US_Autoship_Sales_amt,
  US_Autoship_Margin_amt AS US_Autoship_Margin_amt,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_PlanbyDayNonEP_0"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_PlanbyDayNonEP_1")

# COMMAND ----------
# DBTITLE 1, EXP_LOAD_TSTMP_2


query_2 = f"""SELECT
  TO_DATE(Day_Dt, 'MM/DD/YYYY') AS o_DAY_DT,
  Fiscal_Week AS Fiscal_Week,
  Day_of_Week AS Day_of_Week,
  TO_DECIMAL(REPLACECHR(TRUE, US_STH_Sales_amt, ',', '')) AS o_US_STH_Sales_amt,
  TO_DECIMAL(REPLACECHR(TRUE, US_STH_Margin_amt, ',', '')) AS o_US_STH_Margin_amt,
  TO_DECIMAL(REPLACECHR(TRUE, US_Rx_Sales_amt, ',', '')) AS o_US_Rx_Sales_amt,
  TO_DECIMAL(REPLACECHR(TRUE, US_Rx_Margin_amt, ',', '')) AS o_US_Rx_Margin_amt,
  TO_DECIMAL(REPLACECHR(TRUE, US_Autoship_Sales_amt, ',', '')) AS o_US_Autoship_Sales_amt,
  TO_DECIMAL(REPLACECHR(TRUE, US_Autoship_Margin_amt, ',', '')) AS o_US_Autoship_Margin_amt,
  now() AS LOAD_TSTMP,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_PlanbyDayNonEP_1"""

df_2 = spark.sql(query_2)

df_2.createOrReplaceTempView("EXP_LOAD_TSTMP_2")

# COMMAND ----------
# DBTITLE 1, DIGITAL_NON_EP_PLAN_DAY_PRE


spark.sql("""INSERT INTO
  DIGITAL_NON_EP_PLAN_DAY_PRE
SELECT
  o_DAY_DT AS DAY_DT,
  Fiscal_Week AS FISCAL_WK,
  Day_of_Week AS DAY_OF_WEEK,
  o_US_STH_Sales_amt AS US_STH_SALES_AMT,
  o_US_STH_Margin_amt AS US_STH_MARGIN_AMT,
  o_US_Rx_Sales_amt AS US_RX_SALES_AMT,
  o_US_Rx_Margin_amt AS US_RX_MARGIN_AMT,
  o_US_Autoship_Sales_amt AS US_AUTOSHIP_SALES_AMT,
  o_US_Autoship_Margin_amt AS US_AUTOSHIP_MARGIN_AMT,
  LOAD_TSTMP AS LOAD_TSTMP
FROM
  EXP_LOAD_TSTMP_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_Digital_Non_Ep_Plan_Day_Pre")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_Digital_Non_Ep_Plan_Day_Pre", mainWorkflowId, parentName)
