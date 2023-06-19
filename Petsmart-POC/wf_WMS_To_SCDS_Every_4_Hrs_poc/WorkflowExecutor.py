# Databricks notebook source
# MAGIC %run ./WorkflowUtility

# COMMAND ----------

import json
def readWorkflowJson(workflowName):
    f = open(f"/Workspace/Shared/WMS-Petsmart-POC/Workflow2/workflow_jsons/{workflowName}.json")
    return json.load(f)

# COMMAND ----------

def addNotebookBaseParameters(variables, workflow):
    wf_json = workflow['wf_json']
    if(workflow['type'] == "mainWorkflow"):
        base_parameters = {"mainWorkflowId": "{{job_id}}", "mainWorkflowRunId": "{{run_id}}", "logTableName": "logTableName", "parentName": "name","variablesTableName": "variablesTableName","workflowName" : "workflowName"}
        for variable in variables:
            if(variable == 'name'):
                base_parameters['parentName'] = variables['name']
            else:
                base_parameters[variable] = variables[variable]
    elif workflow['type'] == "subWorkflow" :
        base_parameters = {"mainWorkflowId": "", "mainWorkflowRunId": "", "logTableName" : "", "parentName" : "","variablesTableName" : ""}
        for variable in variables:
            if(variable == 'parentName'):
                base_parameters['parentName'] = wf_json['run_name']
            else:
                base_parameters[variable] = variables[variable]

    for task in wf_json['tasks']:
        base_parameters['workflowName'] = task['task_key']
        parameters_list = {}
        if('base_parameters' in task['notebook_task']):
            parameters_list.update(task['notebook_task']['base_parameters'])
        parameters_list.update(base_parameters)
        task['notebook_task']['base_parameters'] = parameters_list
    return wf_json  

# COMMAND ----------

def main(workflowName):

    #Extract workflow json from json file.
    workflow = readWorkflowJson(workflowName)

    if(workflow['type'] == "mainWorkflow"):

        #Extract workflow variables.
        variables = workflow['variables']

        #Creating log table.
        logTableName = variables['logTableName']
        createLogTable(logTableName)

        wf_json = workflow['wf_json']

        #Add required notebook parametres
        wf_json = addNotebookBaseParameters(variables, workflow)

        # Create job
        job_id_json = createJob(wf_json)
        print("Job Created")

        # Create variables table using the parameters file provided.
        createVariableTable(variables['parameterFileName'], variables['variablesTableName'], job_id_json)
        print("Variables Table Created")

        #Run job
        run_id = runJob(job_id_json)
        print("Job run started with run id : "+ str(run_id))

        #Check job status
        job_id = json.loads(job_id_json)['job_id'] 

        checkJobStatus(run_id, job_id, run_id, logTableName)

        # Update Workflow Variables in database.
        persistVariables(variables['variablesTableName'], workflowName, job_id, '')
    elif (workflow['type'] == "subWorkflow"):

        #Extraction of widgets
        widgets_variable_names = ["mainWorkflowId", "mainWorkflowRunId", "logTableName", "parentName", "variablesTableName"]
        widgets = {}
        for variable_name in widgets_variable_names:
            widgets[variable_name] = dbutils.widgets.get(variable_name)

        #Pre worklet variable updation
        if (len(workflow['preWorkletVariableUpdation']) != 0):
            preWorkletVariableUpdation = workflow['preWorkletVariableUpdation']
            updateVariable(preWorkletVariableUpdation, widgets['variablesTableName'], widgets['mainWorkflowId'], widgets['parentName'], workflowName)

        wf_json = workflow['wf_json']

        #Add required notebook parametres
        wf_json = addNotebookBaseParameters(widgets, workflow)

        # Create and submit a one time run.
        run_id = oneTimeRun(wf_json)

        #Check Run Status.
        checkJobStatus(run_id, widgets['mainWorkflowId'], widgets['mainWorkflowRunId'],widgets['logTableName'])

        #Post worklet variable updation
        if (len(workflow['postWorkletVariableUpdation']) != 0):
            postWorkletVariableUpdation = workflow['postWorkletVariableUpdation']
            updateVariable(postWorkletVariableUpdation, widgets['variablesTableName'], widgets['mainWorkflowId'], widgets['parentName'], workflowName)

        # Update Workflow Variables in database.
        persistVariables(widgets['variablesTableName'], workflowName, widgets['mainWorkflowId'], widgets['parentName'])


# COMMAND ----------

workflowName = dbutils.widgets.get("workflowName")
main(workflowName)