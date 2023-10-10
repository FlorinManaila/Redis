import boto3
import cfnresponse
import time
import json
import requests
import os
from os import environ

accept = "application/json"
content_type = "application/json"
runtime_region = os.environ['AWS_REGION']
stepfunctions = boto3.client("stepfunctions")

def lambda_handler (event, context):
    
    print (event)
    aws_account_id = context.invoked_function_arn.split(":")[4]
    
    if event['ResourceProperties']["dryRun"] == "true":
        event['ResourceProperties']["dryRun"] = True
    elif event['ResourceProperties']["dryRun"] == "false":
        event['ResourceProperties']["dryRun"] = False
        
    if event['ResourceProperties']["multipleAvailabilityZones"] == "true":
        event['ResourceProperties']["multipleAvailabilityZones"] = True
    elif event['ResourceProperties']["multipleAvailabilityZones"] == "false":
        event['ResourceProperties']["multipleAvailabilityZones"] = False
        
    if event['ResourceProperties']["supportOSSClusterApi"] == "true":
        event['ResourceProperties']["supportOSSClusterApi"] = True
    elif event['ResourceProperties']["supportOSSClusterApi"] == "false":
        event['ResourceProperties']["supportOSSClusterApi"] = False
        
    if event['ResourceProperties']["replication"] == "true":
        event['ResourceProperties']["replication"] = True
    elif event['ResourceProperties']["replication"] == "false":
        event['ResourceProperties']["replication"] = False
    
    networking = {}
    networking["deploymentCIDR"] = event['ResourceProperties']["deploymentCIDR"]
    if "vpcId" in event['ResourceProperties']:
        networking["vpcId"] = event['ResourceProperties']["vpcId"]
    
    regionsList = []
    regionsDict = {}
    regionsDict["region"] = event['ResourceProperties']["region"]
    if "multipleAvailabilityZones" in event['ResourceProperties']:
        regionsDict["multipleAvailabilityZones"] = event['ResourceProperties']["multipleAvailabilityZones"]
    if "preferredAvailabilityZones" in event['ResourceProperties']:
        regionsDict["preferredAvailabilityZones"] = event['ResourceProperties']["preferredAvailabilityZones"]
    regionsDict["networking"] = networking
    regionsList.append(regionsDict)
    
    cloudProvidersList = []
    cloudProvidersDict = {}
    if "provider" in event['ResourceProperties']:
        cloudProvidersDict["provider"] = event['ResourceProperties']["provider"]
    if "cloudAccountId" in event['ResourceProperties']:
        cloudProvidersDict["cloudAccountId"] = int(event['ResourceProperties']["cloudAccountId"])
    cloudProvidersDict["regions"] = regionsList
    cloudProvidersList.append(cloudProvidersDict)
    
    throughputMeasurement = {}
    if "by" in event['ResourceProperties']:
        throughputMeasurement["by"] = event['ResourceProperties']["by"]
    if "value" in event['ResourceProperties']:
        throughputMeasurement["value"] = int(event['ResourceProperties']["value"])
    
    modulesList = []
    modulesDict = {}
    if "moduleName" in event['ResourceProperties']:
        modulesDict["name"] = event['ResourceProperties']["moduleName"]
    if "parameters" in event['ResourceProperties']: 
        modulesDict["parameters"] = event['ResourceProperties']["parameters"]
    modulesList.append(modulesDict)
    
    databasesList = []
    databasesDict = {}
    databasesDict["name"] = event['ResourceProperties']["dbname"]
    if "protocol" in event['ResourceProperties']:
        databasesDict["protocol"] = event['ResourceProperties']["protocol"]
    databasesDict["memoryLimitInGb"] = int(event['ResourceProperties']["memoryLimitInGb"])
    if "supportOSSClusterApi" in event['ResourceProperties']:
        databasesDict["supportOSSClusterApi"] = event['ResourceProperties']["supportOSSClusterApi"]
    if "dataPersistence" in event['ResourceProperties']:
        databasesDict["dataPersistence"] = event['ResourceProperties']["dataPersistence"]
    if "replication" in event['ResourceProperties']:
        databasesDict["replication"] = event['ResourceProperties']["replication"]
    if "by" in event['ResourceProperties']:
        databasesDict["throughputMeasurement"] = throughputMeasurement
    if "moduleName" in event['ResourceProperties']:
        databasesDict["modules"] = modulesList
    if "quantity" in event['ResourceProperties']:
        databasesDict["quantity"] = int(event['ResourceProperties']["quantity"])
    if "averageItemSizeInBytes" in event['ResourceProperties']:
        databasesDict["averageItemSizeInBytes"] = int(event['ResourceProperties']["averageItemSizeInBytes"])
    databasesList.append(databasesDict)
    
    callEvent = {}
    if "subName" in event['ResourceProperties']:
        callEvent["name"] = event['ResourceProperties']["subName"]
    if "dryRun" in event['ResourceProperties']:
        callEvent["dryRun"] = event['ResourceProperties']["dryRun"]
    if "deploymentType" in event['ResourceProperties']:
        callEvent["deploymentType"] = event['ResourceProperties']["deploymentType"]
    if "paymentMethod" in event['ResourceProperties']:
        callEvent["paymentMethod"] = event['ResourceProperties']["paymentMethod"]
    if "paymentMethodId" in event['ResourceProperties']:
        callEvent["paymentMethodId"] = int(event['ResourceProperties']["paymentMethodId"])
    if "paymentMethodId" in event['ResourceProperties']:
        callEvent["memoryStorage"] = event['ResourceProperties']["memoryStorage"]
    callEvent["cloudProviders"] = cloudProvidersList
    callEvent["databases"] = databasesList
    if "redisVersion" in event['ResourceProperties']:
        callEvent["redisVersion"] = event['ResourceProperties']["redisVersion"]

    print ("callEvent that is used as the actual API Call is bellow:")
    print (callEvent)
    
    global stack_name
    global base_url
    global x_api_key
    global x_api_secret_key 
    base_url = event['ResourceProperties']['baseURL']
    x_api_key =  RetrieveSecret("redis/x_api_key")["x_api_key"]
    x_api_secret_key =  RetrieveSecret("redis/x_api_secret_key")["x_api_secret_key"]
    stack_name = str(event['StackId'].split("/")[1])
    responseData = {}
    
    responseStatus = 'SUCCESS'
    responseURL = event['ResponseURL']
    responseBody = {'Status': responseStatus,
                    'PhysicalResourceId': context.log_stream_name,
                    'StackId': event['StackId'],
                    'RequestId': event['RequestId'],
                    'LogicalResourceId': event['LogicalResourceId']
                    }
    
    if event['RequestType'] == "Create":
        responseValue = PostSubscription(callEvent)
        print (responseValue) 
        
        try:        
            sub_id, sub_description = GetSubscriptionId (responseValue['links'][0]['href'])
            default_db_id = GetDatabaseId(sub_id)
            print ("New sub id is: " + str(sub_id))
            print ("Description for Subscription with id " + str(sub_id) + " is: " + str(sub_description))
                    
            responseData.update({"SubscriptionId":str(sub_id), "DefaultDatabaseId":str(default_db_id), "SubscriptionDescription":str(sub_description), "PostCall":str(callEvent)})
            responseBody.update({"Data":responseData})
            SFinput = {}
            SFinput["responseBody"] = responseBody
            SFinput["responseURL"] = responseURL
            SFinput["base_url"] = event['ResourceProperties']['baseURL']
            response = stepfunctions.start_execution(
                stateMachineArn = f'arn:aws:states:{runtime_region}:{aws_account_id}:stateMachine:FlexibleSubscription-StateMachine-{runtime_region}-{stack_name}',
                name = f'FlexibleSubscription-StateMachine-{runtime_region}-{stack_name}',
                input = json.dumps(SFinput)
                )
            print ("Output sent to Step Functions is the following:")
            print (json.dumps(SFinput))
                        
        except:
            sub_error = GetSubscriptionError (responseValue['links'][0]['href'])
            responseStatus = 'FAILED'
            reason = str(sub_error)
            if responseStatus == 'FAILED':
                responseBody.update({"Status":responseStatus})
                if "Reason" in str(responseBody):
                    responseBody.update({"Reason":reason})
                else:
                    responseBody["Reason"] = reason
                GetResponse(responseURL, responseBody)

    if event['RequestType'] == "Update":
        cf_sub_id, cf_event, cf_db_id, cf_sub_description = CurrentOutputs()
        PhysicalResourceId = event['PhysicalResourceId']
        responseBody.update({"PhysicalResourceId":PhysicalResourceId})
        sub_status = GetSubscriptionStatus(cf_sub_id)
        
        if str(sub_status) == "active":
            responseValue = PutSubscription(cf_sub_id, callEvent)
            print ("This is the event key after PUT call:")
            print (callEvent)
            cf_event = cf_event.replace("\'", "\"")
            cf_event = cf_event.replace("False", "false")
            cf_event = cf_event.replace("True", "true")
            cf_event = json.loads(cf_event)
            
            cf_event.update(callEvent)
            print (cf_event)
            
            responseData.update({"SubscriptionId":str(cf_sub_id), "DefaultDatabaseId":str(cf_db_id), "SubscriptionDescription":str(cf_sub_description), "PostCall":str(cf_event)})
            print (responseData)
            responseBody.update({"Data":responseData})
            
            GetResponse(responseURL, responseBody)
            
        elif str(sub_status) == "pending":
            responseValue = PutSubscription(cf_sub_id, callEvent)
            print ("this is response value for update in pending")
            print (responseValue)
            sub_error = GetSubscriptionError (responseValue['links'][0]['href'])
            responseStatus = 'FAILED'
            reason = str(sub_error)
            if responseStatus == 'FAILED':
                responseBody.update({"Status":responseStatus})
                if "Reason" in str(responseBody):
                    responseBody.update({"Reason":reason})
                else:
                    responseBody["Reason"] = reason
                GetResponse(responseURL, responseBody)
                
        elif str(sub_status) == "deleting":
            responseValue = PutSubscription(cf_sub_id, callEvent)
            sub_error = GetSubscriptionError (responseValue['links'][0]['href'])
            responseStatus = 'FAILED'
            reason = str(sub_error)
            if responseStatus == 'FAILED':
                responseBody.update({"Status":responseStatus})
                if "Reason" in str(responseBody):
                    responseBody.update({"Reason":reason})
                else:
                    responseBody["Reason"] = reason
                GetResponse(responseURL, responseBody)
        
    
    # #DELETE API call
    # 2 cases for error handling:
    # 1. if Subscription does not exists -> DELETE_COMPLETE
    # 2. IF DeleteSubscription call drops, then throw error: DELETE_FAILED (ROLLBACK DO NOTHING)
    # ! for to delete all DB before deleting subscriptions
    # DACA LISTA DE DB > 1 -> DACA =1 -> VERIFICA DACA E CEA DEFAULT -> DACA E DEFAULT -> DELETE ELSE FAILED AND ANNOUNCE THE ERROR. 
    # NU CAUTA SUBS SI DB ID INAINTE. Direct Delete doar daca singura baza de date arondata subscritiei este cea defaul-> if error with subs/db not found -> DELETE_COMPLETE
    if event['RequestType'] == "Delete":
        try:
            cf_sub_id, cf_event, cf_db_id, cf_sub_description = CurrentOutputs()
        except:
            responseStatus = 'SUCCESS'
            responseBody.update({"Status":responseStatus})
            GetResponse(responseURL, responseBody)
        all_subs = GetSubscription()
        print (all_subs)
        databases = GetAllDatabases(cf_sub_id)
        if str(cf_sub_id) in str(all_subs):
            if len(databases["subscription"][0]["databases"]) == 1:
                if str(databases["subscription"][0]["databases"][0]["databaseId"]) == str(cf_db_id):
                    try:
                        responseValue = DeleteSubscription(cf_sub_id, cf_db_id)
                        responseData.update({"SubscriptionId":str(cf_sub_id), "DefaultDatabaseId":str(cf_db_id), "SubscriptionDescription":str(cf_sub_description), "PostCall":str(cf_event)})
                        print (responseData)
                        responseBody.update({"Data":responseData})
                        GetResponse(responseURL, responseBody)
                    except:
                        responseStatus = 'FAILED'
                        reason = "Unable to delete subscription"
                        if responseStatus == 'FAILED':
                            responseBody.update({"Status":responseStatus})
                            if "Reason" in str(responseBody):
                                responseBody.update({"Reason":reason})
                            else:
                                responseBody["Reason"] = reason
                            GetResponse(responseURL, responseBody)
                else:
                    responseStatus = 'FAILED'
                    reason = "The only database assigned to subscription " + str(cf_sub_id) + " is not the default one."
                    if responseStatus == 'FAILED':
                        responseBody.update({"Status":responseStatus})
                        if "Reason" in str(responseBody):
                            responseBody.update({"Reason":reason})
                        else:
                            responseBody["Reason"] = reason
                        GetResponse(responseURL, responseBody)
            elif len(databases["subscription"][0]["databases"]) > 1:
                responseStatus = 'FAILED'
                reason = "Subscription " + str(cf_sub_id) + " has more than one database assigned. Please delete the other databases."
                if responseStatus == 'FAILED':
                    responseBody.update({"Status":responseStatus})
                    if "Reason" in str(responseBody):
                        responseBody.update({"Reason":reason})
                    else:
                        responseBody["Reason"] = reason
                    GetResponse(responseURL, responseBody)
            else:
                responseStatus = 'FAILED'
                reason = "Subscription " + str(cf_sub_id) + " has no databases assigned."
                if responseStatus == 'FAILED':
                    responseBody.update({"Status":responseStatus})
                    if "Reason" in str(responseBody):
                        responseBody.update({"Reason":reason})
                    else:
                        responseBody["Reason"] = reason
                    GetResponse(responseURL, responseBody)
        else:
            print("Subscription does not exists")
            GetResponse(responseURL, responseBody)
            
def RetrieveSecret(secret_name):
    headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get('AWS_SESSION_TOKEN')}

    secrets_extension_endpoint = "http://localhost:2773/secretsmanager/get?secretId=" + str(secret_name)
    r = requests.get(secrets_extension_endpoint, headers=headers)
    secret = json.loads(r.text)["SecretString"]
    secret = json.loads(secret)

    return secret

def CurrentOutputs():
    cloudformation = boto3.client('cloudformation')
    cf_response = cloudformation.describe_stacks(StackName=stack_name)
    for output in cf_response["Stacks"][0]["Outputs"]:
        if "SubscriptionId" in str(output): 
            cf_sub_id = output["OutputValue"]

        if "PostCall" in str(output): 
            cf_event = output["OutputValue"]

        if "DefaultDatabaseId" in str(output): 
            cf_db_id = output["OutputValue"]

        if "SubscriptionDescription" in str(output): 
            cf_sub_description = output["OutputValue"]
            
    print ("cf_sub_id is: " + str(cf_sub_id))
    print ("cf_event is: " + str(cf_event))
    print ("cf_db_id is: " + str(cf_db_id))
    print ("cf_sub_description is: " + str(cf_sub_description))
    return cf_sub_id, cf_event, cf_db_id, cf_sub_description
    
#Creating Flexible Subscription also requires creating a new Database within    
def PostSubscription (event):
    url = base_url + "/v1/subscriptions/" #will be changed based on new base_url
    
    response = requests.post(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key, "Content-Type":content_type}, json = event)
    response_json = response.json()
    return response_json
    Logs(response_json)

def GetSubscription (subscription_id = ""):
    # If subscription_id string is empty, GET verb will print all Flexible Subscriptions
    url = base_url + "/v1/subscriptions/" + subscription_id
    
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_json = response.json()
    return response_json
    Logs(response_json)

def GetSubscriptionStatus (subscription_id):
    url = base_url + "/v1/subscriptions/" + subscription_id
    
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response = response.json()
    sub_status = response["status"]
    print ("Subscription status is: " + sub_status)
    return sub_status
    
def GetSubscriptionId (url):
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response = response.json()
    print (str(response))
    count = 0
    
    while "resourceId" not in str(response) and count < 30:
        time.sleep(1)
        count += 1
        print (str(response))
        response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
        response = response.json()

    sub_id = response["response"]["resourceId"]
    sub_description = response["description"]
    return sub_id, sub_description

def GetSubscriptionError (url):
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response = response.json()
    count = 0

    while "processing-error" not in str(response) and count < 30:
        time.sleep(1)
        count += 1
        response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
        response = response.json()

    sub_error_description = response["response"]["error"]["description"]
    return sub_error_description
    
def GetDatabaseId (subscription_id, offset = 0, limit = 100):
    url = base_url + "/v1/subscriptions/" + str(subscription_id) + "/databases?offset=" + str(offset) + "&limit=" + str(limit)
    
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_json = response.json()
    default_db_id = response_json["subscription"][0]["databases"][0]["databaseId"]
    return default_db_id
    Logs(response_json)
    
def GetAllDatabases (subscription_id, offset = 0, limit = 100):
    url = base_url + "/v1/subscriptions/" + str(subscription_id) + "/databases?offset=" + str(offset) + "&limit=" + str(limit)
    
    response = requests.get(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_json = response.json()
    return response_json
    Logs(response_json)
    
def PutSubscription (subscription_id, event):
    url = base_url + "/v1/subscriptions/" + subscription_id
    print (event)
    
    update_dict = {}
    for key in list(event):
    	if key == "name":
    	    update_dict['name'] = event[key]
    	elif key == "paymentMethodId":
    	    update_dict['paymentMethodId'] = event[key]
    
    response = requests.put(url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key, "Content-Type":content_type}, json = update_dict)
    print ("PutSubscription response is:")
    print(response)
    response_json = response.json()
    return response_json
    Logs(response_json)

#Deleting Subscription requires deleting the Database underneath it first    
def DeleteSubscription (subscription_id, database_id):
    db_url   = base_url + "/v1/subscriptions/" + subscription_id + "/databases/" + database_id
    subs_url = base_url + "/v1/subscriptions/" + subscription_id
    
    response_db   = requests.delete(db_url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    response_subs = requests.delete(subs_url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    Logs(response_db.json())
    Logs(response_subs.json())
    
def GetResponse(responseURL, responseBody): 
    responseBody = json.dumps(responseBody)
    req = requests.put(responseURL, data = responseBody)
    print ('RESPONSE BODY:n' + responseBody)

def Logs(response_json):
    error_url = response_json['links'][0]['href']
    error_message = requests.get(error_url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
    error_message_json = error_message.json()
    if 'description' in error_message_json:
        while response_json['description'] == error_message_json['description']:
            error_message = requests.get(error_url, headers={"accept":accept, "x-api-key":x_api_key, "x-api-secret-key":x_api_secret_key})
            error_message_json = error_message.json()
        print(error_message_json)
    else:
        print ("No errors")
        
# def WriteToS3(event, file_name, bucket, object_name=None):
#     print ("Object name / filepath is: " + str(object_name))
#     f = open(file_name, "w")
#     f.write(str(event))
#     f.close()
#     if object_name is None:
#         object_name = os.path.basename(file_name)
#     s3 = boto3.client('s3')
#     try:
#         s3.upload_file(file_name, os.environ['BUCKET_NAME'], object_name)
#         print("Upload Successful")
#     except FileNotFoundError:
#         print("The file was not found")

# def ReadFromS3(bucket, key):    
#     s3 = boto3.resource('s3')
    
#     obj = s3.Object(bucket, key)
#     obj = obj.get()['Body'].read().decode('utf-8')
#     return obj
    
# def DeleteObject(object_name):
#     print (type(object_name))
#     print("Object name from S3 is: " + str(object_name))
#     s3 = boto3.client('s3')
#     try:
#         s3.delete_object(Bucket = os.environ['BUCKET_NAME'], Key = object_name)
#         print("Object " + object_name + " was successfully deleted.")
#     except FileNotFoundError:
#         print("The file was not found. Object " + object_name + " has failed to be deleted.")

# def GetNameFromS3(call):
#     s3_event = ReadFromS3(os.environ['BUCKET_NAME'], stack_name + "lambda-logs.txt")
#     s3_event = s3_event.replace("\'", "\"")
#     s3_event = s3_event.replace("False", "false")
#     s3_event = s3_event.replace("True", "true")
#     s3_event = json.loads(s3_event)
#     print ("Json form S3: ")
#     print (s3_event)
#     if call == "Subscription":
#         s3_sub_name = (s3_event['name'])
#         return s3_sub_name
#     elif call == "Database":
#         s3_db_name = (s3_event['databases'][0]['name'])
#         return s3_db_name
#     else:
#         print ("Interogation for names is incorrect. Please choose between 'Subscription' and 'Database'.")
