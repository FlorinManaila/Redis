import json
import json
import cfnresponse
import requests

def lambda_handler(event, context):
    print (event)
    responseURL = event["responseURL"]
    responseBody = event["responseBody"]
    GetResponse(responseURL, responseBody)


def GetResponse(responseURL, responseBody): 
    responseBody = json.dumps(responseBody)
    req = requests.put(responseURL, data = responseBody)
    print ('RESPONSE BODY:n' + responseBody)
