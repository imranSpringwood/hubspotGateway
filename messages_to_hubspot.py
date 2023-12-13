import datetime
import json
import pika
import requests
from utils import check_if_contact_exist, construct_response, filter_received_properties_by_default_properties, update_properties, format_error_response
from dotenv import load_dotenv
import os
import time
from retrying import retry
from pprint import pprint


load_dotenv()
paf_token = os.getenv("PAF_TOKEN")
rabbitmq_return_queue = os.getenv("RABBBITMQ_RETURN_QUEUE")
rabbitmq_return_exchange = os.getenv("RABBITMQ_RETURN_EXCHANGE")
rabbitmq_return_binding_key = os.getenv("RABBITMQ_RETURN_BINDING_KEY")

# global variables
max_timeout_seconds = 3

def retry_intervals(attempt, *args, **kwargs):
    # Define custom retry intervals in milliseconds: 3 seconds, 5 seconds, 7 seconds, 10 seconds
    intervals = [3000, 5000, 7000, 10000]
    return intervals[attempt - 1] if attempt <= len(intervals) else 60000  # 1 minute in milliseconds for testing in prod it will be 10 minutes

def send_data_to_hubspot(receivedMessage,channel) :
    receivedMessageJson = json.loads(receivedMessage)
    message_send = receivedMessageJson.get("message_send", {})
    message_id = message_send.get("id")
    message_interface = message_send.get("interface")
    message_action = message_send.get("action")
    message_timestamp = message_send.get("timestamp")
    received_properties = receivedMessageJson.get("message_send", {}).get("properties", [])

    properties = update_properties(received_properties)

    contact_response = check_if_contact_exist(properties=properties)
    print(f"contact response is succesful")
    print(f"type: {type(max_timeout_seconds)}  value {max_timeout_seconds}")
    if contact_response is not None: 
        # contact exist update the contact
        contact_id = contact_response['id']
        update_contact_response = update_contact_in_hubspot(contact_id,properties, message_id, message_interface, message_timestamp,channel)
        return update_contact_response
    else:
        # contact doesn't exist creata a new contact
        create_contact_response =  create_contact_in_hubspot(properties, message_id, message_interface, message_timestamp,channel)
        return create_contact_response
       
 

@retry(wait_func=retry_intervals, stop_max_attempt_number=4)  
def create_contact(properties):
    print(f"Line: 108 create_contact is invoked for {properties}")
    endpoint = 'https://api.hubapi.com/crm/v3/objects/contacts'
    headers = {
        'Authorization': f'Bearer {paf_token}'
    }
    payload = {
        'properties': properties
    }
    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=max_timeout_seconds)
        response_data = json.loads(response.text)
        message = response_data.get("message", "")
    except requests.exceptions.Timeout:
        raise Exception(f" Hubspot Request timed out for create_contact")
    
    if 401 <= response.status_code < 600 and "Invalid input" not in message:
        raise Exception(f" Error Code {response.status_code} for create_contact, Error Message: {response.text}")
    else:
        return response


@retry(wait_func=retry_intervals, stop_max_attempt_number=4)
def update_contact(contact_id, properties):
    print(f"Line: 76 update_contact is invoked for {contact_id} with {properties}")
    endpoint = f'https://api.hubapi.com/crm/v3/objects/contacts/{contact_id}'
    headers = {
        'Authorization': f'Bearer {paf_token}'
    } 
    payload = {
        'properties': properties
    }
    try:
        response = requests.patch(endpoint, json=payload, headers=headers, timeout=max_timeout_seconds)
        response_data = json.loads(response.text)
        message = response_data.get("error", "")
    except requests.exceptions.Timeout:
        raise Exception(f" Hubspot Request timed out for update_contact = {contact_id}")       

    if 401 <= response.status_code < 600 and "PROPERTY_DOESNT_EXIST" not in message:     
        raise Exception(f" Error Code {response.status_code} for update_contact, Error Message: {response.text}")
    else:
        return response
    
def create_contact_in_hubspot(properties, message_id, message_interface, message_timestamp,channel):
     # create_contact_response =  create_contact(properties=properties) # without retry mechanism
        print("Contact id not found")
        while True:
                try:
                    create_contact_response =  create_contact(properties=properties)
                    break  # Exit the loop if successful
                except Exception as e:
                    current_time = datetime.datetime.now().strftime("%H:%M:%S")
                    print(f" {current_time} Create Operation failed: {e}")
                    # this exception is raised after 4 trials
                    error_json = {"error": str(e)}
                    hubspot_message = json.dumps(error_json)
                    errorResponse =  construct_response(message_id=message_id, interface=message_interface, status="FAILURE", 
                    timestamp=message_timestamp, action="create",hubspot_message=hubspot_message)
                    publish_message(message=errorResponse, channel=channel)
                    time.sleep(60)  # Sleep for 1 minute before retrying

        # print("\n\n Create Contact Response: ",create_contact_response)
        if create_contact_response.status_code == 201:
            hubspot_message = create_contact_response.json()
            hubspotGatewayResponseJsonForCreate = construct_response(message_id=message_id, interface=message_interface, status="SUCCESS", timestamp=message_timestamp, action="create", hubspot_message=hubspot_message)            
            json_data = json.loads(hubspotGatewayResponseJsonForCreate)
            formatted_json_string = json.dumps(json_data, indent=2)
            print(f"<201> Success Create Gateway Response: ",formatted_json_string)
            print("\n\n")
            publish_message(message=hubspotGatewayResponseJsonForCreate, channel=channel)
            return True
        elif create_contact_response.status_code == 400:
            error_json_string = create_contact_response.text
            error_json_string = f'{error_json_string}'
            hubspot_message = format_error_response(error_string=error_json_string)
            hubspotGatewayResponseJsonForCreate = construct_response(message_id=message_id, interface=message_interface, status="SUCCESS", timestamp=message_timestamp, action="create", hubspot_message=hubspot_message)            
            json_data = json.loads(hubspotGatewayResponseJsonForCreate)
            formatted_json_string = json.dumps(json_data, indent=2)
            print(f"<400> Failure Create Gateway Response: ",formatted_json_string)
            print("\n\n")
            publish_message(message=hubspotGatewayResponseJsonForCreate, channel=channel)
            return True
        else:
            error_json_string = create_contact_response.text
            error_json_string = f'{error_json_string}'
            hubspot_message = format_error_response(error_string=error_json_string)
            hubspotGatewayResponseJsonForCreate = construct_response(message_id=message_id, interface=message_interface, status="FAILURE", timestamp=message_timestamp, action="create", hubspot_message=hubspot_message)
            json_data = json.loads(hubspotGatewayResponseJsonForCreate)
            formatted_json_string = json.dumps(json_data, indent=2)
            print(f"<{create_contact_response.status_code}> Failure Create Gateway Response: ",formatted_json_string)
            print("\n\n")
            publish_message(message=hubspotGatewayResponseJsonForCreate, channel=channel)
            return True


def update_contact_in_hubspot(contact_id,properties, message_id, message_interface, message_timestamp, channel):
    print(f"Line: 114  update_contact_in_hubspot with contact id found {contact_id}")
    # update_contact_response =  update_contact(contact_id=contact_id,properties=properties) #without retry mechanism
    while True:
            try:
                update_contact_response =  update_contact(contact_id=contact_id,properties=properties)
                break  # Exit the loop if successful
            except Exception as e:
                current_time = datetime.datetime.now().strftime("%H:%M:%S")
                print(f" {current_time} Update Operation failed: {e}")
                error_json = {"error": str(e)}
                hubspot_message = json.dumps(error_json)
                # this exception is raised after 4 trials
                errorResponse =  construct_response(message_id=message_id, interface= message_interface, status="FAILURE", timestamp=message_timestamp, action="update",hubspot_message=hubspot_message)
                publish_message(message=errorResponse, channel=channel)
                time.sleep(60)  # Sleep for 1 minute before retrying

    if update_contact_response.status_code == 200:
        hubspot_message = update_contact_response.json()
        # print("\n\n Update Contact Response: ",update_contact_response)
        hubspotGatewayResponseJsonForUpdate = construct_response(message_id=message_id, interface=message_interface, status="SUCCESS", timestamp=message_timestamp, action="update",hubspot_message=hubspot_message)
        json_data = json.loads(hubspotGatewayResponseJsonForUpdate)
        formatted_json_string = json.dumps(json_data, indent=2)
        print(f"<200> Success Update Gateway Response: ",formatted_json_string)
        print("\n\n")
        publish_message(message=hubspotGatewayResponseJsonForUpdate, channel=channel)
        return True
    elif update_contact_response.status_code == 400:
        error_json_string = update_contact_response.text
        error_json_string = f'{error_json_string}'
        hubspot_message = format_error_response(error_string=error_json_string)
        hubspotGatewayResponseJsonForUpdate = construct_response(message_id=message_id, interface=message_interface, status="SUCCESS", timestamp=message_timestamp, action="update",hubspot_message=hubspot_message)
        json_data = json.loads(hubspotGatewayResponseJsonForUpdate)
        formatted_json_string = json.dumps(json_data, indent=2)
        print(f"<400> Failure Update Gateway Response: ",formatted_json_string)
        print("\n\n")
        publish_message(message=hubspotGatewayResponseJsonForUpdate, channel=channel)
        return True
    else:
        error_json_string = update_contact_response.text
        error_json_string = f'{error_json_string}'
        hubspot_message = format_error_response(error_string=error_json_string)
        hubspotGatewayResponseJsonForUpdate = construct_response(message_id=message_id, interface=message_interface, status="FAILURE", timestamp=message_timestamp, action="update",hubspot_message=hubspot_message)
        json_data = json.loads(hubspotGatewayResponseJsonForUpdate)
        formatted_json_string = json.dumps(json_data, indent=2)
        print(f"<{update_contact_response.status_code}> FAILURE Update Gateway Response: ",formatted_json_string)
        print("\n\n")
        publish_message(message=hubspotGatewayResponseJsonForUpdate, channel=channel)
        return True

def publish_message(message, channel):
    # print(f"line: 162 publish_message is invoked for {message}")
    channel.queue_declare(queue=rabbitmq_return_queue, durable=True)
    channel.basic_publish(
    exchange=rabbitmq_return_exchange,
    routing_key=rabbitmq_return_binding_key,
    body=message,
    properties=pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
    ))
    print(f" [x] Sent {message}")
    print("publishing\n")
