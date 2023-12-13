
import json
import requests
from dotenv import load_dotenv
import os

load_dotenv()
paf_token = os.getenv("PAF_TOKEN")

def construct_response(message_id, interface, status, timestamp, action, hubspot_message):
    if isinstance(hubspot_message, str):
        # If hubspot_message is a string, load it as JSON
        hubspot_message = json.loads(hubspot_message)
    response_dict = {
        "message_return": {
            "id": message_id,
            "interface": interface,
            "status": status,
            "timestamp": timestamp,
            "action": action,
            "hubspot_message": hubspot_message
        }
    }
    response_json = json.dumps(response_dict, indent=2)
    return response_json

def get_contact_if_exists(property_name, value):
    # print(f"Line: 22 get_contact_if_exists is invoked for {value}")
    url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
    headers = {
        "Authorization": f"Bearer {paf_token}",
    }

    data = {
        "filterGroups": [
            {
                "filters": [
                    {
                        "propertyName": property_name,
                        "operator": "EQ",
                        "value": value
                    }
                ]
            }
        ]
    }

    response = requests.post(url, headers=headers, json=data)
    if response.status_code == 200:
        api_response = response.json()
        total_results = api_response.get('total', 0)
        if total_results > 0:
            get_contact = api_response['results'][0]
            print(f"Line: 48 found contact of {value} : {get_contact}")
            return get_contact
    else:
        print(f"Line: 51 None contact for {property_name} : {value}")
        return None

def check_if_contact_exist(properties):
    dlpb_id = properties.get('dlpb id')
    email = properties.get('email')
    
    # Check if 'dlpb id' exists and is a valid contact
    if dlpb_id:
        dlpb_response = get_contact_if_exists('dlpb id', dlpb_id)
        if dlpb_response:
            print(f"Line: 62 Contact with 'dlpb id' {dlpb_id} exists. Response: {dlpb_response}")
        return dlpb_response
        
    # Check if 'email' exists and is a valid contact
    elif email:
        email_response = get_contact_if_exists('email', email)
        if email_response:
            print(f"Line: 69 Contact with 'email' {email} exists. Response: {email_response}")
        return email_response
            

def filter_received_properties_by_default_properties(received_properties):
    # updated_properties = {}
    updated_properties = {
        "email": None,
        "phone": None,
        "firstname": None,
        "lastname": None,
        "gender":None,
        "language": None,
        "city": None,
        # "date of birth": None,
        # "lead status": None,
    }

    for received_prop in received_properties:
        property_name = received_prop.get("property", "")
        property_value = received_prop.get("value", "")

        # Check if the property name is in the properties to be updated
        if property_name.lower() in updated_properties:
            # Update the existing properties dictionary
            updated_properties[property_name.lower()] = property_value
            updated_properties[property_name.lower()] = property_value

    return updated_properties
        

def update_properties(received_properties):
    updated_properties = {}

    for received_prop in received_properties:
        property_name = received_prop.get("property", "")
        property_value = received_prop.get("value", "")

        # Update the existing properties dictionary
        updated_properties[property_name.lower()] = property_value

    return updated_properties

def format_error_response(error_string):
    # print(f"\n error_string {error_string} \n")
    error_string = str(error_string).replace('\\\"',"\"")
    error_string = str(error_string).replace('\\\\"', '\\"')
    start_index = error_string.find("[")
    last_index = error_string.find("]")
    if start_index < 0 or last_index < 0 or start_index >= len(error_string) or last_index >= len(error_string) or start_index > last_index:
        # Handle invalid indexes
        return "Invalid indexes"
    # Extract text between the specified indexes
    extracted_detail_list = error_string[start_index + 1:last_index]
    # print(f"extracted_detail_list: {extracted_detail_list}")

    # Remove the extracted text from the input string
    modified_error_string = error_string[:start_index + 1] + error_string[last_index:]
    modified_error_string = modified_error_string.replace(": []","")
    modified_error_string = modified_error_string.replace("'''","")
    extracted_error_json_string = f"[{extracted_detail_list}]"
    extracted_error_json_string = extracted_error_json_string.replace()


    # print("fer Extracted Text:", extracted_error_json_string)
    # print("fer Modified String:", modified_error_string)

    error_list_json_data = json.loads(extracted_error_json_string)

    json_data = json.loads(modified_error_string)
    json_data["details"] = error_list_json_data
    json_data = json.dumps(json_data, indent =2)

    return json_data