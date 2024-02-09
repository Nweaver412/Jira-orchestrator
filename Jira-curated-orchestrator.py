import os
import json
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud import contact_center_insights_v1
from google.auth import exceptions

load_dotenv()

def authenticate_gcp():
    try:
        key_file_path = "service_acct_path"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path

        storage_client = storage.Client()
        contact_center_insights_client = contact_center_insights_v1.ContactCenterInsightsClient()

        print("Success")
    except exceptions.DefaultCredentialsError as e:
        print(f"Error: {e}")

def read_data_from_gcs(bucket_name, folder_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    files = bucket.list_blobs(prefix=folder_name)
    data = []
    for file in files:
        json_data = file.download_as_text()
        entries = json.loads(json_data).get("entries", [])
        data.extend(entries)
    return data

def send_to_contact_center_insights(project_id, conversation_data):
    contact_center_insights_client = contact_center_insights_v1.ContactCenterInsightsClient()
    conversation = contact_center_insights_client.create_conversation(
        parent=f"projects/{project_id}/locations/global",
        conversation={
            "utterances": [
                {
                    "text": entry.get("text", ""),
                    "role": entry.get("role", "AGENT"),
                    "user_id": entry.get("user_id", ""),
                    "start_timestamp": {"seconds": int(entry.get("start_timestamp_usec", 0)) // 1000000},
                }
                for entry in conversation_data
            ]
        }
    )

    analysis = contact_center_insights_client.create_analysis(
        parent=conversation.name,
        analysis={
            "query": {
                "language_code": "en" 
            },
            "output_data_config": {
                "gcs_destination": {
                    "uri": "/upload1/jira_curated_en"  
                }
            }
        }
    )

def main():
    authenticate_gcp()

    gcs_bucket_name = "gcd_bucket"
    gcs_folder_name = "gcs_file_path"
    contact_center_insights_project_id = "ccai_id"

    data = read_data_from_gcs(gcs_bucket_name, gcs_folder_name)
    send_to_contact_center_insights(contact_center_insights_project_id, data)

if __name__ == "__main__":
    main()
