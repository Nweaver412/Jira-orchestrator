import os
import json
import toml
from google.cloud import storage
from google.cloud import contact_center_insights_v1
from google.auth import exceptions

def load_secrets():
    try:
        with open("secrets.toml", "r") as secrets_file:
            secrets = toml.load(secrets_file)
        return secrets
    except FileNotFoundError:
        print("Error: secrets.toml file not found.")
        return None

def authenticate_gcp():
    try:
        key_file_path = "/Desktop/Jira-Curated-Project/keboola-ai-4afb21575d2e.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_file_path

        storage_client = storage.Client()
        contact_center_insights_client = contact_center_insights_v1.ContactCenterInsightsClient()

        print("Authentication successful.")
    except exceptions.DefaultCredentialsError as e:
        print(f"Error authenticating to GCP: {e}")

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

    # Analyze the conversation
    analysis = contact_center_insights_client.create_analysis(
        parent=conversation.name,
        analysis={
            # Specify analysis details as needed
        }
    )

    # Handle responses and results
    # (code to process the results and handle errors)

def main():
    authenticate_gcp()

    gcs_bucket_name = "jira_curated"
    gcs_folder_name = "/upload1/1074750538_jira_curation.zip"
    contact_center_insights_project_id = "4717244637851503026"

    data = read_data_from_gcs(gcs_bucket_name, gcs_folder_name)
    send_to_contact_center_insights(contact_center_insights_project_id, data)

if __name__ == "__main__":
    main()
