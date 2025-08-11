# job_manager/__init__.py
import logging
import json
import azure.functions as func
from utils.blob_utils import get_blob_metadata
from utils.servicebus_utils import send_servicebus_message

def main(event: func.EventGridEvent):
    try:
        event_data = event.get_json()
        blob_url = event_data["url"]

        # Example blob_url:
        # https://<account>.blob.core.windows.net/<container>/<path/to/file.pdf>
        blob_path = "/".join(blob_url.split("/", 4)[4:])  # after container name
        extension = blob_path.split(".")[-1].lower()

        # Pull metadata directly from blob
        metadata = get_blob_metadata(blob_path)

        logging.info(f"[JobManager] New blob uploaded: {blob_path} ({extension})")

        # Prepare job payload for Service Bus
        job_payload = {
            "blob_path": blob_path,
            "extension": extension,
            "metadata": metadata,
            "stage": "processor"
        }

        send_servicebus_message(job_payload)
        logging.info(f"[JobManager] Sent job to Service Bus: {job_payload}")

    except Exception as e:
        logging.error(f"[JobManager] Error handling Event Grid event: {e}")
        raise
