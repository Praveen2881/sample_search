# job_manager/__init__.py
import logging
import json
import azure.functions as func
from utils.logging_utils import log_info, log_error
from db.session import SessionLocal
from db.crud import update_job_status, create_job
from utils.blob_utils import download_file, upload_json
from functions.router_function import route_to_processor
from functions.chunk_embed import chunk_and_embed
from utils.servicebus_utils import send_servicebus_message

# Configurable stages
STAGES = ["diverter", "processor", "chunking"]

def main(msg: func.ServiceBusMessage):
    session = SessionLocal()
    try:
        body = json.loads(msg.get_body().decode("utf-8"))
        stage = body.get("stage")
        job_id = body["job_id"]

        log_info(f"Job {job_id} received at stage {stage}")

        if stage == "diverter":
            update_job_status(session, job_id, "diverting")
            extension = body["extension"]
            next_stage = "processor"
            send_servicebus_message({**body, "stage": next_stage})

        elif stage == "processor":
            update_job_status(session, job_id, "processing")
            local_path = download_file(body["blob_path"])
            processed_data = route_to_processor(local_path, body["extension"])
            output_blob = f"processed/{body['doc_name']}.json"
            upload_json(processed_data, output_blob)
            next_stage = "chunking"
            send_servicebus_message({**body, "stage": next_stage, "processed_blob": output_blob})

        elif stage == "chunking":
            update_job_status(session, job_id, "chunking")
            local_path = download_file(body["processed_blob"])
            with open(local_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            chunk_and_embed(data, body["doc_id"], body["metadata"])
            update_job_status(session, job_id, "completed")

        else:
            log_error(f"Unknown stage {stage}")

    except Exception as e:
        log_error(f"Error in job_manager: {e}")
        update_job_status(session, body.get("job_id"), f"failed: {e}")
    finally:
        session.close()
