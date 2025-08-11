# functions/pdf_processor.py

"""
PDF Processor — Page-wise combined text with metadata.
Uses PyMuPDF for text extraction, falls back to OCR for image-only PDFs.
"""

import fitz  # PyMuPDF
import io
from utils.helpers import extract_images_from_pdf_page, ocr_image_pil, normalize_whitespace
from utils.blob_utils import upload_json
from utils.db_utils import get_document_metadata
# from azure.storage.blob import BlobServiceClient
# from config import AZURE_STORAGE_CONNECTION_STRING


def process_pdf(input_blob_path: str, output_blob_path: str, document_id: str):
    """
    Process PDF from Azure Blob directly in memory.
    """
    # Step 1: Read PDF bytes directly from Blob
    # blob_service = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
    # container_name, blob_name = input_blob_path.split("/", 1)
    # blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)
    # pdf_bytes = blob_client.download_blob().readall()

    pdf_bytes = download_blob_to_bytes(blob_path, container_name)
    pdf_stream = io.BytesIO(pdf_bytes)

    # Step 2: Get metadata from DB
    metadata = get_document_metadata(document_id)

    # Step 3: Process PDF
    pdf_doc = fitz.open(stream=pdf_stream, filetype="pdf")
    pages_output = []

    for page_index, page in enumerate(pdf_doc):
        # Extract text
        text = normalize_whitespace(page.get_text("text"))

        # If text empty → OCR from images
        if not text.strip():
            images = extract_images_from_pdf_page(page)
            ocr_text_parts = []
            for img in images:
                ocr_text_parts.append(ocr_image_pil(img))
            text = normalize_whitespace(" ".join(ocr_text_parts))

        pages_output.append({
            "page_number": page_index + 1,
            "combined_text": text
        })

    pdf_doc.close()

    # Step 4: Build output JSON
    output_data = {
        "document_id": document_id,
        "metadata": metadata,
        "pages": pages_output
    }

    # Step 5: Upload JSON to Blob
    upload_json(output_data, output_blob_path)

    print(f"[PDF Processor] Processed '{input_blob_path}' -> '{output_blob_path}'")


# Azure Function entry
def main(msg: dict):
    """
    Azure Function trigger entry point.
    Expected msg:
      {
        "input_blob": "container/blobname.pdf",
        "output_blob": "container/blobname.json",
        "document_id": "12345"
      }
    """
    process_pdf(
        input_blob_path=msg["input_blob"],
        output_blob_path=msg["output_blob"],
        document_id=msg["document_id"]
    )
