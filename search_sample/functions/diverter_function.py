import logging

def route_document(blob_url: str, metadata: dict) -> dict:
    """
    Decides which processor to use based on file extension.
    Returns processed content as JSON.
    """
    extension = blob_url.split('.')[-1].lower()
    logging.info(f"Routing document with extension: {extension}")

    if extension == "pdf":
        return process_pdf(blob_url, metadata)
    elif extension in ["docx", "doc"]:
        return process_docx(blob_url, metadata)
    elif extension == "txt":
        return process_txt(blob_url, metadata)
    else:
        raise ValueError(f"Unsupported file type: {extension}")

# def process_pdf(blob_url, metadata):
#     # Stub: Download blob & parse PDF
#     logging.info(f"Processing PDF from {blob_url}")
#     return {"text": "Extracted text from PDF"}

# def process_docx(blob_url, metadata):
#     logging.info(f"Processing DOCX from {blob_url}")
#     return {"text": "Extracted text from DOCX"}

# def process_txt(blob_url, metadata):
#     logging.info(f"Processing TXT from {blob_url}")
#     return {"text": "Extracted text from TXT"}
