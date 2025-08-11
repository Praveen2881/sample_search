import io
import tempfile
from utils.blob_utils import download_blob_to_bytes
from functions.pdf_processor import process_pdf
from docx import Document
from docx2pdf import convert

def process_docx(blob_path: str, container_name: str = "documents", output_container: str = "processed"):
    """
    Process a DOCX/DOC file:
    - Download from Azure Blob
    - Convert to PDF in memory (temp only for conversion step)
    - Call process_pdf() to handle extraction
    """
    # Download DOCX/DOC into memory
    docx_bytes = download_blob_to_bytes(blob_path, container_name)

    with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as temp_docx:
        temp_docx.write(docx_bytes)
        temp_docx_path = temp_docx.name

    # Convert DOCX to PDF (docx2pdf only works with paths)
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as temp_pdf:
        temp_pdf_path = temp_pdf.name

    convert(temp_docx_path, temp_pdf_path)

    # Read PDF back into memory and send to PDF processor
    with open(temp_pdf_path, "rb") as f:
        pdf_bytes = f.read()

    # Instead of saving to blob again, just call process_pdf with in-memory PDF
    pdf_blob_path = blob_path.rsplit(".", 1)[0] + ".pdf"
    from utils.blob_utils import upload_file
    upload_file(io.BytesIO(pdf_bytes), pdf_blob_path, container_name=container_name)

    return process_pdf(pdf_blob_path, container_name=container_name, output_container=output_container)
