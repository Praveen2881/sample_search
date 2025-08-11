# functions/processors/helpers.py
"""
Shared helpers for processors:
- lightweight table detection & normalization
- image OCR (pytesseract)
- extract images from PDF pages (PyMuPDF)
- small convenience utilities
"""

import io
import os
import tempfile
from typing import List, Tuple, Dict, Optional

from PIL import Image
import pytesseract

# NOTE: Ensure pytesseract binary is available in PATH in your execution environment.
# If not, set pytesseract.pytesseract.tesseract_cmd = r'/usr/bin/tesseract' or appropriate.

def normalize_whitespace(s: str) -> str:
    return " ".join(s.split())

def detect_tables_in_text(page_text: str) -> List[str]:
    """
    Heuristic table detection:
    - Looks for lines that contain multiple consecutive spaces (or tabs) acting as column separators,
      or lines with consistent number of 'columns' across multiple consecutive lines.
    - Converts detected rows into pipe-separated values.
    Limitations:
    - This is a heuristic; for robust extraction use Camelot/Tabula (requires extra dependencies).
    """
    rows = [line for line in (page_text.splitlines()) if line.strip()]
    if not rows:
        return []

    # Compute split heuristics: if many rows have 2+ runs of 2+ spaces, treat as table
    candidate_rows = []
    for r in rows:
        # if contains tab -> strong signal
        if "\t" in r:
            candidate_rows.append(r)
            continue
        # multiple sequences of two or more spaces (indicative of columns)
        if "  " in r:
            candidate_rows.append(r)

    # require at least 2 candidate rows to confirm a table
    if len(candidate_rows) < 2:
        return []

    # Convert candidate rows to pipe-separated rows
    table_rows = []
    for r in candidate_rows:
        # replace tabs and 2+ spaces with |
        row = r.replace("\t", "|")
        # collapse runs of 2+ spaces into '|'
        import re
        row = re.sub(r"\s{2,}", "|", row).strip()
        # also trim leading/trailing separators
        row = row.strip("| ")
        table_rows.append(row)

    return table_rows

def ocr_image_pil(pil_image: Image.Image, lang: str = "eng") -> str:
    """
    Run pytesseract OCR on a PIL image and return extracted text.
    """
    try:
        text = pytesseract.image_to_string(pil_image, lang=lang)
        return text or ""
    except Exception as e:
        # Keep failures non-fatal — return empty string
        return ""

def extract_images_from_pdf_page(pdf_page) -> List[Image.Image]:
    """
    Use PyMuPDF (fitz) page to extract images as PIL Images.
    Returns list of PIL Image objects.
    """
    images = []
    try:
        # PyMuPDF provides list of images with xref etc.
        image_list = pdf_page.get_images(full=True)
        for img_idx, img in enumerate(image_list):
            xref = img[0]
            base_image = pdf_page.parent.extract_image(xref)
            image_bytes = base_image["image"]
            img_ext = base_image.get("ext", "png")
            try:
                im = Image.open(io.BytesIO(image_bytes)).convert("RGB")
                images.append(im)
            except Exception:
                continue
    except Exception:
        # If any failure, return empty list
        pass
    return images
