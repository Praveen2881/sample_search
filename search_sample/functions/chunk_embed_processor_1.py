# functions/processors/chunk_embed_processor.py
"""
Chunk & Embed Processor
Retrieves processed JSON from blob, chunks text, generates embeddings, stores in Mosaic DB.
"""

from db.session import SessionLocal
from db import crud
from utils import blob_utils, embedding_utils
import json

def process_chunk_embed(document_id: int, blob_path: str):
    db = SessionLocal()
    try:
        crud.upsert_job_status(db, document_id, stage="chunk_embed", status="processing")

        # 1. Download JSON
        local_path = blob_utils.download_file(blob_path)
        with open(local_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 2. Chunk text
        text_parts = []
        if "pages" in data:  # from PDF
            for page in data["pages"]:
                text_parts.extend(embedding_utils.chunk_text(page))
        elif "content" in data:  # from Office
            for section in data["content"]:
                if isinstance(section, list):
                    for item in section:
                        if isinstance(item, str):
                            text_parts.extend(embedding_utils.chunk_text(item))
                        elif isinstance(item, list):
                            text_parts.extend(embedding_utils.chunk_text(" ".join(map(str, item or []))))
        else:
            raise ValueError("Unknown processed content format")

        # 3. Generate embeddings
        vectors = embedding_utils.generate_embeddings(text_parts)

        # 4. Store in Mosaic DB (stub — implement real Mosaic ingestion)
        # mosaic_utils.store_embeddings(document_id, vectors)
        print(f"[Stub] Stored {len(vectors)} embeddings for document {document_id}")

        crud.upsert_job_status(db, document_id, stage="chunk_embed", status="completed")
    except Exception as e:
        crud.upsert_job_status(db, document_id, stage="chunk_embed", status="failed", error=str(e))
    finally:
        db.close()
