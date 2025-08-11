"""
Embedding utilities — handles text chunking and embedding generation via MosaicML / Databricks model serving.
Stores vectors directly into MosaicDB.
"""

import re
import logging
import requests
from typing import List
from config import (
    MOSAICDB_URI,
    MOSAIC_API_KEY,
    MOSAIC_MODEL_ENDPOINT,
    CHUNK_SIZE,
    CHUNK_OVERLAP
)

def normalize_whitespace(text: str) -> str:
    """Collapse multiple spaces/newlines into single spaces."""
    return re.sub(r'\s+', ' ', text).strip()

def chunk_text(text: str, max_tokens: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """
    Split text into chunks of `max_tokens` words with `overlap` words carried over.
    Sentence-aware splitting when possible.
    """
    text = normalize_whitespace(text)
    sentences = re.split(r'(?<=[.?!])\s+', text)

    chunks = []
    current_chunk = []
    token_count = 0

    for sentence in sentences:
        sentence_tokens = sentence.split()
        if token_count + len(sentence_tokens) > max_tokens:
            if current_chunk:
                chunks.append(" ".join(current_chunk))
            if overlap > 0 and chunks:
                overlap_tokens = chunks[-1].split()[-overlap:]
                current_chunk = overlap_tokens + sentence_tokens
            else:
                current_chunk = sentence_tokens
            token_count = len(current_chunk)
        else:
            current_chunk.extend(sentence_tokens)
            token_count += len(sentence_tokens)

    if current_chunk:
        chunks.append(" ".join(current_chunk))

    logging.info(f"[Chunking] Created {len(chunks)} chunks (size={max_tokens}, overlap={overlap}).")
    return chunks

def generate_embeddings(chunks: List[str]) -> List[List[float]]:
    """
    Generate embeddings from MosaicML / Databricks model serving endpoint.
    """
    headers = {
        "Authorization": f"Bearer {MOSAIC_API_KEY}",
        "Content-Type": "application/json"
    }

    embeddings = []
    for idx, chunk in enumerate(chunks, start=1):
        payload = {"text": chunk}
        resp = requests.post(MOSAIC_MODEL_ENDPOINT, headers=headers, json=payload)
        resp.raise_for_status()
        result = resp.json()

        if "embedding" not in result:
            raise ValueError(f"Invalid response from Mosaic endpoint: {result}")

        embeddings.append(result["embedding"])
        logging.debug(f"[Embedding] Generated vector {idx}/{len(chunks)}")

    logging.info(f"[Embedding] Generated {len(embeddings)} embeddings.")
    return embeddings

def store_embeddings(document_id: str, chunks: List[str], vectors: List[List[float]], metadata: dict = None):
    """
    Store embeddings into MosaicDB with optional metadata.
    """
    headers = {
        "Authorization": f"Bearer {MOSAIC_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "document_id": document_id,
        "embeddings": [
            {"chunk": chunk, "vector": vector, "metadata": metadata or {}}
            for chunk, vector in zip(chunks, vectors)
        ]
    }
    resp = requests.post(f"{MOSAICDB_URI}/insert", headers=headers, json=payload)
    resp.raise_for_status()
    logging.info(f"[MosaicDB] Stored {len(vectors)} embeddings for document {document_id}.")
