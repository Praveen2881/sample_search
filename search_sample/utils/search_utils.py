"""
Search utilities — vector & hybrid search on MosaicDB with metadata filtering.
"""

import requests
from config import MOSAICDB_URI, MOSAIC_API_KEY, MOSAIC_MODEL_ENDPOINT
from typing import List, Dict

def embed_query(query: str) -> List[float]:
    """Generate embedding for search query using MosaicML model."""
    headers = {
        "Authorization": f"Bearer {MOSAIC_API_KEY}",
        "Content-Type": "application/json"
    }
    resp = requests.post(MOSAIC_MODEL_ENDPOINT, headers=headers, json={"text": query})
    resp.raise_for_status()
    result = resp.json()

    if "embedding" not in result:
        raise ValueError(f"Invalid response from Mosaic endpoint: {result}")

    return result["embedding"]

def vector_search(query: str, metadata_filter: Dict) -> List[Dict]:
    """
    Perform vector search in MosaicDB with metadata filtering.
    metadata_filter example:
    {
        "client_id": "client_456"
    }
    """
    query_embedding = embed_query(query)

    headers = {
        "Authorization": f"Bearer {MOSAIC_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "vector": query_embedding,
        "top_k": 10,  # top 10 results
        "filter": metadata_filter
    }

    resp = requests.post(f"{MOSAICDB_URI}/vector_search", headers=headers, json=payload)
    resp.raise_for_status()
    results = resp.json()

    return results.get("matches", [])

def hybrid_search(query: str, metadata_filter: Dict) -> List[Dict]:
    """
    Perform hybrid search in MosaicDB (vector + keyword) with metadata filtering.
    """
    query_embedding = embed_query(query)

    headers = {
        "Authorization": f"Bearer {MOSAIC_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "vector": query_embedding,
        "text": query,
        "top_k": 10,
        "filter": metadata_filter
    }

    resp = requests.post(f"{MOSAICDB_URI}/hybrid_search", headers=headers, json=payload)
    resp.raise_for_status()
    results = resp.json()

    return results.get("matches", [])
