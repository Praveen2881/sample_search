# search.py
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, List
from utils.search_utils import vector_search, hybrid_search

app = FastAPI(title="MosaicDB Search API", version="1.0")

class SearchRequest(BaseModel):
    query: str
    metadata_filter: Optional[Dict] = None
    search_type: str = Query("vector", description="vector or hybrid")

class SearchResult(BaseModel):
    id: str
    score: float
    metadata: Dict
    chunk: str

@app.post("/search", response_model=List[SearchResult])
def search_documents(request: SearchRequest):
    """
    Search MosaicDB embeddings with optional metadata filter.
    - search_type = "vector": semantic vector search
    - search_type = "hybrid": combined vector + keyword search
    """
    try:
        if request.search_type == "vector":
            matches = vector_search(request.query, request.metadata_filter or {})
        elif request.search_type == "hybrid":
            matches = hybrid_search(request.query, request.metadata_filter or {})
        else:
            raise HTTPException(status_code=400, detail="Invalid search_type, must be 'vector' or 'hybrid'")

        results = [
            SearchResult(
                id=m.get("id"),
                score=m.get("score"),
                metadata=m.get("metadata", {}),
                chunk=m.get("chunk", "")
            )
            for m in matches
        ]
        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
