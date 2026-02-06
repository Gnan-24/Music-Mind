from fastapi import FastAPI
from pydantic import BaseModel
from model import search_songs

app = FastAPI()

class SearchRequest(BaseModel):
    query: str

@app.post("/search")
def search(req: SearchRequest):
    return {
        "query": req.query,
        "results": search_songs(req.query)
    }
