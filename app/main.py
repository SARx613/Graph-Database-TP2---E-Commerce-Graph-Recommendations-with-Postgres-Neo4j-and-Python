from fastapi import FastAPI
import os
import psycopg2
from neo4j import GraphDatabase

app = FastAPI(title="Graph Recs API")

PG_URL = os.environ.get("PG_URL", "postgresql://app:app@postgres:5432/shop")
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

def pg_ok() -> bool:
    try:
        conn = psycopg2.connect(PG_URL)
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
            cur.fetchone()
        conn.close()
        return True
    except Exception:
        return False

def neo4j_ok() -> bool:
    try:
        with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as d:
            with d.session() as s:
                s.run("RETURN 1").consume()
        return True
    except Exception:
        return False

@app.get("/health")
def health():
    return {"ok": pg_ok() and neo4j_ok()}
