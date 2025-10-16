import os
import time
from pathlib import Path
from typing import Iterable, List, Dict

import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from neo4j import GraphDatabase, Driver, Session

# -------------------
# Config from env
# -------------------
PG_URL = os.environ.get("PG_URL", "postgresql://app:app@postgres:5432/shop")
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "password")

WAIT_TIMEOUT_SEC = 120
BATCH_SIZE = 1000


# -------------------
# Readiness helpers
# -------------------
def wait_for_postgres(timeout: int = WAIT_TIMEOUT_SEC) -> None:
    start = time.time()
    while True:
        try:
            conn = psycopg2.connect(PG_URL)
            conn.close()
            return
        except Exception:
            if time.time() - start > timeout:
                raise
            time.sleep(1)


def wait_for_neo4j(timeout: int = WAIT_TIMEOUT_SEC) -> None:
    start = time.time()
    while True:
        try:
            with GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)) as d:
                with d.session() as s:
                    s.run("RETURN 1").consume()
            return
        except Exception:
            if time.time() - start > timeout:
                raise
            time.sleep(1)


# -------------------
# Utility helpers
# -------------------
def run_cypher(session: Session, query: str, params: Dict | None = None):
    return session.run(query, params or {}).consume()


def run_cypher_file(session: Session, cypher_path: Path) -> None:
    text = cypher_path.read_text(encoding="utf-8")
    # Split on semicolons and skip blanks/comments-only parts
    statements = [q.strip() for q in text.split(";") if q.strip()]
    for stmt in statements:
        session.run(stmt).consume()


def chunk(iterable: Iterable, size: int) -> Iterable[List]:
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


# -------------------
# Extract (via SQLAlchemy)
# -------------------
def extract_postgres() -> Dict[str, pd.DataFrame]:
    engine = create_engine(PG_URL)
    tables: Dict[str, pd.DataFrame] = {}
    # Quote table names to avoid reserved word issues
    for tbl in ["customers", "categories", "products", "orders", "order_items", "events"]:
        tables[tbl] = pd.read_sql_query(f'SELECT * FROM "{tbl}";', engine)
    engine.dispose()
    return tables


# -------------------
# Normalize date/time to ISO strings (robust)
# -------------------
def _to_date_str(series: pd.Series) -> pd.Series:
    """
    Convert a Series to 'YYYY-MM-DD' strings.
    Unparsable values -> None.
    """
    s = pd.to_datetime(series.astype(str).str.strip(), errors="coerce") \
          .dt.strftime("%Y-%m-%d")
    return s.where(~s.isna(), None)


def _to_datetime_str(series: pd.Series) -> pd.Series:
    """
    Convert a Series to ISO 'YYYY-MM-DDTHH:MM:SSZ' (UTC).
    Unparsable values -> None.
    """
    s = pd.to_datetime(series.astype(str).str.strip(), utc=True, errors="coerce") \
          .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return s.where(~s.isna(), None)


def normalize_tables(tables: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    t = {k: v.copy() for k, v in tables.items()}

    if "customers" in t and "join_date" in t["customers"].columns:
        t["customers"]["join_date"] = _to_date_str(t["customers"]["join_date"])

    if "orders" in t and "ts" in t["orders"].columns:
        t["orders"]["ts"] = _to_datetime_str(t["orders"]["ts"])

    if "events" in t and "ts" in t["events"].columns:
        t["events"]["ts"] = _to_datetime_str(t["events"]["ts"])

    return t


# -------------------
# Load into Neo4j
# -------------------
def load_graph(tables: Dict[str, pd.DataFrame]) -> None:
    driver: Driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver, driver.session() as session:
        # 1) Constraints / indexes
        queries_path = Path(__file__).with_name("queries.cypher")
        run_cypher_file(session, queries_path)

        # 2) Categories
        cats = tables["categories"].to_dict("records")
        for batch in chunk(cats, BATCH_SIZE):
            session.run("""
                UNWIND $rows AS row
                MERGE (g:Category {id: row.id})
                SET g.name = row.name
            """, {"rows": batch}).consume()

        # 3) Products + IN_CATEGORY
        prods = tables["products"].to_dict("records")
        for batch in chunk(prods, BATCH_SIZE):
            session.run("""
                UNWIND $rows AS row
                MERGE (p:Product {id: row.id})
                SET p.name = row.name, p.price = toFloat(row.price)
                WITH row, p
                MATCH (g:Category {id: row.category_id})
                MERGE (p)-[:IN_CATEGORY]->(g)
            """, {"rows": batch}).consume()

        # 4) Customers (guard join_date)
        custs = tables["customers"].to_dict("records")
        for batch in chunk(custs, BATCH_SIZE):
            session.run("""
                UNWIND $rows AS row
                MERGE (c:Customer {id: row.id})
                SET c.name = row.name,
                    c.join_date = CASE
                        WHEN row.join_date IS NULL OR row.join_date = "" THEN NULL
                        ELSE date(row.join_date)
                    END
            """, {"rows": batch}).consume()

        # 5) Orders + PLACED (guard ts)
        orders = tables["orders"].to_dict("records")
        for batch in chunk(orders, BATCH_SIZE):
            session.run("""
                UNWIND $rows AS row
                MERGE (o:Order {id: row.id})
                SET o.ts = CASE
                    WHEN row.ts IS NULL OR row.ts = "" THEN NULL
                    ELSE datetime(row.ts)
                END
                WITH row, o
                MATCH (c:Customer {id: row.customer_id})
                MERGE (c)-[:PLACED]->(o)
            """, {"rows": batch}).consume()

        # 6) CONTAINS (Order -> Product)
        items = tables["order_items"].to_dict("records")
        for batch in chunk(items, BATCH_SIZE):
            session.run("""
                UNWIND $rows AS row
                MATCH (o:Order {id: row.order_id})
                MATCH (p:Product {id: row.product_id})
                MERGE (o)-[r:CONTAINS]->(p)
                SET r.quantity = toInteger(row.quantity)
            """, {"rows": batch}).consume()

        # 7) Behavioral events (Customer -> Product) with guarded ts
        events = tables["events"].to_dict("records")

        def by_type(t): return [e for e in events if e.get("event_type") == t]

        for rel_type, subset in [
            ("VIEWED", by_type("view")),
            ("CLICKED", by_type("click")),
            ("ADDED_TO_CART", by_type("add_to_cart")),
        ]:
            for batch in chunk(subset, BATCH_SIZE):
                session.run(f"""
                    UNWIND $rows AS row
                    MATCH (c:Customer {{id: row.customer_id}})
                    MATCH (p:Product  {{id: row.product_id}})
                    MERGE (c)-[r:{rel_type}]->(p)
                    SET r.ts = CASE
                        WHEN row.ts IS NULL OR row.ts = "" THEN NULL
                        ELSE datetime(row.ts)
                    END
                """, {"rows": batch}).consume()


# -------------------
# Orchestrate ETL
# -------------------
def etl() -> None:
    """
    1) Wait for services
    2) Extract from Postgres
    3) Normalize dates
    4) Load into Neo4j
    """
    wait_for_postgres()
    wait_for_neo4j()

    tables = extract_postgres()
    tables = normalize_tables(tables)
    load_graph(tables)

    print("ETL done.")


if __name__ == "__main__":
    etl()
