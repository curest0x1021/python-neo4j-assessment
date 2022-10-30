#!/usr/bin/env python
from ast import List
from contextlib import asynccontextmanager
import logging
import os

from fastapi import FastAPI
from neo4j import (basic_auth, AsyncGraphDatabase)

app = FastAPI()

url = os.getenv("NEO4J_URI", "neo4j://db.test.com")
username = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "neo4j")
database = os.getenv("NEO4J_DATABASE", "test")
port = os.getenv("PORT", 8080)

driver = AsyncGraphDatabase.driver(url, auth=basic_auth(username, password))

TYPE_PRODUCT = "products"
TYPE_LIFE = "life_science_firms"

@asynccontextmanager
async def get_db():
    async with driver.session(database=database) as session_:
        yield session_

@app.get("/providers/{id}")
async def get_providers(id: str, type: List = [], skip: int = 0, limit: int = 50):
    async def work(tx):
        subquery = ""
        if TYPE_PRODUCT in type and TYPE_LIFE in type:
            subquery = "--(p:Products) MATCH(l:LifeScienceFirm) RETURN n.display_name, p.product_name, l.life_science_firm_name "
        elif TYPE_PRODUCT in type:
            subquery = "--(p:Products) RETURN n.display_name, p.product_name, '' as life_science_firm_name "
        elif TYPE_LIFE in type:
            subquery = "MATCH(l:LifeScienceFirm) RETURN n.display_name, '' as product_name, l.life_science_firm_name "
        else:
            subquery = "RETURN n.display_name, '' as product_name, '' as life_science_firm_name "

        result = await tx.run(
            "MATCH (n:ProviderIndividual {providerIndividualID:$id}) "
            "$subquery "
            "SKIP $skip "
            "LIMIT $limit",
            {"id": id, "subquery": subquery, "skip": skip, "limit": limit}
        )
        return [record_ async for record_ in result]

    async with get_db() as db:
        result = await db.execute_read(work)
        return result

if __name__ == "__main__":
    import uvicorn

    logging.root.setLevel(logging.INFO)
    logging.info("Starting on port %d, database is at %s", port, url)

    uvicorn.run(app, port=port)
