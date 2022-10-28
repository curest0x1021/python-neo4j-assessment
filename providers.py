#!/usr/bin/env python
from contextlib import asynccontextmanager
from ctypes import Union
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

@asynccontextmanager
async def get_db():
    async with driver.session(database=database) as session_:
        yield session_

@app.get("/providers/{id}")
async def get_providers(id: str, type: Union[str, None] = None, skip: int = 0, limit: int = 50):
    async def work(tx):
        subquery = "RETURN n.display_name, "" as product_name "
        if type is not None:
            subquery = "--(p:Products) RETURN n.display_name, p.product_name "
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

