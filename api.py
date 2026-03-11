"""
PG AI Query Engine — Powered by pgai and pgvector.
Uses pgai semantic search to discover schema and generate SQL.
"""

import os
import re
import json
import asyncio
import sys
from typing import Any, Optional, Dict, List

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import psycopg
from psycopg.rows import dict_row
import requests

# Official pgai and pydantic-ai imports
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.openai import OpenAIProvider
from pgai.semantic_catalog import loader, render

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
NEON_DB_URL = "postgresql://neondb_owner:npg_5RdSgjpHCQ6P@ep-sweet-thunder-a1rqx5ar-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

class Config:
    BANNER = "PG AI Query Engine v3.2.0"
    MODEL = "openai" # Map to Pollinations supported model
    BASE_URL = "https://text.pollinations.ai/v1"

# ═══════════════════════════════════════════════════════════════
# UTILS: DNS & CONNECTION
# ═══════════════════════════════════════════════════════════════
_RESOLVED_CACHE = {}

async def get_resolved_url() -> str:
    """Resolve Neon host using Google DoH to bypass restrictive DNS filters."""
    if NEON_DB_URL in _RESOLVED_CACHE:
        return _RESOLVED_CACHE[NEON_DB_URL]
    
    print(f"[PG AI] Resolving database host...")
    try:
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(NEON_DB_URL)
        host = parsed.hostname
        if host and ".neon.tech" in host:
            doh_url = f"https://dns.google/resolve?name={host}"
            # Run blocking request in a thread
            resp = await asyncio.to_thread(requests.get, doh_url, timeout=5)
            data = resp.json()
            # Extract the first A record
            ip = next((a["data"] for a in data.get("Answer", []) if a["type"] == 1), None)
            if ip:
                endpoint_id = host.split(".")[0]
                query = parsed.query
                opt = f"options=endpoint%3D{endpoint_id}"
                query = f"{query}&{opt}" if query else opt
                netloc = parsed.netloc.replace(host, ip)
                final_url = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, query, parsed.fragment))
                _RESOLVED_CACHE[NEON_DB_URL] = final_url
                print(f"[PG AI] Resolved host to IP: {ip}")
                return final_url
            else:
                print(f"[PG AI] DOH failed to find IP for host: {host}")
    except Exception as e:
        print(f"[PG AI] DNS Resolution Warning: {e}")
    
    return NEON_DB_URL

async def get_connection():
    """Returns an async psycopg connection to the database."""
    print("[PG AI] Connecting to database...")
    url = await get_resolved_url()
    try:
        conn = await psycopg.AsyncConnection.connect(url, autocommit=True, connect_timeout=10)
        print("[PG AI] Database connection successful.")
        return conn
    except Exception as e:
        print(f"[PG AI] Database connection failed: {e}")
        raise

# ═══════════════════════════════════════════════════════════════
# AI AGENT
# ═══════════════════════════════════════════════════════════════
from openai import AsyncOpenAI

llm = OpenAIChatModel(
    model_name=Config.MODEL,
    provider=OpenAIProvider(
        base_url=Config.BASE_URL,
        api_key='keyless'
    )
)

class SQLResponse(BaseModel):
    sql: str = Field(description="The valid PostgreSQL SELECT query.")
    explanation: str = Field(description="A brief explanation of how the query works.")
    confidence: float = Field(default=1.0, description="Confidence score 0-1.")
    suggested_visualization: str = Field(default="table", description="Visualization type.")

sql_agent = Agent(
    llm,
    output_type=SQLResponse,
    system_prompt=(
        "You are 'PG AI', a state-of-the-art PostgreSQL Data Assistant.\n"
        "Your goal: Generate perfect SQL SELECT queries from natural language.\n\n"
        "STRICT GUIDELINES:\n"
        "1. Security: Generate SELECT statements ONLY. No mutations (INSERT/UPDATE/DELETE/ALTER).\n"
        "2. Schema: Only use tables and columns defined in the provided schema context.\n"
        "3. Search: Use ILIKE for text searches to remain case-insensitive.\n"
        "4. pgvector: If you see 'vector' type columns, use '<->' for similarity search when relevant.\n"
        "5. pgai: Leverage pgai features for advanced data retrieval and processing.\n"
    )
)

# ═══════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════
app = FastAPI(title="PG AI Engine", description="Natural Language to SQL")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    prompt: str
    selected_tables: List[str] = Field(default_factory=list)
    role: str = "viewer"

@app.get("/get-database-tables")
async def api_get_schema():
    """Return schema map for the UI side-panel."""
    try:
        async with await get_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT table_name, column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name, ordinal_position
                """)
                rows = await cur.fetchall()
                schema = {}
                for r in rows:
                    schema.setdefault(r['table_name'], {})[r['column_name']] = r['data_type']
                return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-query")
async def api_generate_query(req: QueryRequest):
    """The core engine: NL -> Schema Context -> AI -> SQL -> Execution."""
    try:
        async with await get_connection() as conn:
            # 1. Discover relevant tables
            async with conn.cursor() as cur:
                if req.selected_tables:
                    await cur.execute("SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = ANY(%s) AND c.relkind = 'r'", (req.selected_tables,))
                else:
                    await cur.execute("SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r' LIMIT 15")
                oids = [row[0] for row in await cur.fetchall()]
            
            if not oids:
                return {"success": False, "error": "No tables detected in the public schema."}

            # 2. Use official pgai tools to prepare metadata for high-quality prompting
            tables = await loader.load_tables(conn, oids, sample_size=3)
            schema_context = render.render_tables(tables)

            # 3. Call AI
            prompt = f"User Request: {req.prompt}\n\nSchema Context:\n{schema_context}"
            result = await sql_agent.run(prompt)
            
            sql_spec = result.output
            sql_query = sql_spec.sql.strip()
            print(f"[PG AI] Generated SQL: {sql_query}", file=sys.stderr, flush=True)
            
            if not sql_query:
                return {"success": False, "error": "AI could not formulate a valid query."}

            # 4. Security Check
            blocks = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER"]
            if any(b in sql_query.upper() for b in blocks):
                return {"success": False, "error": "Safety Block: DML/DDL queries are not permitted."}

            # 5. Execute and Return
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(sql_query)
                dataset = await cur.fetchall()
                
                # Heuristic to find targeted table for UI
                table_hint = "results"
                for t in req.selected_tables:
                    if t.lower() in sql_query.lower():
                        table_hint = t
                        break
                
                return {
                    "success": True,
                    "query": sql_query,
                    "explanation": sql_spec.explanation,
                    "chat_answer": sql_spec.explanation, # Alias for frontend
                    "confidence": sql_spec.confidence,
                    "visualization": sql_spec.suggested_visualization,
                    "results": {table_hint: dataset},
                    "tables": [table_hint] if dataset else []
                }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}

# Aliases for backward compatibility with UI
@app.get("/table-metadata")
async def meta(): return await api_get_schema()

@app.post("/query-data")
async def query(req: QueryRequest): return await api_generate_query(req)

@app.get("/")
async def home():
    """Serve the modern PWA frontend."""
    return FileResponse("index.html")

@app.get("/manifest.json")
async def manifest():
    return FileResponse("manifest.json")

@app.get("/sw.js")
async def sw():
    return FileResponse("sw.js")

if __name__ == "__main__":
    import uvicorn
    # Important: loop="asyncio" ensures it respects our policy
    uvicorn.run(app, host="0.0.0.0", port=8001, loop="asyncio")
