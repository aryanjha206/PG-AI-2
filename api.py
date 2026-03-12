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
from pgai.semantic_catalog import loader, render

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════
NEON_DB_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_5RdSgjpHCQ6P@ep-sweet-thunder-a1rqx5ar-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

class Config:
    BANNER = "PG AI Query Engine v3.3.0"
    MODEL = "openai" 
    BASE_URL = "https://text.pollinations.ai/v1"

# ═══════════════════════════════════════════════════════════════
# UTILS: DNS & CONNECTION
# ═══════════════════════════════════════════════════════════════
_RESOLVED_CACHE = {}

async def get_resolved_url() -> str:
    """Resolve Neon host using Google DoH to bypass restrictive DNS filters."""
    if NEON_DB_URL in _RESOLVED_CACHE:
        return _RESOLVED_CACHE[NEON_DB_URL]
    
    try:
        from urllib.parse import urlparse, urlunparse
        parsed = urlparse(NEON_DB_URL)
        host = parsed.hostname
        if host and (".neon.tech" in host or ".aws.neon" in host):
            doh_url = f"https://dns.google/resolve?name={host}"
            resp = await asyncio.to_thread(requests.get, doh_url, timeout=5)
            data = resp.json()
            ip = next((a["data"] for a in data.get("Answer", []) if a["type"] == 1), None)
            if ip:
                endpoint_id = host.split(".")[0]
                query = parsed.query
                opt = f"options=endpoint%3D{endpoint_id}"
                query = f"{query}&{opt}" if query else opt
                netloc = parsed.netloc.replace(host, ip)
                final_url = urlunparse((parsed.scheme, netloc, parsed.path, parsed.params, query, parsed.fragment))
                _RESOLVED_CACHE[NEON_DB_URL] = final_url
                return final_url
    except Exception as e:
        print(f"[PG AI] DNS Resolution Warning: {e}")
    
    return NEON_DB_URL

async def get_connection():
    """Returns an async psycopg connection to the database."""
    url = await get_resolved_url()
    try:
        conn = await psycopg.AsyncConnection.connect(url, autocommit=True, connect_timeout=10)
        return conn
    except Exception as e:
        print(f"[PG AI] Database connection failed: {e}")
        raise

# ═══════════════════════════════════════════════════════════════
# AI AGENT
# ═══════════════════════════════════════════════════════════════
from openai import AsyncOpenAI

client = AsyncOpenAI(base_url=Config.BASE_URL, api_key='keyless')
llm = OpenAIChatModel(
    model_name=Config.MODEL,
    openai_client=client
)

class SQLResponse(BaseModel):
    sql: str = Field(description="The valid PostgreSQL SELECT query.")
    explanation: str = Field(description="A brief explanation of how the query works.")
    confidence: float = Field(default=1.0, description="Confidence score 0-1.")
    suggested_visualization: str = Field(default="table", description="Visualization type: table, chart, or map.")

sql_agent = Agent(
    llm,
    result_type=SQLResponse,
    system_prompt=(
        "You are 'PG AI', a state-of-the-art PostgreSQL Data Assistant.\n"
        "Your mission: Generate high-performance SQL SELECT queries using pgai and pgvector.\n\n"
        "CORE CAPABILITIES:\n"
        "1. pgvector Integration: For similarity search on 'vector' columns, use the <-> (L2 distance), <#> (negative dot product), or <=> (cosine distance) operators.\n"
        "2. pgai Synergy: Leverage advanced schema discovery metadata. If asked for semantic meaning, optimize for vector-based retrieval if appropriate columns exist.\n"
        "3. SQL Excellence: Use ILIKE for case-insensitive searches, proper JOINs, and CTEs for complex logic.\n"
        "4. Security: STRICTLY SELECT queries only. No DROP, DELETE, INSERT, UPDATE, or ALTER.\n"
        "5. Schema Awareness: Use the exact column and table names provided in the context."
    )
)

# ═══════════════════════════════════════════════════════════════
# FASTAPI APP
# ═══════════════════════════════════════════════════════════════
app = FastAPI(title="PG AI Engine", description="Natural Language to SQL via pgai & pgvector")

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

@app.get("/health-check")
async def health_check():
    """Verify database connection and extensions."""
    try:
        async with await get_connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT extname FROM pg_extension")
                extensions = [r[0] for r in await cur.fetchall()]
                return {
                    "status": "online",
                    "extensions": extensions,
                    "pgvector_ready": "vector" in extensions,
                    "pgai_ready": "ai" in extensions or "pgai" in extensions
                }
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.get("/get-database-tables")
async def api_get_schema():
    """Return schema map for the UI side-panel."""
    try:
        async with await get_connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("""
                    SELECT table_name, column_name, data_type, udt_name
                    FROM information_schema.columns 
                    WHERE table_schema = 'public' 
                    ORDER BY table_name, ordinal_position
                """)
                rows = await cur.fetchall()
                schema = {}
                for r in rows:
                    type_info = r['udt_name'] if r['data_type'] == 'USER-DEFINED' else r['data_type']
                    schema.setdefault(r['table_name'], {})[r['column_name']] = type_info
                return schema
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/generate-query")
async def api_generate_query(req: QueryRequest):
    """The core engine: NL -> pgai Schema Context -> pydantic-ai -> SQL -> Execution."""
    try:
        async with await get_connection() as conn:
            # 1. Discover relevant tables using pgai logic
            async with conn.cursor() as cur:
                if req.selected_tables:
                    await cur.execute("SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = ANY(%s) AND c.relkind = 'r'", (req.selected_tables,))
                else:
                    await cur.execute("SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind = 'r' LIMIT 20")
                oids = [row[0] for row in await cur.fetchall()]
            
            if not oids:
                return {"success": False, "error": "No tables detected."}

            # 2. Preparation: Use pgai Python tools for rich schema context
            tables = await loader.load_tables(conn, oids, sample_size=3)
            schema_context = render.render_tables(tables)

            # 3. Call Agent
            user_msg = f"User Request: {req.prompt}\n\nDATABASE SCHEMA CONTEXT:\n{schema_context}"
            result = await sql_agent.run(user_msg)
            
            sql_spec = result.data
            sql_query = sql_spec.sql.strip().rstrip(';') + ';'
            
            # 4. Security Check
            blocks = ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "GRANT", "REVOKE"]
            if any(b in sql_query.upper() for b in blocks):
                return {"success": False, "error": "Safety Block: Potential modification query detected."}

            # 5. Execute
            async with conn.cursor(row_factory=dict_row) as cur:
                print(f"[PG AI] Executing: {sql_query}")
                await cur.execute(sql_query)
                dataset = await cur.fetchall()
                
                table_hint = "results"
                if req.selected_tables:
                    for t in req.selected_tables:
                        if t.lower() in sql_query.lower():
                            table_hint = t; break
                
                return {
                    "success": True,
                    "query": sql_query,
                    "explanation": sql_spec.explanation,
                    "chat_answer": sql_spec.explanation, # Alias for UI
                    "confidence": sql_spec.confidence,
                    "visualization": sql_spec.suggested_visualization,
                    "results": {table_hint: dataset},
                    "tables": [table_hint] if dataset else []
                }

    except Exception as e:
        return {"success": False, "error": str(e)}

# Aliases
@app.get("/table-metadata")
async def meta(): return await api_get_schema()

@app.post("/query-data")
async def query(req: QueryRequest): return await api_generate_query(req)

@app.get("/")
async def home():
    return FileResponse("index.html")

@app.get("/manifest.json")
async def manifest(): return FileResponse("manifest.json")

@app.get("/sw.js")
async def sw(): return FileResponse("sw.js")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, loop="asyncio")
