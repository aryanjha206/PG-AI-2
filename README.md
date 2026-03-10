# 🧠 PG AI Query Engine

**NLP-SQL** — Unlock the power of your database with natural language. Ask for summaries, trends, or specific data points. Converts plain English into optimized SQL queries using automatic schema discovery, dual-tier AI inference, and built-in safety protections.


---

## 🚀 Key Features

- **🧠 `generate_query()`**: Core function that converts natural language to validated SQL.
- **📊 Automatic Schema Discovery**: Uses `get_database_tables()` and `get_table_details()` to analyze your schema automatically — no manual configuration needed.
- **⚡ Dual-Tier AI**:
  - **Tier 1**: Pollinations AI (fast, keyless cloud inference)
  - **Tier 2**: G4F GPT4Free (resilient fallback)
- **🔒 Safety & Security**: Blocks system table access (`information_schema`, `pg_catalog`), enforces read-only mode, validates all generated SQL.
- **📋 Structured JSON Responses**: Output with `query`, `explanation`, `warnings`, `confidence`, and `suggested_visualization`.
- **💬 Conversational Answers**: AI-generated natural language responses alongside raw data.
- **☁️ Cloud Database Ready**: Native Neon PostgreSQL support with DNS-over-HTTPS fallback.

---

## 🛠️ Architecture

```mermaid
graph TD
    User[Natural Language Query] --> Frontend[Web Chat UI — ChatGPT Style]
    Frontend --> API[FastAPI — PG AI Engine]
    API --> Schema[Automatic Schema Discovery]
    Schema --> AI{generate_query — Dual-Tier AI}
    AI -->|Tier 1| Pollinations[Pollinations AI]
    AI -->|Tier 2| G4F[GPT4Free]
    AI --> Validate[SQL Safety Validation]
    Validate --> DB[(Neon PostgreSQL)]
    DB --> Results[JSON Response]
    Results --> Frontend
```

---

## 🏁 Quick Start

### 1. Requirements
```bash
pip install pgai
```

### 2. Configure Database
Set your Neon/PostgreSQL connection string in `api.py`:
```python
NEON_DB_URL = "postgresql://user:pass@host/db"
```

### 3. Run
```bash
python app.py
```

---

## 📡 API Reference

| Endpoint | Method | Description |
| :--- | :--- | :--- |
| `/generate-query` | POST | Core NL-to-SQL — equivalent to `SELECT generate_query('...')` |
| `/get-database-tables` | GET | Schema discovery — equivalent to `get_database_tables()` |
| `/get-table-details/{name}` | GET | Column details — equivalent to `get_table_details('...')` |
| `/audit-logs` | GET | Query audit history (admin only) |

### Example Response
```json
{
  "query": "SELECT * FROM students WHERE status ILIKE '%present%';",
  "success": true,
  "intent": "table_data_retrieval",
  "explanation": "Retrieves all students with present attendance status",
  "warnings": [],
  "confidence": 0.92,
  "suggested_visualization": "table",
  "tables": ["students"],
  "results": { "students": [...] }
}
```

---

## 🔐 Safety & Security

Built-in safety protections:

- **System Table Protection**: Blocks access to `information_schema` and `pg_catalog`
- **Read-Only Mode**: Only `SELECT` queries are allowed
- **Dangerous Keyword Blocking**: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, `TRUNCATE` are blocked
- **Table Scope Enforcement**: Queries can only reference user-selected or auto-discovered tables

---

## 📜 Role-Based Access

| Role | Tables | Description |
| :--- | :--- | :--- |
| **Viewer** | Up to 4 | Basic read access |
| **Analyst** | Up to 8 | Extended analysis |
| **Admin** | Up to 10 | Full access + audit logs |

---

*PG AI Query Engine v3.0 — Powered by Automatic Schema Discovery & Dual-Tier AI*
