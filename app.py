# --- en haut du fichier ---
import os, json, re
import psycopg2
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

from schema_loader import load_schema, pick_tables_for_question, compress_to_handles
from guard import is_select_only, whitelist_ok, ensure_limit
from prompts import NL2SQL_PROMPT, INTERPRET_PROMPT

# OpenAI (optionnel)
from openai import OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

# <<< PARAMÈTRES DB PAR DÉFAUT (cachés de l'API) >>>
DEFAULT_DB = {
    "host": "localhost",
    "port": 5432,
    "db": "planisware",
    "user": "planisware",
    "password": "planisware",
}

app = FastAPI(title="NL→SQL + Insight")

PG_DSN = os.getenv("PG_DSN")

class AskPayload(BaseModel):
    question: str
    limit: int | None = 5000
    debug: bool = True     # pour afficher le bloc "debug" dans la réponse
    sample: int | None = 50


# --- DSN builder (aucun champ lu depuis la requête) ---
def build_dsn() -> str:
    # 1) si PG_DSN est défini dans l'env, on l'utilise
    if os.getenv("PG_DSN"):
        return os.getenv("PG_DSN")

    # 2) sinon, on compose avec env (s'ils existent) ou DEFAULT_DB
    host = os.getenv("PG_HOST", DEFAULT_DB["host"])
    port = os.getenv("PG_PORT", str(DEFAULT_DB["port"]))
    db   = os.getenv("PG_DB",   DEFAULT_DB["db"])
    user = os.getenv("PG_USER", DEFAULT_DB["user"])
    pwd  = os.getenv("PG_PASSWORD", DEFAULT_DB["password"])

    # format libpq (simple, pas d’URL-encoding nécessaire)
    return f"host={host} port={port} dbname={db} user={user} password={pwd}"



# --- exécution SQL avec DSN fourni par le code ---
def run_sql(sql: str, dsn: str):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)




def llm_generate(prompt: str, fallback: str = None) -> str:
    if not client:
        return fallback or "-- LLM non configuré (OPENAI_API_KEY manquant) --"
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=0,
        messages=[{"role":"user","content":prompt}]
    )
    return resp.choices[0].message.content.strip()

def quick_stats(df: pd.DataFrame) -> dict:
    out = {}
    for c in df.select_dtypes(include="number").columns:
        s = df[c]
        out[c] = {
            "count": int(s.count()),
            "mean": float(s.mean()) if s.count() else None,
            "min": float(s.min()) if s.count() else None,
            "max": float(s.max()) if s.count() else None,
        }
    return out

import re



def clean_sql_output(raw: str) -> str:
    if not raw:
        return ""
    s = raw.strip()

    # 1) vire TOUTES les fences et le mot-clé sql, où qu'ils soient
    s = s.replace("```sql", "").replace("```SQL", "").replace("```", "")

    # 2) extrait la première requête SELECT ... ; (case-insensitive)
    m = re.search(r"(SELECT[\s\S]*?);", s, flags=re.I)
    if m:
        s = m.group(1) + ";"
    else:
        # fallback : tout ce qui suit le premier SELECT
        m = re.search(r"(SELECT[\s\S]*)", s, flags=re.I)
        if m:
            s = m.group(1)

    # 3) nettoie espaces redondants
    return s.strip()

def replace_handles_with_tables(sql: str, tables: list[str]) -> str:
    out = sql
    for i, t in enumerate(tables, start=1):
        out = re.sub(rf'\bT{i}\b', t, out)           # T1 -> ordo_project
        out = out.replace(f'"T{i}"', f'"{t}"')       # "T1" -> "ordo_project"
    return out

def is_select_only_tol(sql: str) -> bool:
    # version tolérante qui ignore espaces/parenthèses de tête
    s = (sql or "").lstrip().lstrip("(")
    return s[:6].upper() == "SELECT" and not re.search(
        r"\b(UPDATE|DELETE|INSERT|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b", s, re.I
    )


def replace_handles_with_tables(sql: str, tables: list[str]) -> str:
    """
    Si le modèle a quand même utilisé T1/T2/T3, on les remplace par les vrais noms.
    Attention: c'est safe parce qu'on contrôle les handles.
    """
    out = sql
    for i, t in enumerate(tables, start=1):
        # remplace T1, "T1", T1.  par le vrai nom
        out = re.sub(rf'\bT{i}\b', t, out)
        out = out.replace(f'"T{i}"', f'"{t}"')
    return out

# --- endpoint /ask (aucune info DB lue depuis le body) ---
@app.post("/ask")
def ask(payload: AskPayload):
    # 1) Contexte (tables candidates)
    schema = load_schema()
    tables = pick_tables_for_question(schema, payload.question, top_k=5)
    ctx = compress_to_handles(schema, tables)

    # 2) Génération SQL
    llm_sql_prompt = NL2SQL_PROMPT.format(
        context=ctx,
        question=payload.question,
        limit=payload.limit or 5000,
    )
    raw_sql = llm_generate(llm_sql_prompt, fallback="SELECT 1;")
    sql_clean = clean_sql_output(raw_sql)                         # retire ```sql ... ```
    sql_replaced = replace_handles_with_tables(sql_clean, tables) # T1/T2 -> vrais noms

    # 3) Garde-fous
    if not is_select_only(sql_replaced):
        return {
            "error": "SQL généré non conforme (pas SELECT uniquement).",
            "sql_raw": raw_sql, "sql_clean": sql_clean, "sql_final": sql_replaced
        }
    if not whitelist_ok(sql_replaced, allowed_tables=tables):
        return {
            "error": "SQL utilise des tables non autorisées pour ce contexte.",
            "sql_raw": raw_sql, "sql_clean": sql_clean, "sql_final": sql_replaced,
            "allowed_tables": tables
        }

    sql_final = ensure_limit(sql_replaced, payload.limit or 5000)

    # 4) Exécution
    dsn = build_dsn()
    df = run_sql(sql_final, dsn)

    # 5) Ce que voit l’IA pour l’interprétation
    sample_n = (getattr(payload, "sample", None) or 50)
    df_sample = df.head(sample_n)
    data_preview = df_sample.to_dict(orient="records")      # montré dans la réponse
    sample_csv = df_sample.to_csv(index=False)              # exactement ce qu'on passerait au LLM
    stats = quick_stats(df)

    # 6) Insight : si 0 ligne -> pas d'appel LLM (évite d'inventer)
    if len(df) == 0:
        insight = (
            "Aucune ligne renvoyée par la requête.\n"
            f"Requête exécutée : {sql_final}\n"
            "Pistes : élargir la période, assouplir les filtres, vérifier les colonnes de date."
        )
    else:
        iprompt = INTERPRET_PROMPT.format(
            question=payload.question,
            sample=sample_csv,                 # <-- ce que l'IA reçoit
            stats=json.dumps(stats, ensure_ascii=False),
        )
        insight = llm_generate(iprompt, fallback="(Résumé non généré : pas d'API LLM configurée)")

    # 7) Réponse + bloc debug (pour voir ce qui a été envoyé/obtenu)
    resp = {
        "sql": sql_final,
        "row_count": int(len(df)),
        "data_preview": data_preview,          # premières lignes réelles
        "insight": insight,
        "tables_used": tables,
    }

    if getattr(payload, "debug", True):
        resp["debug"] = {
            "context": ctx,                    # handles (tables/colonnes) passés au LLM SQL
            "llm_sql_prompt": llm_sql_prompt,  # prompt SQL exact
            "llm_sql_raw": raw_sql,
            "sql_clean": sql_clean,
            "sql_after_handles": sql_replaced,
            "sql_final": sql_final,
            "sample_csv": sample_csv,          # données exactes vues par l'IA pour l'insight
            "stats": stats,
        }

    return resp