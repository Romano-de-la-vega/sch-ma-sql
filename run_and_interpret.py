# run_and_interpret.py
import psycopg2, pandas as pd
from guard import is_select_only, whitelist_ok, ensure_limit
from schema_loader import load_schema
# from openai import OpenAI

def run_sql(sql, dsn):
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=cols)
    return df

INTERPRET_PROMPT = """Tu es analyste data.
Contexte:
- Question: {question}
- Aperçu (jusqu'à 50 lignes):
{sample}
- Statistiques: {stats}

Donne un résumé clair (2–4 phrases) + 3–5 bullet points (tendances, pics, anomalies, comparaisons). Mentionne les limites éventuelles (qualité données, périmètre)."""

def quick_stats(df):
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

def interpret(question, df):
    sample = df.head(50).to_csv(index=False)
    stats = quick_stats(df)
    prompt = INTERPRET_PROMPT.format(question=question, sample=sample, stats=stats)
    # resp = client.chat.completions.create( ... )
    # return resp.choices[0].message.content.strip()
    return "<résumé & bullets du modèle>"
