# nl2sql.py
import os
from schema_loader import load_schema, pick_tables_for_question, compress_to_handles
from prompts import NL2SQL_PROMPT
# from openai import OpenAI  # selon ton SDK

def build_sql(question, limit=5000):
    schema = load_schema()
    tables = pick_tables_for_question(schema, question)
    ctx = compress_to_handles(schema, tables)
    prompt = NL2SQL_PROMPT.format(context=ctx, question=question, limit=limit)

    # client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    # resp = client.chat.completions.create(
    #   model="gpt-4o-mini", temperature=0,
    #   messages=[{"role":"user","content":prompt}]
    # )
    # sql = resp.choices[0].message.content.strip()

    sql = "-- ici tu récupères la réponse du modèle --"
    return sql, tables
