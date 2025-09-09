"""Helper to build SQL queries from natural language questions.

The previous implementation only selected tables and then dumped the first
alphabetically sorted columns of each table into the prompt context. As a
result, irrelevant columns (e.g. ``AGL_AA_S_CUR_RELEASE``) frequently ended up
at the top of the context and were chosen by the LLM when generating ``SELECT``
clauses.  To provide better guidance, we now use ``build_context_pack`` which
scores and ranks columns according to the question (date/number/text hints,
name matches, etc.).  This ensures that the most meaningful columns – IDs,
names, dates – appear first.
"""

import os
from schema_loader import load_schema, build_context_pack
from prompts import NL2SQL_PROMPT
# from openai import OpenAI  # selon ton SDK


def build_sql(question, limit=5000):
    schema = load_schema()
    context, tables, _ = build_context_pack(schema, question)
    prompt = NL2SQL_PROMPT.format(context=context, question=question, limit=limit)

    # client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    # resp = client.chat.completions.create(
    #   model="gpt-4o-mini", temperature=0,
    #   messages=[{"role":"user","content":prompt}]
    # )
    # sql = resp.choices[0].message.content.strip()

    sql = "-- ici tu récupères la réponse du modèle --"
    return sql, tables
