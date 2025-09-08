# prompts.py

NL2SQL_PROMPT = """Tu es un assistant SQL. Dialecte: PostgreSQL.
Contexte schéma (handles courts, repères visuels UNIQUEMENT) :
{context}

Règles STRICTES:
- Écris une SEULE requête SQL valide PostgreSQL.
- SELECT uniquement (aucun DDL/DML).
- Utilise UNIQUEMENT les noms de tables RÉELS (pas T1/T2/T3).
- Utilise uniquement les colonnes visibles dans le contexte.
- Joins selon les relations indiquées.
- Ajoute LIMIT {limit} si besoin.
- Format de sortie: la requête SQL PURE sur une seule ligne ou plusieurs, SANS ``` ni commentaires.

Question: "{question}"
SQL:"""


INTERPRET_PROMPT = """Tu es analyste data.
Contexte:
- Question: {question}
- Aperçu (50 lignes max):
{sample}
- Statistiques: {stats}

Donne un résumé clair (2–4 phrases) + 3–5 bullet points (tendances, pics, anomalies, comparaisons). Mentionne les limites éventuelles (qualité données, périmètre)."""
