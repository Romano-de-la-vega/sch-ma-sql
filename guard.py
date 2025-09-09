# guard.py
import re

DDL_DML = re.compile(r"\b(UPDATE|DELETE|INSERT|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE)\b", re.I)

def is_select_only(sql: str) -> bool:
    return sql.strip().upper().startswith("SELECT") and not DDL_DML.search(sql)

def whitelist_ok(sql: str, allowed_tables: list[str]) -> bool:
    # check très simple (à améliorer avec un vrai parseur si besoin)
    # les clauses FROM/JOIN à l'intérieur d'expressions parenthésées (EXTRACT, sous-requêtes...) sont ignorées
    def strip_parentheses(s: str) -> str:
        # retire récursivement le contenu des parenthèses pour éviter les faux positifs
        previous = None
        while previous != s:
            previous = s
            s = re.sub(r'\([^()]*\)', ' ', s)
        return s

    sql_clean = strip_parentheses(sql)
    found = re.findall(r'\bfrom\s+([a-zA-Z0-9_."]+)', sql_clean, flags=re.I)
    found += re.findall(r'\bjoin\s+([a-zA-Z0-9_."]+)', sql_clean, flags=re.I)
    found = [f.strip('"').split(".")[-1] for f in found]
    return all(t in allowed_tables for t in found)

def ensure_limit(sql: str, default_limit=5000) -> str:
    if re.search(r"\blimit\s+\d+", sql, re.I):
        return sql
    return sql.rstrip(" ;") + f" LIMIT {default_limit};"
