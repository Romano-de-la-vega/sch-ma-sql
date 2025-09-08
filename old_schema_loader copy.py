# schema_loader.py
import yaml, re
from pathlib import Path

# schema_loader.py
import json, re
from pathlib import Path

def load_schema(path="schema.json"):
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    # Transformer le format en dictionnaire attendu
    schema = {"tables": {}}
    for t in data.get("tables", []):
        tname = t["name"]
        # Colonnes -> dict {colname: description}
        cols = {c["name"]: c.get("description", "") for c in t.get("columns", [])}
        schema["tables"][tname] = {
            "aliases": [],   # tu pourras les remplir plus tard
            "columns": cols,
            "fks": []        # à compléter si tu ajoutes les clés étrangères
        }
    return schema


def pick_tables_for_question(schema, question, top_k=5):
    q = question.lower()
    scores = []
    for tname, tinfo in schema["tables"].items():
        score = 0
        # 1) match sur nom/aliases
        toks = [tname] + tinfo.get("aliases", [])
        score += sum(1 for tok in toks if tok and tok.lower() in q)
        # 2) match colonnes fréquentes
        cols = list(tinfo.get("columns", {}).keys())[:20]
        score += sum(1 for c in cols if c.lower() in q)
        if score:
            scores.append((score, tname))
    scores.sort(reverse=True)
    return [t for _, t in scores[:top_k]] or list(schema["tables"].keys())[:3]

def compress_to_handles(schema, tables):
    """Retourne un contexte court du style:
       T1=ordo_project(id, code, name, bu_id, start_date, status)
       T2=bu(id, code, name)
       Relations: T1.bu_id -> T2.id
    """
    handles, rels = [], []
    for i, t in enumerate(tables, start=1):
        cols = list(schema["tables"][t]["columns"].keys())[:12]
        handles.append(f"T{i}={t}(" + ", ".join(cols) + ")")
    # relations
    for i, t in enumerate(tables, start=1):
        for fk in schema["tables"][t].get("fks", []):
            to = fk["to"]  # ex: "bu.id"
            tgt = to.split(".")[0]
            if tgt in tables:
                j = tables.index(tgt) + 1
                rels.append(f"T{i}.{fk['from']} -> T{j}.{to.split('.')[1]}")
    out = "\n".join(handles)
    if rels:
        out += "\nRelations: " + "; ".join(rels)
    return out
