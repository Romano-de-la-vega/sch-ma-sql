# schema_loader.py
import json, re
from pathlib import Path

# -------- Chargement & structure du schéma --------

def load_schema(path="tentative decomposer html\output_perimetre_restreint.json"):
    """
    Charge le JSON du MPD (format {tables: [ {name, description_table, columns:[{name,type,description}]} ]})
    et le transforme en:
    {
      "tables": {
        "ORDO_PROJECT": {
          "description_table": "Table des projets",
          "aliases": [],
          "columns": {
            "PROJECT_ID": {"type": "NUMBER",  "description": "..."},
            "NAME":       {"type": "VARCHAR", "description": "..."},
            ...
          },
          "fks": []
        },
        ...
      }
    }
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    tables_block = raw.get("tables", []) if isinstance(raw, dict) else raw
    schema = {"tables": {}}
    if isinstance(tables_block, list):
        for t in tables_block:
            if not isinstance(t, dict):
                continue
            tname = t.get("name")
            if not tname:
                continue
            desc = t.get("description_table") or ""
            cols_list = t.get("columns", []) or []
            cols = {}
            for c in cols_list:
                if not isinstance(c, dict):
                    continue
                cname = c.get("name")
                if not cname:
                    continue
                cols[cname] = {
                    "type": (c.get("type") or None),
                    "description": (c.get("description") or "")
                }
            schema["tables"][tname] = {
                "description_table": desc,
                "aliases": [],
                "columns": cols,
                "fks": []  # à remplir si tu ajoutes des FKs
            }
    elif isinstance(raw, dict) and isinstance(raw.get("tables"), dict):
        schema = raw
    else:
        raise ValueError("Format JSON inattendu pour le schéma")
    return schema


# -------- Sélection des tables (priorité description) --------

def pick_tables_for_question(schema, question, top_k=5):
    q = (question or "").lower()
    words = [w for w in q.replace(",", " ").replace(";", " ").split() if w]
    scores = []
    for tname, tinfo in schema["tables"].items():
        score = 0
        # 1) description de table (poids fort)
        desc = (tinfo.get("description_table") or "").lower()
        if desc:
            score += sum(3 for w in words if w in desc)
        # 2) nom + alias (poids moyen)
        toks = [tname] + tinfo.get("aliases", [])
        score += sum(2 for tok in toks if tok and tok.lower() in q)
        # 3) colonnes (poids faible)
        cols = list(tinfo.get("columns", {}).keys())[:30]
        score += sum(1 for c in cols if c.lower() in q)

        if score:
            scores.append((score, tname))
    scores.sort(reverse=True)
    return [t for _, t in scores[:top_k]] or list(schema["tables"].keys())[:3]


# -------- Contexte “handles” (debug/legacy) --------

def compress_to_handles(schema, tables, max_cols=12, show_types=True):
    """
    Produit un contexte compact, ex:
      T1=ORDO_PROJECT(PROJECT_ID NUMBER, NAME VARCHAR, START_DATE DATE)
      Relations: T1.BU_ID -> T2.ID
    """
    handles, rels = [], []
    for i, t in enumerate(tables, start=1):
        cols_meta = schema["tables"][t].get("columns", {})
        col_names = sorted(cols_meta.keys())[:max_cols]
        if show_types:
            cols_txt = ", ".join(
                f"{cn} {cols_meta[cn].get('type') or ''}".rstrip()
                for cn in col_names
            )
        else:
            cols_txt = ", ".join(col_names)
        handles.append(f"T{i}={t}(" + cols_txt + ")")
    # relations (si un jour tu en ajoutes dans fks)
    for i, t in enumerate(tables, start=1):
        for fk in schema["tables"][t].get("fks", []):
            to = fk["to"]
            tgt = to.split(".")[0]
            if tgt in tables:
                j = tables.index(tgt) + 1
                rels.append(f"T{i}.{fk['from']} -> T{j}.{to.split('.')[1]}")
    out = "\n".join(handles)
    if rels:
        out += "\nRelations: " + "; ".join(rels)
    return out


# -------- Sélection en 2 temps (tables -> colonnes) --------

def _sqltype_kind(sql_type: str | None) -> str:
    """Mappe SQL type -> {date|number|text|bool|other}."""
    if not sql_type:
        return "other"
    t = sql_type.upper()
    if any(k in t for k in ["DATE", "TIMESTAMP", "TIME"]):
        return "date"
    if any(k in t for k in ["INT", "NUM", "DEC", "FLOAT", "DOUBLE", "REAL"]):
        return "number"
    if any(k in t for k in ["CHAR", "TEXT", "STRING", "CLOB"]):
        return "text"
    if "BOOL" in t:
        return "bool"
    return "other"

# indices de type via le texte de la question
YEAR_RE = re.compile(r"\b(20[0-9]{2})\b")
_DATE_WORDS = {"date","année","an","mois","jour","semaine","trimestre","semestre","y","year","month","day"}
_NUM_WORDS  = {"combien","total","somme","moyenne","count","avg","sum","min","max","écart","répartition","top","%","percent","pourcentage","nb"}
_TEXT_WORDS = {"nom","name","titre","code","libellé","description","contient","like","ilike"}

def _extract_years(question: str) -> set[int]:
    return {int(y) for y in YEAR_RE.findall(question or "") if 2000 <= int(y) <= 2099}

def _expected_kinds(question: str) -> set[str]:
    """Détecte les types attendus d'après la question."""
    q = (question or "").lower()
    words = set(re.findall(r"[a-zà-ÿ%><=]+", q))
    kinds = set()
    if words & _DATE_WORDS or _extract_years(question):
        kinds.add("date")
    if words & _NUM_WORDS:
        kinds.add("number")
    if words & _TEXT_WORDS:
        kinds.add("text")
    return kinds

def pick_columns_for_table(schema: dict, table: str, question: str, max_cols: int = 20) -> list[str]:
    """
    Score les colonnes d'une table et retourne les top-N noms.
    Pondérations :
      +3  type colonne == type attendu (date/number/text)
      +2  match du nom de colonne dans la question
      +1  match de la description de colonne
    Boosts fixes :
      +4  ID / *_ID
      +3  NAME / CODE
      +3  START_DATE / END_DATE
    Bonus si une année 2000..2099 est citée :
      +3  pour les colonnes date
      -1  pour non-date (facultatif)
    """
    tmeta = schema["tables"][table]
    cols_meta = tmeta.get("columns", {})  # {col: {"type":..., "description":...}}
    q = (question or "").lower()

    kinds = _expected_kinds(question)
    years = _extract_years(question)
    tokens = set(re.findall(r"[a-z0-9_à-ÿ]+", q))

    scores = []
    for col, meta in cols_meta.items():
        cname = col.lower()
        cdesc = (meta.get("description") or "").lower()
        ckind = _sqltype_kind(meta.get("type"))

        score = 0
        # type attendu
        if kinds and ckind in kinds:
            score += 3
        # années explicites -> sur-boost des dates
        if years:
            if ckind == "date":
                score += 3
            else:
                score -= 1

        # match nom/desc
        if any(tok and tok in cname for tok in tokens):
            score += 2
        if cdesc and any(tok and tok in cdesc for tok in tokens):
            score += 1

        # boosts fixes
        if col == "ID" or col.endswith("_ID"):
            score += 4
        if col in ("NAME","CODE"):
            score += 3
        if col in ("START_DATE","END_DATE"):
            score += 3

        scores.append((score, col))

    scores.sort(key=lambda x: (-x[0], x[1]))

    must_keep = [c for c in ("ID","PROJECT_ID","NAME","CODE","START_DATE","END_DATE") if c in cols_meta]

    picked = []
    for _, c in scores:
        if c not in picked:
            picked.append(c)
        if len(picked) >= max_cols:
            break

    for c in must_keep:
        if c not in picked and len(picked) < max_cols:
            picked.append(c)

    return picked

def build_context_pack(schema: dict, question: str, top_tables: int = 3, cols_per_table: int = 20):
    """
    1) Sélectionne les meilleures tables (pick_tables_for_question).
    2) Sélectionne les meilleures colonnes pour chaque table.
    3) Construit un contexte compact avec types (ex: 3 tables × 20 colonnes).
    Retourne: (context_str, tables, selected_cols_by_table)
    """
    tables = pick_tables_for_question(schema, question, top_k=top_tables)
    selected_cols = {t: pick_columns_for_table(schema, t, question, max_cols=cols_per_table) for t in tables}

    # Contexte compact
    lines = []
    for i, t in enumerate(tables, start=1):
        cols_meta = schema["tables"][t]["columns"]
        cols = selected_cols[t]
        pretty = ", ".join(f"{c} {(cols_meta[c].get('type') or '')}".rstrip() for c in cols)
        lines.append(f"T{i}={t}({pretty})")

    # Relations (si tu remplis un jour schema["tables"][t]["fks"])
    rels = []
    for i, t in enumerate(tables, start=1):
        for fk in schema["tables"][t].get("fks", []):
            to = fk["to"]
            tgt = to.split(".")[0]
            if tgt in tables:
                j = tables.index(tgt) + 1
                rels.append(f"T{i}.{fk['from']} -> T{j}.{to.split('.')[1]}")
    context = "\n".join(lines)
    if rels:
        context += "\nRelations: " + "; ".join(rels)

    return context, tables, selected_cols


__all__ = [
    "load_schema",
    "pick_tables_for_question",
    "compress_to_handles",
    "pick_columns_for_table",
    "build_context_pack",
]
