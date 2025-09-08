#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convertit un MPD HTML + une whitelist (txt) en un JSON compact :
- Chaque ligne de la whitelist = un nom de table à inclure
- Pour chaque table trouvée dans le MPD :
    { name, description_table, columns: [ {name, type, description} ] }
- Si une table de la whitelist est introuvable -> on la saute (et on loggue)

Robuste aux MPD HTML hétérogènes :
- 2 parseurs :
  (A) parse structure HTML (titres + tableaux) si possible
  (B) fallback regex sur le texte brut "Table: ... Class: ..." et colonnes "NAME : desc" ou "NAME - desc"

Usage:
    python mpd_whitelist_to_json.py \
        --mpd "/mnt/data/mpd E715 1.html" \
        --whitelist tables.txt \
        --out output.json

Dépendances suggérées: bs4, lxml (mais fonctionne aussi avec html.parser).
"""

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore


# ---------------------- Utilitaires ----------------------
UPNAME = re.compile(r"^[A-Z0-9_]+$")
COL_LINE = re.compile(r"^\s*([A-Z][A-Z0-9_]{1,})\s*(?:[:\-–]\s+(.+))?$")
# ex: "PROJECT_ID : identifiant" OU "PROJECT_ID - identifiant"

TABLE_BLOCK = re.compile(
    r"Table:\s*(?P<name>[^\r\n]+?)\s*(?P<body>.*?)(?=\n\s*Table:\s|\Z)",
    re.DOTALL | re.IGNORECASE,
)
CLASS_LINE = re.compile(r"\bClass\s*:\s*(.+)", re.IGNORECASE)
COLUMNS_HEADER = re.compile(r"\b(Columns?|Fields?|Attributes?)\b", re.IGNORECASE)
TYPE_HINT = re.compile(r"\b(type|datatype|data type)\s*[:\-]\s*(\w+)", re.IGNORECASE)


# ---------------------- Chargement whitelist ----------------------
def load_whitelist(path: Path) -> List[str]:
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8", errors="ignore").splitlines()]
    # enlever vides et commentaires (# ...)
    out = [ln for ln in lines if ln and not ln.lstrip().startswith('#')]
    return out


# ---------------------- Parse HTML structure (A) ----------------------
def parse_html_struct(html: str) -> Dict[str, dict]:
    """Tente d'extraire tables/colonnes via structure HTML (titres, tableaux)."""
    if BeautifulSoup is None:
        return {}
    soup = BeautifulSoup(html, "lxml" if 'lxml' in globals() else "html.parser")

    index: Dict[str, dict] = {}

    # Heuristique : les titres contenant "Table:" définissent une section
    title_nodes = soup.find_all(string=re.compile(r"^\s*Table:\s*", re.IGNORECASE))
    for node in title_nodes:
        section_text = node.strip()
        # Ex: "Table: ORDO_PROJECT"
        m = re.search(r"Table:\s*(.+)$", section_text, re.IGNORECASE)
        if not m:
            continue
        tname = m.group(1).strip()
        key = tname.lower()

        # Chercher description de classe proche
        desc = None
        parent = getattr(node, 'parent', None)
        block = parent
        # Regarder quelques siblings suivants
        if parent is not None:
            sibs = list(parent.next_siblings)
        else:
            sibs = []
        raw_text = []
        for s in sibs[:12]:  # lecture courte autour
            try:
                txt = s.get_text(" ", strip=True) if hasattr(s, 'get_text') else str(s).strip()
            except Exception:
                txt = str(s)
            if txt:
                raw_text.append(txt)
            # Première occurrence "Class:"
            cm = re.search(r"Class\s*:\s*(.+)", txt, re.IGNORECASE)
            if cm and not desc:
                desc = cm.group(1).strip()
        near_text = "\n".join(raw_text)

        # Colonnes : chercher un tableau HTML à proximité
        columns: List[dict] = []
        table_el = None
        if parent is not None:
            # un tableau dans les siblings
            table_el = next((el for el in sibs if getattr(el, 'name', '') == 'table'), None)
        if table_el is None:
            # fallback: premier <table> après le titre dans le DOM
            table_el = soup.find('table') if soup else None

        if table_el is not None:
            try:
                rows = table_el.find_all('tr')
                # tenter d'identifier colonnes [Name, Type, Description]
                headers = [th.get_text(" ", strip=True).lower() for th in rows[0].find_all(['th','td'])] if rows else []
                for r in rows[1:]:
                    cells = [td.get_text(" ", strip=True) for td in r.find_all(['td','th'])]
                    if not cells:
                        continue
                    col_name = cells[0].strip()
                    col_type = None
                    col_desc = None
                    if len(cells) >= 2:
                        # heuristique : si un header ressemble à 'type' ou 'description'
                        if any('type' in h for h in headers):
                            try:
                                idx = headers.index('type')
                                col_type = cells[idx] if idx < len(cells) else None
                            except Exception:
                                pass
                        if any('description' in h for h in headers):
                            try:
                                idx = headers.index('description')
                                col_desc = cells[idx] if idx < len(cells) else None
                            except Exception:
                                pass
                    # si rien, tenter "NAME - desc"
                    if not col_desc and len(cells) == 2:
                        col_desc = cells[1]
                    if col_name:
                        columns.append({"name": col_name, "type": col_type, "description": col_desc})
            except Exception:
                pass

        # Si pas de colonnes via tableau, tenter regex sur le texte proche
        if not columns and near_text:
            for line in near_text.splitlines():
                m2 = COL_LINE.match(line)
                if m2:
                    cname = m2.group(1).strip()
                    cdesc = (m2.group(2) or '').strip() or None
                    if UPNAME.match(cname):
                        columns.append({"name": cname, "type": None, "description": cdesc})

        index[key] = {
            "name": tname,
            "description_table": desc,
            "columns": columns,
        }

    return index


# ---------------------- Parse texte (B) ----------------------
def parse_text_blocks(html: str) -> Dict[str, dict]:
    """Fallback en scannant le texte complet du HTML."""
    # retirer balises, garder texte brut si possible
    text = html
    if BeautifulSoup is not None:
        try:
            text = BeautifulSoup(html, "lxml" if 'lxml' in globals() else "html.parser").get_text("\n")
        except Exception:
            pass

    index: Dict[str, dict] = {}
    for m in TABLE_BLOCK.finditer(text):
        tname = m.group('name').strip()
        body = m.group('body') or ''
        key = tname.lower()
        # description de table
        desc = None
        cm = CLASS_LINE.search(body)
        if cm:
            desc = cm.group(1).strip()

        # extraire colonnes heuristiques dans le bloc
        columns: List[dict] = []
        # Chercher éventuellement une section Columns/Fields
        block = body
        # Essayer ligne par ligne
        for line in block.splitlines():
            line = line.strip()
            m2 = COL_LINE.match(line)
            if m2:
                cname = m2.group(1).strip()
                cdesc = (m2.group(2) or '').strip() or None
                if UPNAME.match(cname):
                    columns.append({"name": cname, "type": None, "description": cdesc})

        index[key] = {
            "name": tname,
            "description_table": desc,
            "columns": columns,
        }

    return index


# ---------------------- Merge indexes ----------------------
def merge_indexes(a: Dict[str, dict], b: Dict[str, dict]) -> Dict[str, dict]:
    out = dict(a)
    for k, v in b.items():
        if k not in out or not out[k].get('columns'):
            out[k] = v
        else:
            # compléter colonnes manquantes par b
            seen = {c['name'].lower() for c in out[k]['columns']}
            for c in v.get('columns', []):
                if c['name'].lower() not in seen:
                    out[k]['columns'].append(c)
            # compléter description si absente
            if not out[k].get('description_table') and v.get('description_table'):
                out[k]['description_table'] = v['description_table']
    return out


# ---------------------- Programme principal ----------------------

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--mpd', required=True, help='Chemin du MPD HTML')
    p.add_argument('--whitelist', required=True, help='Chemin du fichier txt, 1 table par ligne')
    p.add_argument('--out', required=True, help='Chemin du JSON de sortie')
    args = p.parse_args()

    mpd_path = Path(args.mpd)
    wl_path = Path(args.whitelist)
    out_path = Path(args.out)

    html = mpd_path.read_text(encoding='utf-8', errors='ignore')

    # 1) index via structure HTML
    idx_a = parse_html_struct(html)
    # 2) index via texte brut
    idx_b = parse_text_blocks(html)
    # 3) fusion (A prioritaire, B en complément)
    index = merge_indexes(idx_a, idx_b)

    # Normaliser les noms trouvés → map casefold
    norm_index = {k.lower(): v for k, v in index.items()}

    # Charger whitelist
    wanted = load_whitelist(wl_path)

    result = {
        "source_mpd": str(mpd_path),
        "tables": [],
        "skipped": [],
    }

    for raw_name in wanted:
        key = raw_name.strip().lower()
        info = norm_index.get(key)
        if not info:
            # Essayer variante sans décorations (ex: retirer suffixes/prefixes courants)
            candidates = [
                key.replace('-', '_'),
                key.replace('__', '_'),
                key.replace(':', ''),
            ]
            info = next((norm_index.get(c) for c in candidates if c in norm_index), None)
        if not info:
            result['skipped'].append(raw_name)
            continue

        # Nettoyage/garanties
        cols = info.get('columns') or []
        # dédupliquer par nom
        seen = set()
        clean_cols = []
        for c in cols:
            cname = (c.get('name') or '').strip()
            if not cname or cname.lower() in seen:
                continue
            seen.add(cname.lower())
            clean_cols.append({
                "name": cname,
                "type": c.get('type'),
                "description": (c.get('description') or None)
            })

        result['tables'].append({
            "name": info.get('name') or raw_name,
            "description_table": info.get('description_table'),
            "columns": clean_cols
        })

    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

    # Petit récap console
    found = [t['name'] for t in result['tables']]
    print(f"Tables exportées: {len(found)}")
    if found:
        print("- " + "\n- ".join(found))
    if result['skipped']:
        print(f"Tables introuvables (skipped): {len(result['skipped'])}")
        print("- " + "\n- ".join(result['skipped']))


if __name__ == '__main__':
    main()
