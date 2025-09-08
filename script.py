import os, psycopg2, yaml
from collections import defaultdict

DB_DSN = os.getenv("PG_DSN", "host=localhost dbname=planisware user=planisware password=planisware")
SCHEMAS = ("public",)
TOP_TABLES = 100
TOP_COLS_PER_TABLE = 15

conn = psycopg2.connect(DB_DSN)
try:
    with conn.cursor() as cur:
        # 1) Essai par "usage" via pg_stat_user_tables (scans)
        try:
            cur.execute("""
              SELECT n.nspname AS schema, c.relname AS table, (s.seq_scan + s.idx_scan) AS scans
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              JOIN pg_stat_user_tables s ON s.relid = c.oid
              WHERE n.nspname = ANY(%s) AND c.relkind = 'r'
              ORDER BY (s.seq_scan + s.idx_scan) DESC
              LIMIT %s
            """, (list(SCHEMAS), TOP_TABLES))
            top_tables = [r[1] for r in cur.fetchall()]
        except Exception:
            # 2) Fallback par "taille" approx via reltuples
            cur.execute("""
              SELECT c.relname AS table
              FROM pg_class c
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE c.relkind = 'r' AND n.nspname = ANY(%s)
              ORDER BY c.reltuples DESC
              LIMIT %s
            """, (list(SCHEMAS), TOP_TABLES))
            top_tables = [r[0] for r in cur.fetchall()]

        # Colonnes & métadonnées
        cur.execute("""
          SELECT table_schema, table_name, column_name, data_type, is_nullable
          FROM information_schema.columns
          WHERE table_schema = ANY(%s)
          ORDER BY table_schema, table_name, ordinal_position
        """, (list(SCHEMAS),))
        cols = cur.fetchall()

        # Stats (null_frac, n_distinct) — tolère l’absence de droits
        try:
            cur.execute("""
              SELECT schemaname, tablename, attname, null_frac, n_distinct
              FROM pg_stats
              WHERE schemaname = ANY(%s)
            """, (list(SCHEMAS),))
            stat_rows = cur.fetchall()
            stats = {(s,t,a): (nf, nd) for s,t,a,nf,nd in stat_rows}
        except Exception:
            stats = {}

        # PK
        cur.execute("""
        SELECT tc.table_schema, tc.table_name, kc.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kc
          ON kc.constraint_name = tc.constraint_name
        WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema = ANY(%s)
        """, (list(SCHEMAS),))
        pk_map = defaultdict(list)
        for s,t,c in cur.fetchall():
            pk_map[(s,t)].append(c)

        # FK
        cur.execute("""
        SELECT tc.table_schema, tc.table_name, kcu.column_name,
               ccu.table_schema, ccu.table_name, ccu.column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type='FOREIGN KEY' AND tc.table_schema = ANY(%s)
        """, (list(SCHEMAS),))
        fk_map = defaultdict(list)
        for s,t,col,fs,ft,fc in cur.fetchall():
            fk_map[(s,t)].append({"from": col, "to": f"{ft}.{fc}"})

        # Commentaires
        cur.execute("""
        SELECT n.nspname, c.relname, a.attname,
               obj_description(c.oid) AS table_comment,
               col_description(c.oid, a.attnum) AS column_comment
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0
        WHERE c.relkind='r' AND n.nspname = ANY(%s)
        """, (list(SCHEMAS),))
        comments = defaultdict(lambda: {"table": None, "cols": {}})
        for s,t,col,tcom,ccom in cur.fetchall():
            if tcom and not comments[(s,t)]["table"]:
                comments[(s,t)]["table"] = tcom
            if col and ccom:
                comments[(s,t)]["cols"][col] = ccom

        # Construction dictionnaire
        dictionary = {"version": 1, "dialect": "postgres", "tables": {}}
        by_table = defaultdict(list)
        for s,t,c,typ,nullable in cols:
            by_table[(s,t)].append((c,typ,nullable))

        def prio_score(schema, table, col):
            nf, nd = stats.get((schema, table, col), (None, None))
            score = 0
            if nf is not None:
                score += (1 - float(nf))      # moins de nulls = mieux
            if nd is not None and float(nd) > 0:
                score += min(1.0, float(nd))  # n_distinct relatif (approx)
            return score

        for (s,t), columns in by_table.items():
            if t not in top_tables:
                continue
            ranked = sorted(columns, key=lambda x: prio_score(s,t,x[0]), reverse=True)
            kept = ranked[:TOP_COLS_PER_TABLE]

            entry = {"aliases": [], "columns": {}}
            tcom = comments[(s,t)]["table"]
            if tcom:
                entry["desc"] = tcom

            for c,typ,nullable in kept:
                nf, nd = stats.get((s,t,c), (None, None))
                col = {"type": typ, "nullable": (nullable == "YES")}
                if nf is not None:
                    col["null_frac"] = round(float(nf), 4)
                if nd is not None:
                    col["n_distinct~"] = float(nd)
                ccom = comments[(s,t)]["cols"].get(c)
                if ccom:
                    col["desc"] = ccom
                entry["columns"][c] = col

            if pk_map[(s,t)]:
                entry["pk"] = pk_map[(s,t)]
            if fk_map[(s,t)]:
                entry["fks"] = fk_map[(s,t)]

            dictionary["tables"][t] = entry

        with open("schema.yaml", "w", encoding="utf-8") as f:
            yaml.safe_dump(dictionary, f, allow_unicode=True, sort_keys=False)

        print(f"Écrit schema.yaml avec {len(dictionary['tables'])} tables.")
finally:
    conn.close()
