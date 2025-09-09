import os
import sys
import sqlite3

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sql import semantic_columns


def setup_demo_schema():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
    cur.execute(
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)"
    )
    conn.commit()
    cur.close()
    return conn


def test_find_relevant_column_returns_expected_match():
    conn = setup_demo_schema()
    semantic_columns._clear_cache()
    semantic_columns.build_column_embeddings(conn)
    result = semantic_columns.find_relevant_columns("email of user")
    assert result[0] == "users.email"
    conn.close()


def test_build_column_embeddings_is_cached():
    conn = setup_demo_schema()
    semantic_columns._clear_cache()
    emb1 = semantic_columns.build_column_embeddings(conn)
    emb2 = semantic_columns.build_column_embeddings(conn)
    assert emb1 is emb2
    conn.close()
