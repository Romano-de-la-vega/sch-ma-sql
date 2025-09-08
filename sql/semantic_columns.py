"""Utilities for semantic comparison between queries and database columns.

This module builds simple text embeddings for each column in a database
schema. If SQLAlchemy is available the schema inspection utilities are used,
otherwise a minimal fallback relying on the DB-API is employed. The resulting
embeddings are cached in memory so that subsequent similarity searches do not
require rebuilding the vectors.
"""

from __future__ import annotations

from collections import Counter
import math
from typing import List

try:  # Optional dependency
    from sqlalchemy import inspect as sa_inspect  # type: ignore
except Exception:  # pragma: no cover - SQLAlchemy may not be installed
    sa_inspect = None

# Module level caches -------------------------------------------------------

_VECTOR_VOCAB: dict[str, int] = {}
_COLUMN_VECTORS: List[List[float]] = []
_COLUMN_NAMES: List[str] = []


def _tokenize(text: str) -> List[str]:
    """Tokenize a piece of text into a list of lowercase terms.

    Splits on spaces, underscores and dots which are common in SQL identifiers.
    """
    text = text.lower().replace('.', ' ').replace('_', ' ')
    return [tok for tok in text.split() if tok]


def build_column_embeddings(connection) -> List[List[float]]:
    """Build and cache embeddings for all columns in the given database.

    Parameters
    ----------
    connection : object
        Either a SQLAlchemy engine/connection or a raw DB-API connection.

    Returns
    -------
    list
        List of normalised embedding vectors corresponding to the discovered
        columns. Each vector position corresponds to a token from the global
        vocabulary built from all column identifiers.
    """
    global _VECTOR_VOCAB, _COLUMN_VECTORS, _COLUMN_NAMES
    if _COLUMN_VECTORS:
        # Already built – return cached vectors
        return _COLUMN_VECTORS

    columns_text: List[str] = []

    if sa_inspect is not None and hasattr(connection, "dialect"):
        inspector = sa_inspect(connection)
        for table in inspector.get_table_names():
            for col in inspector.get_columns(table):
                col_name = f"{table}.{col['name']}"
                _COLUMN_NAMES.append(col_name)
                columns_text.append(col_name)
    else:  # Fallback using SQLite PRAGMA or information_schema
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            for table in tables:
                cursor.execute(f"PRAGMA table_info('{table}')")
                for col in cursor.fetchall():
                    col_name = f"{table}.{col[1]}"
                    _COLUMN_NAMES.append(col_name)
                    columns_text.append(col_name)
        finally:
            cursor.close()

    # Build vocabulary
    for text in columns_text:
        for token in _tokenize(text):
            if token not in _VECTOR_VOCAB:
                _VECTOR_VOCAB[token] = len(_VECTOR_VOCAB)

    # Create embeddings
    for text in columns_text:
        vec = [0.0] * len(_VECTOR_VOCAB)
        token_counts = Counter(_tokenize(text))
        for token, count in token_counts.items():
            vec[_VECTOR_VOCAB[token]] = float(count)
        # Normalise vector
        norm = math.sqrt(sum(v * v for v in vec))
        if norm:
            vec = [v / norm for v in vec]
        _COLUMN_VECTORS.append(vec)

    return _COLUMN_VECTORS


def find_relevant_columns(query_text: str, top_k: int = 5) -> List[str]:
    """Return the column names most similar to the ``query_text``.

    The function assumes :func:`build_column_embeddings` has already been
    called. It encodes the query text using the same vocabulary and computes
    cosine similarity against cached column embeddings.

    Parameters
    ----------
    query_text : str
        Natural language description of the desired column.
    top_k : int, optional
        Number of most similar columns to return, by default 5.
    """
    if not _COLUMN_VECTORS:
        raise ValueError("Embeddings have not been built. Call build_column_embeddings() first.")

    # Encode the query
    query_vec = [0.0] * len(_VECTOR_VOCAB)
    token_counts = Counter(_tokenize(query_text))
    for token, count in token_counts.items():
        if token in _VECTOR_VOCAB:
            query_vec[_VECTOR_VOCAB[token]] = float(count)
    norm = math.sqrt(sum(v * v for v in query_vec))
    if norm:
        query_vec = [v / norm for v in query_vec]

    # Compute cosine similarity with each column
    similarities = []
    for idx, col_vec in enumerate(_COLUMN_VECTORS):
        sim = sum(a * b for a, b in zip(query_vec, col_vec))
        similarities.append((sim, idx))

    similarities.sort(reverse=True)
    indices = [idx for _, idx in similarities[:top_k]]
    return [_COLUMN_NAMES[i] for i in indices]


def _clear_cache():  # pragma: no cover - helper for tests
    """Remove cached embeddings. Intended for unit tests."""
    global _VECTOR_VOCAB, _COLUMN_VECTORS, _COLUMN_NAMES
    _VECTOR_VOCAB = {}
    _COLUMN_VECTORS = []
    _COLUMN_NAMES = []
