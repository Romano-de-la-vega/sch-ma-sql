import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from guard import whitelist_ok


def test_whitelist_allows_valid_table_with_extract():
    sql = (
        "SELECT COUNT(*) FROM ORDO_PROJECT "
        "WHERE AGL_AA_S_CUR_RELEASE IS NULL AND EXTRACT(YEAR FROM BUDGET_START) = 2013;"
    )
    assert whitelist_ok(sql, ["ORDO_PROJECT"]) is True


def test_whitelist_blocks_unknown_tables():
    sql = "SELECT * FROM UNKNOWN_TABLE"
    assert whitelist_ok(sql, ["ORDO_PROJECT"]) is False
