import pathlib, sys
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))
from sql.utils import coerce_numeric_string_literals


def test_coerce_numeric_string_literals():
    schema = {
        "tables": {
            "ORDO_PROJECT": {
                "columns": {
                    "AUTOMATIC_BROADCAST": {"type": "VARCHAR"},
                    "BUDGET_START": {"type": "DATE"},
                }
            }
        }
    }
    sql = (
        "SELECT * FROM ORDO_PROJECT "
        "WHERE AUTOMATIC_BROADCAST = 1 "
        "AND EXTRACT(YEAR FROM BUDGET_START) = 2014;"
    )
    fixed = coerce_numeric_string_literals(sql, schema)
    assert (
        fixed
        == "SELECT * FROM ORDO_PROJECT WHERE AUTOMATIC_BROADCAST = '1' "
        "AND EXTRACT(YEAR FROM BUDGET_START) = 2014;"
    )
