"""Schema-generator tests: both SQL dialects stay in sync with schema.TABLES.

``schema.py`` is the single source of truth for the structured tables, and it
generates two checked-in DDL files -- ``sql/velop_schema.sql`` (CrateDB) and
``sql/velop_schema_postgres.sql`` (PostgreSQL). These tests fail if either file
was hand-edited or left stale after a TABLES change.
"""

from pathlib import Path

import pytest

from velop_watcher import schema

REPO = Path(__file__).resolve().parent.parent
CRATE_SQL = REPO / "sql" / "velop_schema.sql"
PG_SQL = REPO / "sql" / "velop_schema_postgres.sql"


def test_every_crate_type_has_a_postgres_equivalent():
    """A new CrateDB type in TABLES must be mapped before the pg DDL can build."""
    used = {ctype for cols in schema.TABLES.values() for _name, ctype in cols}
    used |= {ctype for _name, ctype in schema.PK_PREFIX}
    assert used <= set(schema.PG_TYPES), used - set(schema.PG_TYPES)


@pytest.mark.parametrize(
    "dialect,path", [("crate", CRATE_SQL), ("postgres", PG_SQL)]
)
def test_checked_in_ddl_matches_generator(dialect, path):
    """Regenerate with:  python -m velop_watcher.schema [--dialect postgres]"""
    assert path.read_text() == schema.schema_sql(dialect), f"{path} is stale"


def test_postgres_ddl_has_no_cratedb_only_types():
    """DOUBLE/ARRAY(TEXT)/OBJECT(IGNORED) must not survive into the pg DDL."""
    # statements only -- the header comment names the CrateDB types it maps from
    sql = "\n".join(
        line
        for line in schema.schema_sql("postgres").splitlines()
        if not line.lstrip().startswith("--")
    )
    assert "OBJECT(IGNORED)" not in sql
    assert "ARRAY(TEXT)" not in sql
    # bare DOUBLE (CrateDB) vs DOUBLE PRECISION (PostgreSQL)
    assert " DOUBLE,\n" not in sql and " DOUBLE\n" not in sql
    assert "JSONB" in sql and "TEXT[]" in sql and "DOUBLE PRECISION" in sql


def test_postgres_indexes_name_real_tables_and_columns():
    for table, columns in schema.PG_INDEXES:
        known = {name for name, _t in schema.PK_PREFIX} | set(schema.column_names(table))
        assert set(columns) <= known, (table, columns)


def test_unknown_dialect_is_rejected():
    with pytest.raises(ValueError):
        schema.schema_sql("mysql")
