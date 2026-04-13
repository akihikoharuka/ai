"""SQL DDL parser using sqlglot. Extracts table schemas, constraints, and FK relationships."""

from __future__ import annotations

import re
from collections import defaultdict, deque
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool = True
    is_primary_key: bool = False
    default: str | None = None
    check_constraint: str | None = None
    is_unique: bool = False

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "is_primary_key": self.is_primary_key,
            "default": self.default,
            "check_constraint": self.check_constraint,
            "is_unique": self.is_unique,
        }


@dataclass
class ForeignKey:
    column: str
    references_table: str
    references_column: str

    def to_dict(self) -> dict:
        return {
            "column": self.column,
            "references_table": self.references_table,
            "references_column": self.references_column,
        }


@dataclass
class TableSchema:
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)
    check_constraints: list[str] = field(default_factory=list)
    unique_constraints: list[list[str]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "primary_keys": self.primary_keys,
            "foreign_keys": [fk.to_dict() for fk in self.foreign_keys],
            "check_constraints": self.check_constraints,
            "unique_constraints": self.unique_constraints,
        }


def parse_ddl(ddl: str) -> list[TableSchema]:
    """Parse SQL DDL string and return a list of TableSchema objects."""
    tables = []

    # Try parsing with multiple dialects
    parsed = None
    for dialect in [None, "mysql", "postgres", "tsql"]:
        try:
            parsed = sqlglot.parse(ddl, dialect=dialect)
            if parsed and any(isinstance(stmt, exp.Create) for stmt in parsed if stmt):
                break
        except Exception:
            continue

    if not parsed:
        raise ValueError("Failed to parse SQL DDL. Please check the SQL syntax.")

    for statement in parsed:
        if not isinstance(statement, exp.Create):
            continue

        schema = statement.find(exp.Schema)
        if not schema:
            continue

        table_name = _extract_table_name(statement)
        table = TableSchema(name=table_name)

        # Extract columns
        for col_def in schema.find_all(exp.ColumnDef):
            column = _parse_column(col_def)
            table.columns.append(column)

        # Extract table-level constraints
        _extract_table_constraints(schema, table, ddl)

        # Mark PK columns
        for col in table.columns:
            if col.name in table.primary_keys:
                col.is_primary_key = True

        tables.append(table)

    return tables


def _extract_table_name(create_stmt: exp.Create) -> str:
    """Extract table name from CREATE statement."""
    table = create_stmt.find(exp.Table)
    if table:
        return table.name
    return "unknown"


def _parse_column(col_def: exp.ColumnDef) -> ColumnInfo:
    """Parse a column definition into ColumnInfo."""
    name = col_def.name
    data_type = _extract_data_type(col_def)
    nullable = True
    is_pk = False
    default = None
    check_constraint = None
    is_unique = False

    for constraint in col_def.find_all(exp.ColumnConstraint):
        kind = constraint.find(exp.ColumnConstraintKind)
        if kind is None:
            # Check child nodes directly
            for child in constraint.args.get("kind", constraint).walk():
                if isinstance(child, exp.PrimaryKeyColumnConstraint):
                    is_pk = True
                elif isinstance(child, exp.NotNullColumnConstraint):
                    nullable = False
                elif isinstance(child, exp.UniqueColumnConstraint):
                    is_unique = True
            continue

        if isinstance(kind, exp.PrimaryKeyColumnConstraint):
            is_pk = True
            nullable = False
        elif isinstance(kind, exp.NotNullColumnConstraint):
            nullable = False
        elif isinstance(kind, exp.UniqueColumnConstraint):
            is_unique = True
        elif isinstance(kind, exp.DefaultColumnConstraint):
            default_expr = kind.find(exp.Literal)
            if default_expr:
                default = default_expr.this
            else:
                default = kind.sql()
                # Clean up "DEFAULT " prefix
                default = re.sub(r"^DEFAULT\s+", "", default, flags=re.IGNORECASE)
        elif isinstance(kind, exp.CheckColumnConstraint):
            check_constraint = kind.sql()

    return ColumnInfo(
        name=name,
        data_type=data_type,
        nullable=nullable,
        is_primary_key=is_pk,
        default=default,
        check_constraint=check_constraint,
        is_unique=is_unique,
    )


def _extract_data_type(col_def: exp.ColumnDef) -> str:
    """Extract and normalize the data type from a column definition."""
    dtype = col_def.find(exp.DataType)
    if dtype:
        return dtype.sql().upper()
    return "VARCHAR"


def _extract_table_constraints(schema: exp.Schema, table: TableSchema, raw_ddl: str) -> None:
    """Extract table-level PRIMARY KEY, FOREIGN KEY, UNIQUE, and CHECK constraints."""
    for constraint in schema.find_all(exp.PrimaryKey):
        pk_cols = [col.name for col in constraint.find_all(exp.Column)]
        table.primary_keys.extend(pk_cols)

    for constraint in schema.find_all(exp.ForeignKey):
        fk_cols = []
        ref_table = None
        ref_cols = []

        # Get the FK columns (sqlglot stores them as Identifier nodes)
        expressions = constraint.args.get("expressions", [])
        for expr in expressions:
            if isinstance(expr, (exp.Column, exp.Identifier)):
                fk_cols.append(expr.name)

        # Get the reference
        reference = constraint.find(exp.Reference)
        if reference:
            ref_table_node = reference.find(exp.Table)
            if ref_table_node:
                ref_table = ref_table_node.name
            # Reference columns are inside a Schema node as Identifiers
            ref_schema = reference.find(exp.Schema)
            if ref_schema:
                ref_cols = [
                    ident.name for ident in ref_schema.find_all(exp.Identifier)
                    if ident.name != ref_table  # Exclude the table name identifier
                ]

        for i, fk_col in enumerate(fk_cols):
            ref_col = ref_cols[i] if i < len(ref_cols) else ref_cols[0] if ref_cols else fk_col
            if ref_table:
                table.foreign_keys.append(ForeignKey(
                    column=fk_col,
                    references_table=ref_table,
                    references_column=ref_col,
                ))

    for constraint in schema.find_all(exp.UniqueColumnConstraint):
        unique_cols = [col.name for col in constraint.find_all(exp.Column)]
        if unique_cols:
            table.unique_constraints.append(unique_cols)

    # Extract inline PK from column constraints (if not already found)
    if not table.primary_keys:
        for col in table.columns:
            if col.is_primary_key:
                table.primary_keys.append(col.name)

    # Fallback: extract CHECK constraints from raw DDL for this table
    _extract_check_constraints_from_raw(table, raw_ddl)


def _extract_check_constraints_from_raw(table: TableSchema, raw_ddl: str) -> None:
    """Extract CHECK constraints using regex as fallback from raw DDL."""
    # Find the CREATE TABLE block for this table
    pattern = rf"CREATE\s+TABLE\s+(?:\w+\.)?{re.escape(table.name)}\s*\((.*?)\)\s*;"
    match = re.search(pattern, raw_ddl, re.IGNORECASE | re.DOTALL)
    if not match:
        return

    body = match.group(1)

    # Find all CHECK(...) expressions
    check_pattern = r"CHECK\s*\((.*?)\)(?:\s*,|\s*$)"
    for check_match in re.finditer(check_pattern, body, re.IGNORECASE):
        constraint = check_match.group(1).strip()
        if constraint not in table.check_constraints:
            table.check_constraints.append(constraint)


def topological_sort(tables: list[TableSchema]) -> list[str]:
    """Topologically sort tables by FK dependencies using Kahn's algorithm.

    Returns table names in generation order: parent/dimension tables first,
    child/fact tables last.
    """
    table_names = {t.name for t in tables}
    table_map = {t.name: t for t in tables}

    # Build adjacency list and in-degree count
    # Edge: parent -> child (parent must be generated first)
    in_degree: dict[str, int] = {name: 0 for name in table_names}
    adjacency: dict[str, list[str]] = defaultdict(list)

    for table in tables:
        for fk in table.foreign_keys:
            parent = fk.references_table
            child = table.name
            # Skip self-references
            if parent == child:
                continue
            # Only count if parent table exists in our schema
            if parent in table_names:
                adjacency[parent].append(child)
                in_degree[child] += 1

    # Kahn's algorithm
    queue = deque([name for name, deg in in_degree.items() if deg == 0])
    result = []

    while queue:
        node = queue.popleft()
        result.append(node)
        for neighbor in adjacency[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # If result doesn't contain all tables, there's a cycle — add remaining
    remaining = [name for name in table_names if name not in result]
    result.extend(remaining)

    return result


def parse_and_sort(ddl: str) -> tuple[list[TableSchema], list[str]]:
    """Parse DDL and return tables with their generation order."""
    tables = parse_ddl(ddl)
    order = topological_sort(tables)
    return tables, order
