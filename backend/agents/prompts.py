"""LLM prompt templates for all agents."""

PLAIN_ENGLISH_TO_SCHEMA_PROMPT = """You are a database schema expert. The user has described some database tables in plain English (or informal SQL). Convert their description into a precise, structured database schema.

## User Input
{user_input}

## Instructions
1. Identify every table mentioned (explicit or implied).
2. For each table identify all columns. If the user omits an "id" primary key, add one.
3. Infer appropriate SQL data types:
   - id / *_id columns → INT
   - name, title, description, status, type, code → VARCHAR(255)
   - long text / notes / bio → TEXT
   - price / amount / cost / salary → DECIMAL(10,2)
   - quantity / count / age / year → INT
   - is_* / active / enabled → BOOLEAN
   - created_at / updated_at / *_date / *_time → TIMESTAMP
   - email → VARCHAR(255)
   - phone → VARCHAR(20)
4. Mark id / primary-key columns as NOT NULL, is_primary_key=true, is_unique=true.
5. Infer foreign-key relationships: if a column name is "<parent_table>_id" and that parent table exists, add a FK.
6. Determine generation_order: parent tables (no incoming FKs) come first, child tables last.
7. If the user mentions allowed values (e.g. status in active/inactive), add them as a check_constraint string like "status IN ('active','inactive')".

## Output Format (JSON only — no explanation, no markdown fences)
{{
  "tables": [
    {{
      "name": "table_name",
      "columns": [
        {{
          "name": "id",
          "data_type": "INT",
          "nullable": false,
          "is_primary_key": true,
          "default": null,
          "check_constraint": null,
          "is_unique": true
        }}
      ],
      "primary_keys": ["id"],
      "foreign_keys": [
        {{
          "column": "user_id",
          "references_table": "users",
          "references_column": "id"
        }}
      ],
      "check_constraints": [],
      "unique_constraints": []
    }}
  ],
  "generation_order": ["parent_table", "child_table"]
}}
"""

BRAIN_AGENT_ANALYSIS_PROMPT = """You are a data analysis expert. Analyze the following database schema and create a synthetic data generation plan.

## Parsed Schema (JSON)
{schema_json}

## Generation Order (topologically sorted by FK dependencies)
{generation_order}

## Your Tasks

1. **Classify each column** into a generation strategy:
   - `sequential`: Auto-increment integer IDs (primary keys)
   - `faker`: Use Faker library methods (names, emails, phones, addresses, dates, etc.)
   - `reference_data`: Domain-specific codes from curated lists (ICD-10 medical codes, country codes, state codes, etc.)
   - `foreign_key`: Sample from parent table's primary key column (for FK columns)
   - `distribution`: Numeric values following a statistical distribution (amounts, prices, scores)
   - `computed`: Values derived from other columns (total = qty * price)
   - `custom`: Business logic with specific allowed values and optional weights (status fields, type fields with CHECK constraints)

2. **Identify semantic context** for each column:
   - Column names like `icd10_code`, `diag_code` → medical codes (use reference_data)
   - Column names like `npi_number` → healthcare provider NPI (use faker with npi pattern)
   - Column names like `sku` → product SKU (use faker with pattern)
   - Column names like `email` → email addresses (use faker)
   - Column names like `phone` → phone numbers (use faker)
   - Column names like `state` with CHAR(2) → US state codes (use reference_data)
   - Column names like `zip_code` → ZIP codes (use faker)
   - CHECK constraints with IN (...) → use the allowed values as custom strategy

3. **Generate clarifying questions** for any ambiguous columns (optional — only if truly ambiguous).

4. **Write a human-readable summary** of the generation plan.

## Output Format
Respond with a JSON object:
{{
  "column_strategies": [
    {{
      "table": "table_name",
      "column": "column_name",
      "strategy": "faker|reference_data|foreign_key|distribution|sequential|computed|custom",
      "details": {{
        // For faker: {{"method": "name", "params": {{}}}}
        // For reference_data: {{"source": "icd10_codes", "column": "code"}}
        // For foreign_key: {{"parent_table": "...", "parent_column": "..."}}
        // For distribution: {{"type": "normal", "mean": 100, "std": 20, "min": 0, "max": 1000}}
        // For sequential: {{"start": 1}}
        // For computed: {{"formula": "quantity * unit_price"}}
        // For custom: {{"values": ["a", "b", "c"], "weights": [0.5, 0.3, 0.2]}}
      }},
      "semantic_type": "medical_code|email|name|phone|address|date|currency|id|code|text|boolean|status|..."
    }}
  ],
  "clarifying_questions": ["question1", "question2"],
  "summary": "Human-readable summary of what will be generated..."
}}

{user_context}
"""

PYTHON_AGENT_GENERATION_PROMPT = """You are a Python code generation expert. Generate a complete, self-contained Python script that creates synthetic data based on the following specifications.

## Schema
{schema_json}

## Generation Order
{generation_order}

## Column Strategies
{strategies_json}

## Row Counts
{row_counts}

## Requirements
1. The script must be completely self-contained — only use these imports:
   `import pandas as pd, numpy as np, os, sys, json, csv, random, argparse`
   `from faker import Faker`
   `from datetime import datetime, timedelta, date`

2. Accept command line arguments:
   - `--output-dir`: Directory to write CSV files (required)
   - `--row-count`: Override default row count for all tables (optional)

3. Generate tables in this exact order: {generation_order}

4. For FOREIGN KEY columns: Read the parent table's already-generated CSV and sample from its primary key column.

5. For UNIQUE columns: Ensure no duplicate values are generated.

6. For NOT NULL columns: Never generate null/NaN values.

7. For CHECK constraints: Respect the allowed values.

8. Write each table to a CSV file named `<table_name>.csv` in the output directory.

9. At the end, print a JSON status to stdout:
   `{{"status": "success", "tables": {{"table_name": {{"rows": N, "file": "path"}}}}}}`

10. Use `Faker('en_US')` and set seed with `Faker.seed(42)` and `random.seed(42)` and `np.random.seed(42)` for reproducibility.

## Reference Data
The reference data directory is: {reference_data_dir}

**Available reference data files** (use ONLY these exact filenames — do NOT invent or guess names):
{available_reference_files}

Loading rules:
- `.json` files: `json.load(open(path))` — may return a list (e.g. `["AL","AK",...]`) or a dict
- `.csv` files: use `csv.DictReader`; pick the appropriate column

Example — loading state codes from `state_codes.json`:
```python
with open(os.path.join(ref_data_dir, 'state_codes.json'), encoding='utf-8') as f:
    state_codes = json.load(f)  # returns a list like ["AL","AK",...]
```

{error_context}

Generate ONLY the Python script, wrapped in ```python ... ``` code blocks. No explanations needed.
"""

PYTHON_AGENT_SELF_HEAL_PROMPT = """The previously generated script failed with an error. Fix the script.

## Previous Script
```python
{previous_script}
```

## Error
```
{error_message}
```

## Validation Failures (if any)
{validation_failures}

## Reference Data
The reference data directory is: {reference_data_dir}

**Available reference data files** (use ONLY these exact filenames — do NOT invent or guess names):
{available_reference_files}

Fix ONLY the issues described. Do not rewrite unrelated parts. Generate the complete fixed script wrapped in ```python ... ``` code blocks.
"""
