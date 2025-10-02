#!/usr/bin/env python3
"""Generate CSV dataset for AI training from stg.youthpolicy_landing.

This script connects to the PostgreSQL database, fetches the latest raw_json
per policy from the staging landing table, renames JSON keys to their Korean
descriptions (based on tools/fileds.csv), replaces coded values with their
Korean labels (based on tools/values.csv), and writes the transformed records
to a CSV file.

Usage example:

    python tools/generate_data_for_ai.py --output tools/youthpolicy_ai.csv

Pass ``-`` to ``--output`` to stream CSV data to stdout.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None  # type: ignore

if load_dotenv is not None:
    load_dotenv()


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_FIELDS_PATH = BASE_DIR / "fileds.csv"
DEFAULT_VALUES_PATH = BASE_DIR / "values.csv"


class FieldNameResolver:
    """Resolve original JSON keys into human-readable column names."""

    def __init__(self) -> None:
        self._exact: Dict[str, str] = {}
        self._canonical: Dict[str, str] = {}

    def add(self, key: str, alias: str) -> None:
        self._exact[key] = alias
        canonical = key.lower()
        if canonical not in self._canonical:
            self._canonical[canonical] = alias

    def translate(self, key: str) -> str:
        if key in self._exact:
            return self._exact[key]
        canonical = key.lower()
        if canonical in self._canonical:
            return self._canonical[canonical]
        return key


class MissingCodeTracker:
    """Track unmapped code values to help maintain values.csv."""

    def __init__(self, per_field_limit: int = 5) -> None:
        self._data: Dict[str, set[str]] = defaultdict(set)
        self._limit = per_field_limit

    def add(self, field: str, code: str) -> None:
        code = str(code).strip()
        if not code:
            return
        bucket = self._data[field]
        if len(bucket) < self._limit:
            bucket.add(code)

    def has_missing(self) -> bool:
        return bool(self._data)

    def items(self) -> Iterable[tuple[str, Sequence[str]]]:
        for field, codes in self._data.items():
            yield field, sorted(codes)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate youth policy CSV for AI training")
    parser.add_argument(
        "--output",
        "-o",
        default=str(BASE_DIR / "youthpolicy_ai_dataset.csv"),
        help="Output CSV path (use '-' for stdout).",
    )
    parser.add_argument(
        "--fields",
        default=str(DEFAULT_FIELDS_PATH),
        help="CSV mapping file for field names (default: tools/fileds.csv)",
    )
    parser.add_argument(
        "--values",
        default=str(DEFAULT_VALUES_PATH),
        help="CSV mapping file for code values (default: tools/values.csv)",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN (overrides PG_DSN/DATABASE_URL environment variables)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of policies (for sampling/debugging)",
    )
    parser.add_argument(
        "--policy-id",
        dest="policy_ids",
        action="append",
        help="Filter by specific policy_id (repeatable)",
    )
    return parser.parse_args()


def load_field_name_mapping(path: Path) -> FieldNameResolver:
    resolver = FieldNameResolver()
    name_counts: Dict[str, int] = {}

    if not path.exists():
        raise FileNotFoundError(f"Field mapping CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            raw_field = (row.get("항목") or "").strip()
            description = (row.get("설명") or "").strip()
            if not raw_field or not description:
                continue

            field_key = raw_field.replace("<", "").replace(">", "").strip()
            if not field_key:
                continue

            base_alias = description
            alias_index = name_counts.get(base_alias, 0)
            if alias_index:
                alias = f"{base_alias}_{alias_index + 1}"
            else:
                alias = base_alias
            name_counts[base_alias] = alias_index + 1

            resolver.add(field_key, alias)

    return resolver


def load_value_mappings(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Value mapping CSV not found: {path}")

    mappings: Dict[str, Dict[str, str]] = {}

    with path.open("r", encoding="utf-8-sig", newline="") as fp:
        reader = csv.DictReader(fp)
        for row in reader:
            field = (row.get("분류(영문)") or "").strip()
            code = (row.get("코드") or "").strip()
            label = (row.get("코드내용") or "").strip()
            if not field or not code or not label:
                continue

            canonical = field.lower()
            bucket = mappings.setdefault(canonical, {})
            bucket[code] = label

    return mappings


def load_region_lookup(conn: psycopg.Connection) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select zip_code, full_name
              from master.region
             where zip_code is not null
               and full_name is not null
            """
        )
        for row in cur:
            zip_code = str(row["zip_code"]).strip()
            full_name = str(row["full_name"]).strip()
            if not zip_code or not full_name:
                continue
            lookup.setdefault(zip_code, full_name)
    return lookup


def fetch_latest_policies(
    conn: psycopg.Connection,
    *,
    policy_ids: Sequence[str] | None = None,
    limit: int | None = None,
) -> Iterator[tuple[str, Dict[str, Any]]]:
    sql_parts: List[str] = [
        "select distinct on (policy_id) policy_id, raw_json",
        "from stg.youthpolicy_landing",
    ]

    params: List[Any] = []
    if policy_ids:
        sql_parts.append("where policy_id = any(%s)")
        params.append(list(policy_ids))

    sql_parts.append("order by policy_id, ingested_at desc")
    if limit is not None and limit > 0:
        sql_parts.append("limit %s")
        params.append(limit)

    sql = " ".join(sql_parts)

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params or None)
        while True:
            batch = cur.fetchmany(1000)
            if not batch:
                break
            for row in batch:
                policy_id = str(row["policy_id"])
                payload = row["raw_json"]
                if not isinstance(payload, dict):
                    payload = {} if payload is None else json.loads(json.dumps(payload))
                yield policy_id, payload


def translate_code_value(
    field: str,
    value: Any,
    value_mappings: Dict[str, Dict[str, str]],
) -> tuple[Any, List[str]]:
    table = value_mappings.get(field)
    if table is None:
        table = value_mappings.get(field.lower())
    if table is None:
        return value, []

    if value is None:
        return None, []

    missing: List[str] = []

    if isinstance(value, (list, tuple)):
        translated_list: List[Any] = []
        for item in value:
            code = str(item).strip()
            if not code:
                continue
            label = table.get(code)
            if label is None:
                missing.append(code)
                translated_list.append(item)
            else:
                translated_list.append(label)
        return translated_list, missing

    raw_text = str(value).strip()
    if raw_text == "":
        return value, []

    direct_label = table.get(raw_text)
    if direct_label is not None:
        return direct_label, []

    separators = [",", "|", "/", ";"]
    for sep in separators:
        if sep in raw_text:
            parts = [p.strip() for p in raw_text.split(sep)]
            translated_parts: List[str] = []
            changed = False
            for part in parts:
                if not part:
                    continue
                label = table.get(part)
                if label is None:
                    missing.append(part)
                    translated_parts.append(part)
                else:
                    translated_parts.append(label)
                    changed = True
            if changed:
                joiner = ", " if sep == "," else sep
                return joiner.join(translated_parts), missing

    missing.append(raw_text)
    return value, missing


def translate_region_value(
    value: Any,
    region_lookup: Mapping[str, str],
) -> tuple[Any, List[str]]:
    if value is None:
        return None, []

    missing: List[str] = []

    def resolve(code: Any) -> Any:
        text = str(code).strip()
        if not text:
            return None
        label = region_lookup.get(text)
        if label is None:
            missing.append(text)
            return text
        return label

    if isinstance(value, (list, tuple, set)):
        translated_list: List[Any] = []
        for item in value:
            mapped = resolve(item)
            if mapped is not None:
                translated_list.append(mapped)
        return translated_list, missing

    raw_text = str(value).strip()
    if not raw_text:
        return value, []

    direct = region_lookup.get(raw_text)
    if direct is not None:
        return direct, []

    separators = [",", "|", "/", ";"]
    for sep in separators:
        if sep in raw_text:
            parts = [p.strip() for p in raw_text.split(sep) if p.strip()]
            translated_parts: List[str] = []
            changed = False
            for part in parts:
                label = region_lookup.get(part)
                if label is None:
                    missing.append(part)
                    translated_parts.append(part)
                else:
                    translated_parts.append(label)
                    changed = True
            if changed:
                joiner = ", " if sep == "," else sep
                return joiner.join(translated_parts), missing

    missing.append(raw_text)
    return value, missing


def normalize_csv_value(value: Any) -> str:
    if value is None:
        result = ""
    elif isinstance(value, (list, tuple, set)):
        result = ", ".join(str(item) for item in value if item is not None)
    elif isinstance(value, dict):
        result = json.dumps(value, ensure_ascii=False, sort_keys=True)
    else:
        result = str(value)

    if not result:
        return ""

    return result.replace("\r\n", "\\n").replace("\r", "\\n").replace("\n", "\\n")


def transform_policy(
    policy_id: str,
    payload: Dict[str, Any],
    field_names: FieldNameResolver,
    value_mappings: Dict[str, Dict[str, str]],
    region_lookup: Mapping[str, str],
    missing_tracker: MissingCodeTracker,
) -> Dict[str, Any]:
    record: Dict[str, Any] = {"정책ID": policy_id}

    for key, value in payload.items():
        translated_key = field_names.translate(key)
        lower_key = key.lower()

        if lower_key == "zipcd":
            translated_value, missing_codes = translate_region_value(value, region_lookup)
            for code in missing_codes:
                missing_tracker.add(key, code)
        else:
            translated_value, missing_codes = translate_code_value(key, value, value_mappings)
            for code in missing_codes:
                missing_tracker.add(key, code)
        record[translated_key] = translated_value

    return record


def write_csv(
    records: Sequence[Dict[str, Any]],
    headers: Sequence[str],
    output_path: str,
) -> None:
    output_handle = None
    close_handle = False

    if output_path == "-":
        output_handle = sys.stdout
    else:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        output_handle = path.open("w", newline="", encoding="utf-8")
        close_handle = True

    try:
        writer = csv.DictWriter(output_handle, fieldnames=list(headers), extrasaction="ignore")
        writer.writeheader()
        for record in records:
            row = {header: normalize_csv_value(record.get(header)) for header in headers}
            writer.writerow(row)
    finally:
        if close_handle:
            output_handle.close()


def main() -> None:
    args = parse_args()

    dsn = args.dsn or os.getenv("PG_DSN") or os.getenv("DATABASE_URL")
    if not dsn:
        print("Missing database DSN. Set PG_DSN/DATABASE_URL or use --dsn.", file=sys.stderr)
        sys.exit(1)

    field_mapping = load_field_name_mapping(Path(args.fields))
    value_mappings = load_value_mappings(Path(args.values))
    missing_codes = MissingCodeTracker()

    records: List[Dict[str, Any]] = []
    headers: List[str] = ["정책ID"]
    header_set = set(headers)

    with psycopg.connect(dsn) as conn:
        region_lookup = load_region_lookup(conn)
        for policy_id, payload in fetch_latest_policies(
            conn,
            policy_ids=args.policy_ids,
            limit=args.limit,
        ):
            record = transform_policy(
                policy_id,
                payload,
                field_mapping,
                value_mappings,
                region_lookup,
                missing_codes,
            )
            for key in record.keys():
                if key not in header_set:
                    header_set.add(key)
                    headers.append(key)
            records.append(record)

    write_csv(records, headers, args.output)

    message = f"Generated {len(records)} policy rows with {len(headers)} columns -> {args.output}"
    print(message, file=sys.stderr)

    if missing_codes.has_missing():
        print("\n⚠️ Missing code translations detected:", file=sys.stderr)
        for field, codes in missing_codes.items():
            snippet = ", ".join(codes)
            print(f"  - {field}: {snippet}", file=sys.stderr)


if __name__ == "__main__":
    main()
