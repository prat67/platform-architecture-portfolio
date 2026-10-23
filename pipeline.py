"""
Reference implementation of the generic per-provider ingestion runner.
Enforces the standardized endpoint module contract:
  - TABLE_NAME: str
  - PARAMS: dict | PARAMS_LIST: list[dict]
  - transform_records(record: dict) -> tuple
  - INSERT_QUERY: str
  - Optional: expand_record(record) / group_records(records)
  - Optional: DEDUPE_UNCHANGED_SNAPSHOTS: bool + COLUMNS: list[str]
"""
from typing import Any, Callable, Protocol


class EndpointModuleContract(Protocol):
    TABLE_NAME: str
    INSERT_QUERY: str
    COLUMNS: list[str]
    DEDUPE_UNCHANGED_SNAPSHOTS: bool

    def transform_records(self, record: dict[str, Any]) -> tuple[Any, ...]: ...


def fetch_all_records(
    api_client: Callable[[dict[str, Any]], list[dict[str, Any]]],
    module: Any,
) -> list[dict[str, Any]]:
    """
    Supports both single PARAMS dict and multi-pass PARAMS_LIST
    (e.g., querying separate ELECTRICAL and MECHANICAL device categories
    and merging into a unified snapshot).
    """
    param_sets: list[dict[str, Any]] = getattr(
        module, "PARAMS_LIST", [getattr(module, "PARAMS", {})]
    )
    aggregated: list[dict[str, Any]] = []
    for params in param_sets:
        batch = api_client(params)
        aggregated.extend(batch)
    return aggregated


def apply_contract_hooks(
    raw_records: list[dict[str, Any]],
    module: Any,
) -> list[tuple[Any, ...]]:
    """Applies optional expand_record / group_records hooks before row transformation."""
    records = raw_records

    if hasattr(module, "expand_record"):
        expanded: list[dict[str, Any]] = []
        for rec in records:
            expanded.extend(module.expand_record(rec))
        records = expanded

    if hasattr(module, "group_records"):
        records = module.group_records(records)

    return [module.transform_records(rec) for rec in records]


def has_snapshot_changed(
    cursor: Any,
    module: Any,
    new_rows: list[tuple[Any, ...]],
) -> bool:
    """
    Compares incoming batch against the most recently stored snapshot as a set.
    Prevents redundant table growth when source APIs return full device inventories
    on every poll without delta filtering.
    """
    if not getattr(module, "DEDUPE_UNCHANGED_SNAPSHOTS", False):
        return True

    cols = ", ".join(module.COLUMNS)
    query = f"""
        SELECT {cols}
        FROM {module.TABLE_NAME}
        WHERE SnapshotTimestamp = (
            SELECT MAX(SnapshotTimestamp) FROM {module.TABLE_NAME}
        )
    """
    cursor.execute(query)
    existing_snapshot = {tuple(row) for row in cursor.fetchall()}
    incoming_snapshot = set(new_rows)

    return incoming_snapshot != existing_snapshot


def execute_module_pipeline(
    db_connection: Any,
    api_client: Callable[[dict[str, Any]], list[dict[str, Any]]],
    module: Any,
) -> int:
    """End-to-end execution: fetch -> expand/group -> transform -> dedupe check -> commit."""
    raw_records = fetch_all_records(api_client, module)
    transformed_rows = apply_contract_hooks(raw_records, module)

    if not transformed_rows:
        return 0

    cursor = db_connection.cursor()
    if not has_snapshot_changed(cursor, module, transformed_rows):
        print(f"[SKIP] {module.TABLE_NAME}: Snapshot unchanged since last poll.")
        return 0

    cursor.executemany(module.INSERT_QUERY, transformed_rows)
    db_connection.commit()
    return len(transformed_rows)