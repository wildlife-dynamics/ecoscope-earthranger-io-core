"""QA script to test ERWarehouseClient against a live DWH.

Requires:
  - A valid EarthRanger API token
  - Google ADC configured (``gcloud auth application-default login``)

Usage examples:

    # Subject group observations
    python tests/test_dwh_client.py --server root.dev.pamdas.org --token TOKEN \
        --since 2026-01-01T00:00:00Z --until 2026-02-01T00:00:00Z \
        subject-observations --subject-group "Elephants"

    # Patrol observations (filtered by type/status)
    python tests/test_dwh_client.py --server root.dev.pamdas.org --token TOKEN \
        --since 2026-01-01T00:00:00Z --until 2026-02-01T00:00:00Z \
        patrol-observations --patrol-type routine_patrol --status done

    # Two-step patrol observations (get_patrols_minimal -> get_patrol_observations)
    python tests/test_dwh_client.py --server root.dev.pamdas.org --token TOKEN \
        --since 2026-01-01T00:00:00Z --until 2026-02-01T00:00:00Z \
        patrol-obs-by-id --patrol-type routine_patrol --status done

    # Same, but narrowed to specific patrol IDs
    python tests/test_dwh_client.py --server root.dev.pamdas.org --token TOKEN \
        --since 2026-01-01T00:00:00Z --until 2026-02-01T00:00:00Z \
        patrol-obs-by-id --patrol-ids "abc-123,def-456"
"""

from __future__ import annotations

import argparse
import sys

import pyarrow as pa


def _print_table(table: pa.Table, label: str = "Result") -> None:
    """Print a summary of a PyArrow table."""
    print(f"\n--- {label} ---")
    print(f"Rows returned:  {len(table)}")
    print(f"Columns:        {table.column_names}")
    print(f"Schema:\n{table.schema}")
    print()
    if len(table) > 0:
        print("First 5 rows:")
        print(table.slice(0, min(5, len(table))).to_pandas().to_string())
    else:
        print("(no rows returned)")
    print()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="QA smoke-test for ERWarehouseClient against a live server.",
    )

    parser.add_argument(
        "--server",
        required=True,
        help="EarthRanger tenant domain (e.g. root.dev.pamdas.org)",
    )
    parser.add_argument(
        "--token",
        required=True,
        help="EarthRanger API token",
    )
    parser.add_argument(
        "--since",
        default=None,
        help="Start of time range (ISO 8601, e.g. 2026-01-01T00:00:00Z)",
    )
    parser.add_argument(
        "--until",
        default=None,
        help="End of time range (ISO 8601, e.g. 2026-02-01T00:00:00Z)",
    )
    parser.add_argument(
        "--warehouse-url",
        default=None,
        help="Override warehouse base URL (skips status endpoint resolution)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- subject-observations ---
    sub_obs = subparsers.add_parser(
        "subject-observations",
        help="Fetch observations for a subject group",
    )
    sub_obs.add_argument(
        "--subject-group",
        required=True,
        help="Subject group name",
    )

    # --- patrol-observations ---
    sub_patrol = subparsers.add_parser(
        "patrol-observations",
        help="Fetch patrol observations filtered by type/status",
    )
    sub_patrol.add_argument(
        "--patrol-type",
        action="append",
        default=None,
        help="Patrol type value (repeatable, e.g. --patrol-type routine_patrol)",
    )
    sub_patrol.add_argument(
        "--status",
        action="append",
        default=None,
        help="Patrol status (repeatable, e.g. --status done)",
    )

    # --- patrol-obs-by-id ---
    sub_by_id = subparsers.add_parser(
        "patrol-obs-by-id",
        help="Two-step: get_patrols_minimal -> get_patrol_observations",
    )
    sub_by_id.add_argument(
        "--patrol-type",
        action="append",
        default=None,
        help="Patrol type value (repeatable)",
    )
    sub_by_id.add_argument(
        "--status",
        action="append",
        default=None,
        help="Patrol status (repeatable)",
    )
    sub_by_id.add_argument(
        "--patrol-ids",
        default=None,
        help="Comma-separated patrol UUIDs to filter the patrols table",
    )

    return parser


def _make_client(args: argparse.Namespace):
    from ecoscope_earthranger_io_core.client import ERWarehouseClient

    print(f"Server:         {args.server}")
    print(f"Since:          {args.since or '(not set)'}")
    print(f"Until:          {args.until or '(not set)'}")
    print(f"Warehouse URL:  {args.warehouse_url or '(auto-resolve from status)'}")
    print()

    return ERWarehouseClient(
        server=args.server,
        token=args.token,
        warehouse_base_url=args.warehouse_url,
    )


def _cmd_subject_observations(client, args: argparse.Namespace) -> None:
    print(f"Subject group:  {args.subject_group}")
    print("Fetching subject group observations...")

    table = client.get_subjectgroup_observations(
        subject_group_name=args.subject_group,
        since=args.since,
        until=args.until,
    )
    _print_table(table, label="Subject Group Observations")


def _cmd_patrol_observations(client, args: argparse.Namespace) -> None:
    print(f"Patrol type:    {args.patrol_type}")
    print(f"Status:         {args.status}")
    print("Fetching patrol observations with patrol filter...")

    table = client.get_patrol_observations_with_patrol_filter(
        since=args.since,
        until=args.until,
        patrol_type_value=args.patrol_type,
        status=args.status,
    )
    _print_table(table, label="Patrol Observations (filtered)")


def _cmd_patrol_obs_by_id(client, args: argparse.Namespace) -> None:
    import pyarrow.compute as pc

    print(f"Patrol type:    {args.patrol_type}")
    print(f"Status:         {args.status}")
    print(f"Patrol IDs:     {args.patrol_ids or '(all)'}")
    print()

    if args.patrol_ids and not args.since:
        filter_ids = [s.strip() for s in args.patrol_ids.split(",")]
        print(f"Building patrols table from {len(filter_ids)} provided ID(s)...")
        patrols_table = pa.table({"id": filter_ids})
    else:
        print("Step 1: Fetching patrols (minimal)...")
        patrols_table = client.get_patrols_minimal(
            since=args.since,
            until=args.until,
            patrol_type_value=args.patrol_type,
            status=args.status,
        )

        patrol_ids_found = patrols_table.column("id").to_pylist()
        print(f"Patrols found:  {len(patrol_ids_found)}")
        for pid in patrol_ids_found[:10]:
            print(f"  - {pid}")
        if len(patrol_ids_found) > 10:
            print(f"  ... and {len(patrol_ids_found) - 10} more")
        print()

        if args.patrol_ids:
            filter_ids = [s.strip() for s in args.patrol_ids.split(",")]
            print(f"Filtering to {len(filter_ids)} patrol ID(s)...")
            mask = pc.is_in(
                patrols_table.column("id"),
                value_set=pa.array(filter_ids),
            )
            patrols_table = patrols_table.filter(mask)
            print(f"Patrols after filter: {len(patrols_table)}")
            if len(patrols_table) == 0:
                print("No patrols matched the provided IDs. Exiting.")
                return
            print()

    _print_table(patrols_table, label="Patrols (minimal)")

    print("Step 2: Fetching observations for patrols...")
    obs_table = client.get_patrol_observations(patrols_df=patrols_table)
    _print_table(obs_table, label="Patrol Observations (by ID)")


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    client = _make_client(args)

    commands = {
        "subject-observations": _cmd_subject_observations,
        "patrol-observations": _cmd_patrol_observations,
        "patrol-obs-by-id": _cmd_patrol_obs_by_id,
    }
    commands[args.command](client, args)


if __name__ == "__main__":
    sys.exit(main() or 0)
