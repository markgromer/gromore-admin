"""
Main orchestrator and CLI for the analytics reporting system.

Usage:
    python run.py --client ace_plumbing --month 2026-03
    python run.py --client ace_plumbing --month 2026-03 --api
    python run.py --all
    python run.py --all --api
    python run.py --client ace_plumbing --month 2026-03 --report-type internal
    python run.py --client ace_plumbing --check-api
"""
import argparse
import json
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from src.parsers import load_client_data
from src.analytics import build_full_analysis
from src.suggestions import generate_suggestions, format_suggestions_for_internal, format_suggestions_for_client
from src.reports import generate_internal_report, generate_client_report
from src.api_pull import pull_all_api_data, merge_api_and_csv_data, check_api_setup
from src import database as db


CONFIG_DIR = Path(__file__).parent / "config"


def load_clients():
    clients_path = CONFIG_DIR / "clients.json"
    with open(clients_path, "r") as f:
        clients = json.load(f)
    # Remove template entry
    clients.pop("_template", None)
    return clients


def get_current_month():
    return datetime.now().strftime("%Y-%m")


def process_client(client_id, month, client_config, report_type="both", use_api=False):
    """
    Full pipeline for one client/month:
    1. Load data (from API, CSV, or both)
    2. Store in database
    3. Run analysis against benchmarks and prior month
    4. Generate suggestions
    5. Build reports
    """
    print(f"\n{'='*60}")
    print(f"Processing: {client_config.get('display_name', client_id)}")
    print(f"Month: {month}")
    print(f"Mode: {'API + CSV fallback' if use_api else 'CSV imports'}")
    print(f"{'='*60}")

    # Step 1: Load data
    print("\n[1/5] Loading data...")

    data = {}
    api_sources = {}

    if use_api:
        print("  Attempting API connections...")
        api_data, api_sources = pull_all_api_data(client_id, month, client_config)
        data.update(api_data)

        # Fill gaps with CSV
        csv_data = load_client_data(client_id, month)
        if csv_data:
            for source, parsed in csv_data.items():
                if source not in data:
                    data[source] = parsed
                    print(f"  CSV fallback: {source}")
    else:
        data = load_client_data(client_id, month)

    if not data:
        print(f"  WARNING: No data found for {client_id}/{month}")
        if not use_api:
            print(f"  Expected location: data/imports/{client_id}/{month}/")
            print(f"  Or try: --api to pull from connected APIs")
        else:
            print(f"  Check API config in clients.json and credential files")
        return False

    sources_found = list(data.keys())
    source_labels = []
    for s in sources_found:
        label = s
        if api_sources.get(s) == "api":
            label += " (API)"
        else:
            label += " (CSV)"
        source_labels.append(label)
    print(f"  Data loaded: {', '.join(source_labels)}")

    # Step 2: Store in database
    print("\n[2/5] Storing data in database...")
    for source, parsed_data in data.items():
        db.store_monthly_data(client_id, month, source, parsed_data)
        print(f"  Stored: {source} ({parsed_data.get('row_count', '?')} rows)")

    # Step 3: Analysis
    print("\n[3/5] Running analysis...")
    analysis = build_full_analysis(client_id, month, data, client_config)
    print(f"  Overall grade: {analysis['overall_grade']}")
    print(f"  Highlights: {len(analysis['highlights'])}")
    print(f"  Concerns: {len(analysis['concerns'])}")

    # Step 4: Generate suggestions
    print("\n[4/5] Generating suggestions...")
    suggestions = generate_suggestions(analysis)
    print(f"  Generated {len(suggestions)} recommendations")

    # Store summary
    db.store_monthly_summary(client_id, month, analysis, suggestions)

    # Step 5: Build reports
    print("\n[5/5] Building reports...")
    if report_type in ("both", "internal"):
        internal_suggestions = format_suggestions_for_internal(suggestions)
        generate_internal_report(analysis, internal_suggestions)

    if report_type in ("both", "client"):
        client_suggestions = format_suggestions_for_client(suggestions)
        generate_client_report(analysis, client_suggestions)

    print(f"\nDone: {client_config.get('display_name', client_id)} - {month}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Home Services Ad Agency - Analytics & Reporting System"
    )
    parser.add_argument("--client", help="Client ID to process (e.g., ace_plumbing)")
    parser.add_argument("--month", help="Month to process (YYYY-MM format, e.g., 2026-03)")
    parser.add_argument("--all", action="store_true", help="Process all clients for the specified or current month")
    parser.add_argument("--api", action="store_true", help="Pull data from APIs instead of CSV imports")
    parser.add_argument("--check-api", action="store_true", help="Check API setup status for a client")
    parser.add_argument(
        "--report-type",
        choices=["both", "internal", "client"],
        default="both",
        help="Which report to generate (default: both)"
    )

    args = parser.parse_args()

    # API setup check mode
    if args.check_api:
        if not args.client:
            print("Error: --check-api requires --client")
            sys.exit(1)
        clients = load_clients()
        if args.client not in clients:
            print(f"Error: Client '{args.client}' not found")
            sys.exit(1)

        status = check_api_setup(clients[args.client])
        print(f"\nAPI Setup Status for: {clients[args.client].get('display_name', args.client)}")
        print(f"{'='*50}")
        for platform, info in status.items():
            configured = "YES" if info["configured"] else "NO"
            creds = "YES" if info.get("credentials_exist") else "NO"
            pkg = "YES" if info.get("package_installed") else "NO"
            print(f"\n  {platform.upper()}:")
            print(f"    Configured in clients.json: {configured}")
            print(f"    Credentials file exists:    {creds}")
            print(f"    Python package installed:   {pkg}")
            if info.get("configured"):
                for k, v in info.items():
                    if k not in ("configured", "credentials_exist", "package_installed"):
                        print(f"    {k}: {v}")
        return

    if not args.client and not args.all:
        parser.print_help()
        print("\nError: Specify --client or --all")
        sys.exit(1)

    month = args.month or get_current_month()
    clients = load_clients()

    if args.all:
        print(f"Processing all clients for {month}...")
        if args.api:
            print("Mode: API + CSV fallback")
        success = 0
        for client_id, client_config in clients.items():
            if process_client(client_id, month, client_config, args.report_type, args.api):
                success += 1
        print(f"\nCompleted: {success}/{len(clients)} clients processed")
    else:
        if args.client not in clients:
            print(f"Error: Client '{args.client}' not found in config/clients.json")
            print(f"Available clients: {', '.join(clients.keys())}")
            sys.exit(1)
        process_client(args.client, month, clients[args.client], args.report_type, args.api)


if __name__ == "__main__":
    main()
