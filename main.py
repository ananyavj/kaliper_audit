#main.py
from core.plan_loader import load_tracking_plan
from core.plan_normalizer import normalize_specs
from core.plan_analyzer import analyze_tracking_plan
from core.detectors import detect_issues
from core.state_store import StreamState

from simulators.simulators import generate_clean_flow, generate_flow_with_errors
from simulators.simulators_saas import generate_saas_clean_flow, generate_saas_flow_with_errors
from simulators.simulators_content import generate_content_clean_flow, generate_content_flow_with_errors


PLAN_FILES = {
    "ecommerce": "sample_data/tracking_plan_ecommerce.json",
    "saas": "sample_data/tracking_plan_saas.json",
    "content": "sample_data/tracking_plan_content.json",
}

SIMULATORS = {
    "ecommerce": (generate_clean_flow, generate_flow_with_errors),
    "saas": (generate_saas_clean_flow, generate_saas_flow_with_errors),
    "content": (generate_content_clean_flow, generate_content_flow_with_errors),
}


def print_issues(title: str, issues):
    print(f"\n{title}")
    if not issues:
        print("No issues found.")
        return

    for issue in issues:
        print(f"- [{issue.severity}] {issue.issue_type}: {issue.message}")


def choose_mode():
    print("\nChoose a simulator:")
    print("1. Ecommerce")
    print("2. SaaS")
    print("3. Content")
    choice = input("Enter 1, 2, or 3: ").strip()

    if choice == "1":
        return "ecommerce"
    if choice == "2":
        return "saas"
    if choice == "3":
        return "content"

    print("Invalid choice. Defaulting to ecommerce.")
    return "ecommerce"


def main():
    mode = choose_mode()

    plan_path = PLAN_FILES[mode]
    clean_flow_fn, error_flow_fn = SIMULATORS[mode]

    raw_specs = load_tracking_plan(plan_path)
    specs = normalize_specs(raw_specs)
    profile = analyze_tracking_plan(specs)

    print(f"\nLoaded {len(specs)} tracking plan events")
    print(f"Detected domain from tracking plan: {profile.domain}")
    print(f"Confidence: {profile.confidence:.2f}")
    print(
        "\n[NOTE] main.py is a standalone CLI batch runner. It does NOT write to "
        "SQLite or talk to the ingestion server.\n"
        "       If ingestion_server.py is also running, each will produce its own "
        "issue-set with no shared state.\n"
        "       Run one or the other, not both, unless you specifically need "
        "parallel independent analysis."
    )

    clean_events = clean_flow_fn()
    error_events = error_flow_fn()

    clean_state = StreamState()
    error_state = StreamState()

    clean_issues = detect_issues(
        clean_events,
        specs,
        enabled_checks=profile.enabled_checks,
        state=clean_state,
    )

    error_issues = detect_issues(
        error_events,
        specs,
        enabled_checks=profile.enabled_checks,
        state=error_state,
    )

    print("\nSIGNALS:")
    if profile.signals:
        for signal in profile.signals:
            print(f"- {signal}")
    else:
        print("No strong signals detected.")

    print("\nCHECKLIST:")
    for check in profile.checks:
        status = "ON" if check.enabled else "OFF"
        print(f"- [{status}] {check.key} ({check.severity}): {check.reason}")

    print_issues("CLEAN FLOW ISSUES:", clean_issues)
    print_issues("ERROR FLOW ISSUES:", error_issues)


if __name__ == "__main__":
    main()