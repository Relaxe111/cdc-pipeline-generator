"""Help text rendering helpers for the main CLI commands entrypoint."""

from __future__ import annotations

from pathlib import Path


def print_help(
    workspace_root: Path,
    implementation_name: str | None,
    is_dev_container: bool,
    *,
    doc_text: str | None,
    command_groups: dict[str, dict[str, dict[str, str]]],
) -> None:
    """Print help message with all available commands."""
    print(doc_text)

    if is_dev_container:
        if implementation_name:
            print(f"📍 Environment: Dev container - /implementations/{implementation_name}")
        else:
            print("📍 Environment: Dev container - /workspace (generator)")
    else:
        print(f"📍 Environment: Host - {workspace_root}")
        if implementation_name:
            print(f"   Implementation: {implementation_name}")

    print("\n📦 Top-level generator commands:")
    for cmd, info in command_groups["generator"].items():
        print(f"  {cmd:20} - {info['description']}")

    print("\n🧩 Service management:")
    for cmd, info in command_groups["service"].items():
        desc = info["description"]
        usage = info.get("usage")
        if usage is not None:
            desc += f"\n  {' ' * 20}   Usage: {usage}"
        print(f"  {'manage-services ' + cmd:20} - {desc}")

    print("\n⚡ Aliases:")
    print("  ms                   - alias for manage-services")
    print("  msc                  - alias for manage-services config")
    print("  msr                  - alias for manage-services resources")
    print("  mss                  - alias for manage-services resources")
    print("  msog                 - alias for manage-source-groups")
    print("  msig                 - alias for manage-sink-groups")
    print("  mp                   - alias for manage-pipelines")
    print("  mm                   - alias for manage-migrations")

    print("\n🔄 Pipeline management:")
    for cmd, info in command_groups["pipeline"].items():
        desc = info["description"]
        usage = info.get("usage")
        if usage is not None:
            desc += f"\n  {' ' * 20}   Usage: {usage}"
        print(f"  {'manage-pipelines ' + cmd:20} - {desc}")

    print("\n🗄️ Migration management:")
    for cmd, info in command_groups["migration"].items():
        desc = info["description"]
        usage = info.get("usage")
        if usage is not None:
            desc += f"\n  {' ' * 20}   Usage: {usage}"
        print(f"  {'manage-migrations ' + cmd:20} - {desc}")

    print("\n🧪 Testing:")
    print("  test                 - Run tests (--cli for e2e, --all for everything)")
    print("  test-coverage        - Show test coverage report by cdc command (-v for details)")

    print("\n💡 Tip: Run commands from implementation directory or dev container")
