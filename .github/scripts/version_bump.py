#!/usr/bin/env python3
"""
Determine version bump based on conventional commits and update pyproject.toml.
Outputs version info to GitHub Actions outputs.
"""
import os
import re
import subprocess
from pathlib import Path
from typing import List, Tuple


def get_latest_tag() -> str:
    """Get the latest git tag or default to v0.1.18."""
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return "v0.1.18"


def get_commits_since_tag(tag: str) -> List[str]:
    """Get commit messages since the given tag."""
    try:
        result = subprocess.run(
            ["git", "log", f"{tag}..HEAD", "--pretty=format:%s"],
            capture_output=True,
            text=True,
            check=True
        )
        commits = result.stdout.strip().split('\n')
        return [c for c in commits if c]  # Filter empty lines
    except subprocess.CalledProcessError:
        # If tag doesn't exist, get all commits
        result = subprocess.run(
            ["git", "log", "--pretty=format:%s"],
            capture_output=True,
            text=True,
            check=True
        )
        commits = result.stdout.strip().split('\n')
        return [c for c in commits if c]


def parse_version(version_str: str) -> Tuple[int, int, int]:
    """Parse version string (with or without 'v' prefix) into (major, minor, patch)."""
    version = version_str.lstrip('v')
    parts = version.split('.')
    return int(parts[0]), int(parts[1]), int(parts[2])


def determine_bump_type(commits: List[str]) -> str:
    """Determine version bump type based on conventional commits."""
    # Check for breaking changes (! in commit)
    if any('!' in commit for commit in commits):
        return 'major'
    
    # Check for features (feat:)
    if any(re.match(r'^feat(\(.+\))?:', commit, re.IGNORECASE) for commit in commits):
        return 'minor'
    
    # Check for fixes/chore (fix:, chore:)
    if any(re.match(r'^(fix|chore)(\(.+\))?:', commit, re.IGNORECASE) for commit in commits):
        return 'patch'
    
    # Default to patch
    return 'patch'


def bump_version(old_version: str, bump_type: str) -> str:
    """Bump version based on bump type."""
    major, minor, patch = parse_version(old_version)
    
    if bump_type == 'major':
        return f"{major + 1}.0.0"
    elif bump_type == 'minor':
        return f"{major}.{minor + 1}.0"
    else:  # patch
        return f"{major}.{minor}.{patch + 1}"


def update_pyproject_toml(new_version: str) -> None:
    """Update version in pyproject.toml."""
    pyproject_path = Path(__file__).parent.parent.parent / 'pyproject.toml'
    content = pyproject_path.read_text()
    
    # Replace version line
    new_content = re.sub(
        r'^version = ".*"',
        f'version = "{new_version}"',
        content,
        flags=re.MULTILINE
    )
    
    pyproject_path.write_text(new_content)
    print(f"Updated pyproject.toml to version {new_version}")


def set_github_output(name: str, value: str, multiline: bool = False) -> None:
    """Set GitHub Actions output."""
    github_output = Path(os.environ.get('GITHUB_OUTPUT', '/dev/stdout'))
    with github_output.open('a') as f:
        if multiline:
            f.write(f"{name}<<EOF\n")
            f.write(f"{value}\n")
            f.write("EOF\n")
        else:
            f.write(f"{name}={value}\n")


def main() -> None:
    """Main execution."""
    # Get latest tag
    latest_tag = get_latest_tag()
    old_version = latest_tag.lstrip('v')
    print(f"Latest tag: {latest_tag}")
    
    # Get commits since tag
    commits = get_commits_since_tag(latest_tag)
    print(f"Found {len(commits)} commits since {latest_tag}")
    
    # Determine bump type
    bump_type = determine_bump_type(commits)
    print(f"Bump type: {bump_type}")
    
    # Calculate new version
    new_version = bump_version(old_version, bump_type)
    print(f"Version bump: {old_version} → {new_version}")
    
    # Update pyproject.toml
    update_pyproject_toml(new_version)
    
    # Set GitHub Actions outputs
    set_github_output('old_version', old_version)
    set_github_output('new_version', new_version)
    set_github_output('bump_type', bump_type)
    
    # Write commits for release notes (multiline)
    commits_text = '\n'.join(f"- {commit}" for commit in commits)
    set_github_output('commits', commits_text, multiline=True)


if __name__ == '__main__':
    main()
