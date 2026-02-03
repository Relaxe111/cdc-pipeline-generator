#!/usr/bin/env python3
"""
Local Docker image build and test workflow.
Usage: ./test-local.py
"""

import subprocess
import sys


IMAGE_NAME = "cdc-local:test"


def run_command(cmd: list[str], interactive: bool = False) -> int:
    """Run a command and return exit code."""
    if interactive:
        return subprocess.run(cmd).returncode
    else:
        result = subprocess.run(cmd, capture_output=False)
        return result.returncode


def build_image() -> bool:
    """Build Docker image locally."""
    print("🔨 Building Docker image locally...")
    if run_command(["docker", "build", "-t", IMAGE_NAME, "."]) != 0:
        print("❌ Build failed!")
        return False
    print("✅ Build complete!")
    return True


def test_help_command() -> bool:
    """Test: cdc help."""
    print("\n1️⃣ Testing: cdc help")
    if run_command(["docker", "run", "--rm", IMAGE_NAME, "help"]) != 0:
        print("❌ Help command test failed!")
        return False
    return True


def test_scaffold_missing_args() -> bool:
    """Test: cdc scaffold (should show colorful error message)."""
    print("\n2️⃣ Testing: cdc scaffold (error message with colors)")
    # This should fail with exit code 2 and show colorful error
    result = run_command(["docker", "run", "--rm", IMAGE_NAME, "scaffold"])
    if result == 2:  # Expected error code for missing arguments
        print("✅ Error message displayed correctly")
        return True
    print("❌ Expected exit code 2, got", result)
    return False


def test_scaffold_help() -> bool:
    """Test: cdc scaffold --help."""
    print("\n3️⃣ Testing: cdc scaffold --help")
    if run_command(["docker", "run", "--rm", IMAGE_NAME, "scaffold", "--help"]) != 0:
        print("❌ Scaffold help test failed!")
        return False
    return True


def test_interactive_fish() -> bool:
    """Test: Interactive Fish shell with completions."""
    print("\n4️⃣ Testing: Fish shell with completions")
    print("   (Type 'cdc [TAB]' to test completions, 'exit' to quit)")
    # Override ENTRYPOINT to run fish shell
    if run_command(["docker", "run", "--rm", "-it", "--entrypoint", "fish", IMAGE_NAME], interactive=True) != 0:
        print("❌ Interactive shell test failed!")
        return False
    return True


def run_all_tests() -> bool:
    """Run all tests in sequence."""
    tests = [
        test_help_command,
        test_scaffold_missing_args,
        test_scaffold_help,
        test_interactive_fish,
    ]
    
    print("\n🧪 Running automated tests...\n")
    
    for test in tests:
        if not test():
            return False
    
    return True


def main() -> int:
    """Build and test Docker image locally."""
    # Build image
    if not build_image():
        return 1
    
    # Run tests
    if not run_all_tests():
        return 1
    
    # Success
    print("\n✅ All tests passed!")
    print("\n📦 Ready to push? Run:")
    print("   git push origin master")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
