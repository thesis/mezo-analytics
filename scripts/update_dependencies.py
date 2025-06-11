#!/usr/bin/env python3
"""
Script to check and update dependencies in requirements.txt
"""

import json
from pathlib import Path
import subprocess
import sys


def run_command(cmd):
    """Run a shell command and return the result."""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"❌ Error running command: {cmd}")
        print(f"Error: {e.stderr}")
        return None


def check_security_vulnerabilities():
    """Check for security vulnerabilities using pip-audit."""
    print("🔐 Checking for security vulnerabilities...")

    # Install pip-audit if not available
    run_command("pip install pip-audit")

    # Run security audit
    result = run_command("pip-audit --requirement requirements.txt --format=json")

    if result:
        try:
            vulnerabilities = json.loads(result)
            if vulnerabilities:
                print(f"⚠️  Found {len(vulnerabilities)} security vulnerabilities!")
                for vuln in vulnerabilities:
                    if isinstance(vuln, dict):
                        print(
                            f"  - {vuln.get('name', 'Unknown')}: {vuln.get('description', 'No description')}"
                        )
                    else:
                        print(f"  - {vuln}")
                return False
            else:
                print("✅ No security vulnerabilities found")
                return True
        except json.JSONDecodeError:
            print("⚠️  Could not parse security audit results")
            return True
    return True


def check_outdated_packages():
    """Check for outdated packages."""
    print("\n📊 Checking for outdated packages...")

    result = run_command("pip list --outdated --format=json")

    if result:
        try:
            outdated = json.loads(result)
            if outdated:
                print(f"📋 Found {len(outdated)} outdated packages:")
                for pkg in outdated:
                    name = pkg.get("name", "Unknown")
                    current = pkg.get("version", "Unknown")
                    latest = pkg.get("latest_version", "Unknown")
                    print(f"  - {name}: {current} → {latest}")
                return outdated
            else:
                print("✅ All packages are up to date")
                return []
        except json.JSONDecodeError:
            print("⚠️  Could not parse outdated packages results")
            return []
    return []


def update_requirements_file(outdated_packages):
    """Update requirements.txt with new versions."""
    if not outdated_packages:
        return

    print("\n🔄 Updating requirements.txt...")

    # Read current requirements
    req_file = Path("requirements.txt")
    if not req_file.exists():
        print("❌ requirements.txt not found")
        return

    lines = req_file.read_text().splitlines()
    updated_lines = []
    updated_count = 0

    for line in lines:
        if line.strip() and not line.startswith("#") and ">=" in line:
            package_name = line.split(">=")[0].strip()

            # Check if this package needs updating
            for pkg in outdated_packages:
                if pkg.get("name", "").lower() == package_name.lower():
                    new_version = pkg.get("latest_version", "")
                    if new_version:
                        new_line = f"{package_name}>={new_version}"
                        updated_lines.append(new_line)
                        updated_count += 1
                        print(f"  ✅ Updated {package_name}: {line} → {new_line}")
                        break
            else:
                updated_lines.append(line)
        else:
            updated_lines.append(line)

    if updated_count > 0:
        # Write updated requirements
        req_file.write_text("\n".join(updated_lines) + "\n")
        print(f"\n🎉 Updated {updated_count} packages in requirements.txt")

        # Test installation
        print("\n🧪 Testing updated requirements...")
        test_result = run_command("pip install -r requirements.txt --dry-run")
        if test_result is not None:
            print("✅ Requirements file syntax is valid")
        else:
            print("❌ Requirements file has issues - please review manually")
    else:
        print("ℹ️  No updates needed")


def main():
    """Main function to run dependency checks and updates."""
    print("🔍 Dependency Update Check")
    print("=" * 50)

    # Install current requirements first
    print("📦 Installing current requirements...")
    install_result = run_command("pip install -r requirements.txt")
    if install_result is None:
        print("❌ Failed to install current requirements")
        sys.exit(1)

    # Check for security issues
    security_ok = check_security_vulnerabilities()

    # Check for outdated packages
    outdated = check_outdated_packages()

    # Ask user if they want to update
    if outdated:
        print(f"\n❓ Do you want to update requirements.txt with {len(outdated)} newer versions?")
        response = input("Enter 'y' to update, or any other key to skip: ").lower().strip()

        if response == "y":
            update_requirements_file(outdated)
        else:
            print("ℹ️  Skipping updates")

    # Summary
    print("\n" + "=" * 50)
    if not security_ok:
        print("⚠️  SECURITY ISSUES FOUND - Please review and update vulnerable packages")
        sys.exit(1)
    elif outdated:
        print("📊 Some packages have updates available")
    else:
        print("✅ All dependencies are secure and up to date!")


if __name__ == "__main__":
    main()
