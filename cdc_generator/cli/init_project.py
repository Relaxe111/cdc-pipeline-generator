#!/usr/bin/env python3
"""
Initialize a new CDC Pipeline project with dev container setup.
"""

import sys
import shutil
import subprocess
from pathlib import Path
from importlib import resources


def copy_template_files(target_dir: Path):
    """Copy template files to target directory."""
    try:
        # Use importlib.resources to access package data
        import cdc_generator.templates.init as init_templates
        
        templates_path = Path(init_templates.__file__).parent
        
        files_to_copy = [
            'docker-compose.yml',
            'Dockerfile.dev',
            'server-groups.yaml',
            'README.md',
            '.gitignore',
            'cdc.fish'
        ]
        
        for file_name in files_to_copy:
            src = templates_path / file_name
            dst = target_dir / file_name
            
            if dst.exists():
                print(f"⚠️  {file_name} already exists, skipping...")
                continue
            
            shutil.copy2(src, dst)
            print(f"✅ Created {file_name}")
            
    except Exception as e:
        print(f"❌ Error copying templates: {e}")
        return False
    
    return True


def create_directory_structure(target_dir: Path):
    """Create the required directory structure."""
    directories = [
        'services',
        'pipeline-templates',
        'generated/pipelines',
        'generated/schemas',
        'generated/table-definitions',
    ]
    
    for dir_path in directories:
        full_path = target_dir / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {dir_path}/")


def create_env_example(target_dir: Path):
    """Create .env.example file."""
    env_content = """# Source Database (MSSQL)
MSSQL_HOST=mssql
MSSQL_PORT=1433
MSSQL_USER=sa
MSSQL_PASSWORD=YourPassword123!

# Source Database (PostgreSQL - if using PostgreSQL as source)
POSTGRES_SOURCE_HOST=postgres-source
POSTGRES_SOURCE_PORT=5432
POSTGRES_SOURCE_USER=postgres
POSTGRES_SOURCE_PASSWORD=postgres
POSTGRES_SOURCE_DB=source_db

# Target Database (PostgreSQL - CDC always targets PostgreSQL)
POSTGRES_TARGET_HOST=postgres-target
POSTGRES_TARGET_PORT=5432
POSTGRES_TARGET_USER=postgres
POSTGRES_TARGET_PASSWORD=postgres
POSTGRES_TARGET_DB=cdc_target

# Redpanda Connect
REDPANDA_CONNECT_VERSION=latest
"""
    
    env_file = target_dir / '.env.example'
    env_file.write_text(env_content)
    print("✅ Created .env.example")


def init_git_repo(target_dir: Path):
    """Initialize git repository."""
    try:
        result = subprocess.run(
            ['git', 'init'],
            cwd=target_dir,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("✅ Initialized git repository")
            return True
    except FileNotFoundError:
        print("⚠️  Git not found, skipping repository initialization")
    except Exception as e:
        print(f"⚠️  Could not initialize git: {e}")
    
    return False


def build_container(target_dir: Path):
    """Build and start the dev container."""
    print("\n🐳 Building dev container...")
    
    try:
        # Check if docker compose is available
        result = subprocess.run(
            ['docker', 'compose', 'version'],
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            print("❌ Docker Compose not found. Please install Docker.")
            return False
        
        # Build and start container
        print("   This may take a few minutes on first run...")
        result = subprocess.run(
            ['docker', 'compose', 'up', '-d', '--build'],
            cwd=target_dir,
            capture_output=False  # Show output to user
        )
        
        if result.returncode == 0:
            print("\n✅ Dev container built and started!")
            return True
        else:
            print("\n❌ Failed to build container")
            return False
            
    except FileNotFoundError:
        print("❌ Docker not found. Please install Docker first.")
        return False
    except Exception as e:
        print(f"❌ Error building container: {e}")
        return False


def init_project(args):
    """
    Initialize a new CDC Pipeline project.
    
    Args:
        args: Command-line arguments (currently unused)
    """
    target_dir = Path.cwd()
    
    # Check if directory is empty (allow .git, .gitignore, README.md)
    existing_files = [
        f for f in target_dir.iterdir() 
        if f.name not in ['.git', '.gitignore', 'README.md', '.DS_Store']
    ]
    
    if existing_files:
        print("⚠️  Current directory is not empty!")
        print(f"   Found: {', '.join(f.name for f in existing_files[:5])}")
        response = input("\nContinue anyway? [y/N]: ")
        if response.lower() != 'y':
            print("Aborted.")
            return 1
    
    print("\n🚀 Initializing CDC Pipeline project...\n")
    
    # Step 1: Create directory structure
    print("📁 Creating directory structure...")
    create_directory_structure(target_dir)
    
    # Step 2: Copy template files
    print("\n📄 Copying template files...")
    if not copy_template_files(target_dir):
        return 1
    
    # Step 3: Create .env.example
    print("\n🔧 Creating environment configuration...")
    create_env_example(target_dir)
    
    # Step 4: Initialize git (optional)
    print("\n🔨 Setting up version control...")
    init_git_repo(target_dir)
    
    # Step 5: Ask to build container
    print("\n" + "="*60)
    print("✅ Project initialized successfully!")
    print("="*60)
    
    response = input("\n🐳 Build and start dev container now? [Y/n]: ")
    if response.lower() != 'n':
        if build_container(target_dir):
            print("\n🎉 All done! Enter the container with:")
            print(f"\n   cd {target_dir.name}")
            print("   docker compose exec dev fish\n")
            print("Inside the container, run:")
            print("   cdc manage-service --create <service-name>")
            return 0
        else:
            print("\n⚠️  Container build failed. You can try manually:")
            print("   docker compose up -d --build")
            return 1
    else:
        print("\n📝 Next steps:")
        print("   1. Edit server-groups.yaml with your database details")
        print("   2. Copy .env.example to .env and configure")
        print("   3. Build container: docker compose up -d --build")
        print("   4. Enter container: docker compose exec dev fish")
        return 0


if __name__ == "__main__":
    sys.exit(init_project(sys.argv[1:]))
