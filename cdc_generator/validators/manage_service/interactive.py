"""Interactive prompts for CLI workflows (legacy functionality)."""

from typing import List, Dict, Optional
from helpers_logging import print_info, print_success, print_warning, print_error, print_header, Colors


def prompt_select(prompt: str, options: List[str], allow_empty: bool = False) -> Optional[str]:
    """Interactive prompt to select from a list of options."""
    if not options:
        print_error(f"No options available for: {prompt}")
        return None
    
    print_info(f"\n{prompt}")
    for i, option in enumerate(options, 1):
        print(f"  {i}. {option}")
    
    if allow_empty:
        print(f"  0. Skip")
    
    while True:
        try:
            choice = input(f"\nSelect (1-{len(options)}): ").strip()
            if allow_empty and choice == "0":
                return None
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                return options[idx]
            print_warning(f"Please enter a number between 1 and {len(options)}")
        except (ValueError, KeyboardInterrupt):
            print_error("\nCancelled")
            return None


def prompt_multiselect(prompt: str, options: List[str]) -> List[str]:
    """Interactive prompt to select multiple items from a list."""
    if not options:
        return []
    
    print_info(f"\n{prompt}")
    for i, option in enumerate(options, 1):
        print(f"  {i}. {option}")
    print(f"  0. Done")
    
    selected = []
    while True:
        try:
            choice = input(f"\nSelect (1-{len(options)}, 0=done): ").strip()
            if choice == "0":
                break
            idx = int(choice) - 1
            if 0 <= idx < len(options):
                item = options[idx]
                if item in selected:
                    selected.remove(item)
                    print_warning(f"Removed: {item}")
                else:
                    selected.append(item)
                    print_success(f"Added: {item}")
            else:
                print_warning(f"Please enter a number between 0 and {len(options)}")
        except (ValueError, KeyboardInterrupt):
            print_error("\nCancelled")
            return []
    
    return selected


def prompt_mappings(source_columns: List[str], sink_columns: List[str]) -> Dict[str, str]:
    """Interactive prompt to create column mappings."""
    print_header("Column Mappings")
    print_info("Map source columns to sink columns (empty to skip)")
    
    mappings = {}
    for src_col in source_columns:
        print(f"\n{Colors.CYAN}Source column: {src_col}{Colors.RESET}")
        print("Available sink columns:")
        for i, sink_col in enumerate(sink_columns, 1):
            print(f"  {i}. {sink_col}")
        print("  0. Skip (no mapping)")
        
        try:
            choice = input(f"Map to (0-{len(sink_columns)}): ").strip()
            if choice == "0" or not choice:
                continue
            idx = int(choice) - 1
            if 0 <= idx < len(sink_columns):
                mappings[src_col] = sink_columns[idx]
                print_success(f"{src_col} → {sink_columns[idx]}")
        except (ValueError, KeyboardInterrupt):
            print_error("\nCancelled")
            return {}
    
    return mappings


def validate_table_compatibility(source_def: Dict, sink_def: Dict, mappings: Dict[str, str]) -> bool:
    """Validate that source and sink tables are compatible.
    
    TODO: Add actual validation logic:
    - Check data types compatibility
    - Check nullable constraints
    - Check primary key compatibility
    """
    print_warning("⚠️  Table compatibility validation not yet implemented")
    return True
