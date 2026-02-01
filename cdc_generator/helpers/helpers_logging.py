"""Simple logging helpers for CDC generator CLI."""


class Colors:
    """ANSI color codes for terminal output."""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    CYAN = '\033[96m'  # Alias for OKCYAN
    BLUE = '\033[94m'  # Alias for OKBLUE
    OKGREEN = '\033[92m'
    GREEN = '\033[92m'  # Alias for OKGREEN
    WARNING = '\033[93m'
    YELLOW = '\033[93m'  # Alias for WARNING
    FAIL = '\033[91m'
    RED = '\033[91m'  # Alias for FAIL
    ENDC = '\033[0m'
    RESET = '\033[0m'  # Alias for ENDC
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    DIM = '\033[2m'


def print_header(msg: str) -> None:
    """Print a header message."""
    print(f"{Colors.HEADER}{Colors.BOLD}{msg}{Colors.ENDC}")


def print_info(msg: str) -> None:
    """Print an info message."""
    print(f"{Colors.OKCYAN}{msg}{Colors.ENDC}")


def print_success(msg: str) -> None:
    """Print a success message."""
    print(f"{Colors.OKGREEN}✓ {msg}{Colors.ENDC}")


def print_warning(msg: str) -> None:
    """Print a warning message."""
    print(f"{Colors.YELLOW}⚠️  {msg}{Colors.ENDC}")


def print_error(msg: str) -> None:
    """Print an error message."""
    print(f"{Colors.RED}❌ {msg}{Colors.ENDC}")
