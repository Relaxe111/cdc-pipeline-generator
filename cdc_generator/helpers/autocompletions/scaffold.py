"""Scaffold flag autocompletion functions."""


def scaffold_flag_completions(flag: str) -> list[str]:
    """Return appropriate completions for scaffold subcommand flags.

    Args:
        flag: The flag name (--pattern, --source-type, etc.).

    Returns:
        List of "value\tDescription" formatted completions.

    Example:
        >>> scaffold_flag_completions('--pattern')
        ['db-per-tenant\\tOne database per tenant', 'db-shared\\tShared database']
    """
    completions = {
        '--pattern': [
            'db-per-tenant\tOne database per tenant',
            'db-shared\tShared database for all tenants',
        ],
        '--source-type': [
            'postgres\tPostgreSQL database',
            'mssql\tMicrosoft SQL Server',
        ],
    }

    return completions.get(flag, [])
