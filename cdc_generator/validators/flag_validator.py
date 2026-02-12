"""
Flag combination validation for manage-source-groups commands.

This module validates flag combinations and provides helpful error messages
when users specify incompatible or incorrect flag combinations.

Example:
    >>> from argparse import Namespace
    >>> args = Namespace(update=True, create='mygroup', all=False)
    >>> result = validate_manage_server_group_flags(args)
    >>> if not result.valid:
    ...     print(result.message)
    ❌ Cannot use multiple actions together: --update, --create
"""

import argparse
from dataclasses import dataclass
from typing import ClassVar


@dataclass
class ValidationResult:
    """Result of flag validation.

    Attributes:
        valid: Whether the flag combination is valid
        level: Severity level ('error', 'warning', 'ok')
        message: Human-readable message explaining the issue
        suggestion: Optional suggestion for correct usage
    """
    valid: bool
    level: str  # 'error' | 'warning' | 'ok'
    message: str | None = None
    suggestion: str | None = None


class ManageServerGroupFlagValidator:
    """Validates flag combinations for manage-source-groups command."""

    # Action flags (mutually exclusive)
    ACTIONS: ClassVar[frozenset[str]] = frozenset({
        'update', 'info',
        'db_definitions',
        'add_server', 'remove_server', 'list_servers',
        'set_kafka_topology',
        'add_to_ignore_list', 'list_ignore_patterns',
        'add_to_schema_excludes', 'list_schema_excludes',
        'add_env_mapping', 'list_env_mappings',
    })

    OK_ACTIONS: ClassVar[frozenset[str]] = frozenset({
        'db_definitions',
        'info',
        'list_servers',
        'list_ignore_patterns',
        'list_schema_excludes',
        'list_env_mappings',
    })

    # Flags that only make sense with --update
    UPDATE_ONLY_FLAGS: ClassVar[frozenset[str]] = frozenset({'all'})

    # Connection flags (work with --add-server)
    CONNECTION_FLAGS: ClassVar[frozenset[str]] = frozenset(
        {'host', 'port', 'user', 'password', 'source_type'}
    )

    def validate(self, args: argparse.Namespace) -> ValidationResult:
        """Validate flag combinations.

        Args:
            args: Parsed command line arguments

        Returns:
            ValidationResult with validation status and messages

        Example:
            >>> args = Namespace(update='default', all=True)
            >>> validator = ManageServerGroupFlagValidator()
            >>> result = validator.validate(args)
            >>> result.valid
            True
        """
        # Find which action(s) are present
        active_actions = self._get_active_actions(args)

        # No action = OK (will show help or general info)
        if not active_actions:
            return ValidationResult(valid=True, level='ok')

        # Multiple actions = ERROR
        if len(active_actions) > 1:
            return self._error_multiple_actions(active_actions)

        action = active_actions[0]

        # Validate based on specific action
        if action == 'update':
            result = self._validate_update(args)
        elif action == 'add_server':
            result = self._validate_add_server(args)
        elif action == 'remove_server':
            result = self._validate_remove_server(args)
        elif action == 'set_kafka_topology':
            result = self._validate_set_kafka_topology(args)
        elif action in self.OK_ACTIONS:
            result = ValidationResult(valid=True, level='ok')
        else:
            result = self._validate_config_action(args, action)

        return result

    def _get_active_actions(self, args: argparse.Namespace) -> list[str]:
        """Get list of active action flags.

        Args:
            args: Parsed arguments

        Returns:
            List of active action flag names
        """
        active: list[str] = []
        for action in self.ACTIONS:
            attr_value = getattr(args, action, None)
            # Check if flag is set (could be True, a string value, or non-None)
            if attr_value is not None and attr_value is not False:
                active.append(action)
        return active

    def _validate_update(self, args: argparse.Namespace) -> ValidationResult:
        """Validate --update flag combination.

        Args:
            args: Parsed arguments

        Returns:
            ValidationResult for update action
        """
        warnings: list[str] = []

        # Check --all with explicit server_name
        if getattr(args, 'all', False):
            update_value = getattr(args, 'update', None)
            # If update has a value other than 'default' (the default), it's a conflict
            if update_value and update_value != 'default':
                return ValidationResult(
                    valid=False,
                    level='error',
                    message="❌ Cannot use both --all and specific server name",
                    suggestion=(
                        "💡 Choose one approach:\n"
                        "   cdc manage-source-groups --update --all "
                        "# Update all servers\n"
                        "   cdc manage-source-groups --update prod "
                        "# Update specific server"
                    )
                )

        if warnings:
            return ValidationResult(valid=True, level='warning', message='\n'.join(warnings))

        return ValidationResult(valid=True, level='ok')

    def _validate_add_server(self, args: argparse.Namespace) -> ValidationResult:
        """Validate --add-server flag combination.

        Args:
            args: Parsed arguments

        Returns:
            ValidationResult for add-server action
        """
        # --add-server requires a server name
        if not getattr(args, 'add_server', None):
            return ValidationResult(
                valid=False,
                level='error',
                message="❌ --add-server requires a server name",
                suggestion=(
                    "💡 Example:\n"
                    "   cdc manage-source-groups --add-server analytics"
                )
            )

        # Check for incompatible flags
        warnings: list[str] = []
        incompatible = self.UPDATE_ONLY_FLAGS
        for flag in incompatible:
            if hasattr(args, flag) and getattr(args, flag):
                flag_name = f'--{flag.replace("_", "-")}'
                warnings.append(f"⚠️  WARNING: {flag_name} is ignored with --add-server")

        if warnings:
            return ValidationResult(valid=True, level='warning', message='\n'.join(warnings))

        return ValidationResult(valid=True, level='ok')

    def _validate_remove_server(self, args: argparse.Namespace) -> ValidationResult:
        """Validate --remove-server.

        Args:
            args: Parsed arguments

        Returns:
            ValidationResult for remove-server action
        """
        if not getattr(args, 'remove_server', None):
            return ValidationResult(
                valid=False,
                level='error',
                message="❌ --remove-server requires a server name",
                suggestion="💡 Example: cdc manage-source-groups --remove-server analytics"
            )

        return ValidationResult(valid=True, level='ok')

    def _validate_set_kafka_topology(self, args: argparse.Namespace) -> ValidationResult:
        """Validate --set-kafka-topology.

        Args:
            args: Parsed arguments

        Returns:
            ValidationResult for set-kafka-topology action
        """
        topology = getattr(args, 'set_kafka_topology', None)

        if not topology:
            return ValidationResult(
                valid=False,
                level='error',
                message="❌ --set-kafka-topology requires a value",
                suggestion=(
                    "💡 Valid values:\n"
                    "   cdc manage-source-groups --set-kafka-topology shared "
                    "# Same Kafka for all servers\n"
                    "   cdc manage-source-groups --set-kafka-topology per-server "
                    "# Separate Kafka per server"
                )
            )

        if topology not in {'shared', 'per-server'}:
            return ValidationResult(
                valid=False,
                level='error',
                message=f"❌ Invalid topology: {topology}",
                suggestion="💡 Valid values: shared, per-server"
            )

        return ValidationResult(valid=True, level='ok')

    def _validate_config_action(self, args: argparse.Namespace, action: str) -> ValidationResult:
        """Validate configuration actions (add-to-ignore-list, etc.).

        Args:
            args: Parsed arguments
            action: Action name

        Returns:
            ValidationResult for config actions
        """
        value = getattr(args, action, None)

        if not value:
            return ValidationResult(
                valid=False,
                level='error',
                message=f"❌ --{action.replace('_', '-')} requires a value"
            )

        return ValidationResult(valid=True, level='ok')

    def _error_multiple_actions(self, actions: list[str]) -> ValidationResult:
        """Return error for multiple actions.

        Args:
            actions: List of active actions

        Returns:
            ValidationResult with error for multiple actions
        """
        action_names = ', '.join(f'--{a.replace("_", "-")}' for a in sorted(actions))
        return ValidationResult(
            valid=False,
            level='error',
            message=f"❌ Cannot use multiple actions together: {action_names}",
            suggestion="💡 Use only one action flag at a time"
        )


def validate_manage_server_group_flags(args: argparse.Namespace) -> ValidationResult:
    """Convenience function to validate manage-source-groups flags.

    Args:
        args: Parsed command line arguments

    Returns:
        ValidationResult with validation status

    Example:
        >>> from argparse import Namespace
        >>> args = Namespace(update='default', all=False)
        >>> result = validate_manage_server_group_flags(args)
        >>> result.valid
        True
    """
    validator = ManageServerGroupFlagValidator()
    return validator.validate(args)
