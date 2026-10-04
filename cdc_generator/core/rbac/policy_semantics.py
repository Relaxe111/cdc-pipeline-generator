"""Compile-time proof for the closed flat context predicate language, never an evaluator in production."""

from __future__ import annotations

import itertools
import re
from dataclasses import dataclass

from cdc_generator.core.rbac.composition_models import Policy
from cdc_generator.core.rbac.validation import ROLES, SESSION

Value = str | tuple[str, tuple["Value", ...]]
State = tuple[str | None, bool, bool]
MAX_TOKENS = 512
MAX_DEPTH = 32


@dataclass(frozen=True)
class Expression:
    """Positive Boolean expression whose comparisons can grant access only when true."""

    kind: str
    children: tuple[Expression, ...] = ()
    atom: str = ""

    def accepts(self, state: State) -> bool:
        """Enumerate symbolic equality outcomes, including null/missing context denial."""
        if self.kind == "and":
            return all(child.accepts(state) for child in self.children)
        if self.kind == "or":
            return any(child.accepts(state) for child in self.children)
        if self.kind == "true":
            return True
        if self.kind == "false":
            return False
        if self.atom.startswith("role:"):
            return state[0] == self.atom[5:]
        return state[1] if self.atom == "customer_id" else state[2]


class Parser:
    """Parse only AND/OR, bool constants and exact typed field/session equalities."""

    def __init__(self, sql: str) -> None:
        pattern = r"\s+|'(?:''|[^'])*'|\"[a-z_][a-z0-9_]*\"|[a-z_][a-z0-9_]*|::|[(),=]"
        self.tokens = re.findall(pattern, sql, re.IGNORECASE)
        if "".join(self.tokens) != sql:
            raise ValueError("Unsupported retained predicate token")
        self.tokens = [token for token in self.tokens if not token.isspace()]
        if len(self.tokens) > MAX_TOKENS:
            raise ValueError("Retained predicate exceeds bounded flat profile")
        self.pos = 0
        self.depth = 0

    def peek(self) -> str:
        """Return the next token without consuming an absent value."""
        if self.pos >= len(self.tokens):
            return ""
        token = self.tokens[self.pos]
        return token if token.startswith(("'", '"')) else token.lower()

    def take(self, token: str) -> None:
        """Require exact syntax rather than normalizing unknown SQL."""
        if self.peek() != token:
            raise ValueError(f"Unsupported retained predicate: expected {token}")
        self.pos += 1

    def expression(self, operator: str = "or") -> Expression:
        """Parse Boolean precedence with a finite nesting limit."""
        self.depth += 1
        if self.depth > MAX_DEPTH:
            raise ValueError("Retained predicate exceeds depth limit")
        children = [self.expression("and") if operator == "or" else self.primary()]
        while self.peek() == operator:
            self.pos += 1
            children.append(self.expression("and") if operator == "or" else self.primary())
        self.depth -= 1
        return children[0] if len(children) == 1 else Expression(operator, tuple(children))

    def primary(self) -> Expression:
        """Parse a comparison or parentheses enclosing a Boolean expression."""
        start = self.pos
        if self.peek() == "(":
            self.pos += 1
            expr = self.expression()
            self.take(")")
            if self.peek() != "=":
                return expr
            self.pos = start
        if self.peek() in {"true", "false"}:
            kind = self.peek()
            self.pos += 1
            return Expression(kind)
        left = self.value()
        self.take("=")
        right = self.value()
        for name, guc in SESSION.values():
            expected: Value = ("uuid", (("nullif", (("current_setting", (f"'{guc}'", "true")), "''")),))
            if left == name and right == expected:
                return Expression("atom", atom=name)
        role_value: Value = ("nullif", (("current_setting", ("'app.role'", "true")), "''"))
        if left == role_value and isinstance(right, str) and right in {f"'{role}'" for role in ROLES}:
            return Expression("atom", atom="role:" + right[1:-1])
        raise ValueError("Unsupported retained equality/context/type")

    def value(self) -> Value:
        """Parse safe scalar syntax, accepting pg_get_expr's explicit text casts."""
        value: Value
        token = self.peek()
        if token == "(":
            self.pos += 1
            value = self.value()
            self.take(")")
        elif token in {"current_setting", "nullif"}:
            self.pos += 1
            self.take("(")
            args = [self.value()]
            self.take(",")
            args.append(self.value())
            self.take(")")
            value = (token, tuple(args))
        elif token and (token.startswith(("'", '"')) or re.fullmatch(r"[a-z_][a-z0-9_]*", token)):
            value = token.strip('"')
            self.pos += 1
        else:
            raise ValueError("Unsupported retained scalar")
        while self.peek() == "::":
            self.pos += 1
            cast = self.peek()
            self.pos += 1
            if cast not in {"text", "uuid"}:
                raise ValueError("Unsupported retained cast")
            # PostgreSQL text casts on string literals/context text are redundant.
            # A UUID field cast changes the comparison type and is not this profile.
            text_value = (isinstance(value, str) and value.startswith("'")) or (
                isinstance(value, tuple) and value[0] in {"nullif", "current_setting"}
            )
            if cast != "text" or not text_value:
                arguments: tuple[Value, ...] = (value,)
                value = (cast, arguments)
        return value


def parse(sql: str | None) -> Expression:
    """Reject unknown/null definitions; no PostgreSQL fallback is fabricated."""
    if sql is None:
        raise ValueError("Missing predicate in supported retained policy profile")
    parser = Parser(sql)
    result = parser.expression()
    if parser.pos != len(parser.tokens):
        raise ValueError("Unsupported trailing retained predicate syntax")
    return result


def prove_compatible(generated: tuple[Policy, ...], retained: tuple[Policy, ...]) -> None:
    """Prove permissive OR/restrictive AND non-widening and preserve all authorized positives."""
    states = tuple(itertools.product((*ROLES, None, "unknown"), (False, True), (False, True)))
    for command, field in [("SELECT", "using"), ("INSERT", "check"), ("UPDATE", "using"), ("UPDATE", "check"), ("DELETE", "using")]:
        owned = [parse(getattr(policy, field)) for policy in generated if policy.command == command]
        if not owned:
            raise ValueError(f"Missing generated command witness: {command}")
        applicable = [policy for policy in retained if policy.command in {command, "ALL"}]
        permissive = [parse(getattr(policy, field)) for policy in applicable if policy.permissive]
        restrictive = [parse(getattr(policy, field)) for policy in applicable if not policy.permissive]
        for state in states:
            authorized = any(expr.accepts(state) for expr in owned)
            effective = (authorized or any(expr.accepts(state) for expr in permissive)) and all(expr.accepts(state) for expr in restrictive)
            if effective and not authorized:
                raise ValueError(f"Retained {command} {field} widens generated authorization")
            if authorized and not effective:
                raise ValueError(f"Retained {command} {field} removes authorized positive operations")
