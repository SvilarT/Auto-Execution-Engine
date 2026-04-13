from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
from math import isfinite
from typing import TypeAlias

DecimalInput: TypeAlias = Decimal | str | int | float

MONEY_QUANTUM = Decimal("0.01")
PRICE_QUANTUM = Decimal("0.0001")
QUANTITY_QUANTUM = Decimal("0.00000001")


class DecimalPolicyError(ValueError):
    """Raised when a value violates the repository Decimal precision policy."""


def parse_decimal(value: DecimalInput) -> Decimal:
    if isinstance(value, Decimal):
        decimal_value = value
    elif isinstance(value, int):
        decimal_value = Decimal(value)
    elif isinstance(value, float):
        if not isfinite(value):
            raise DecimalPolicyError("float inputs must be finite")
        decimal_value = Decimal(str(value))
    elif isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            raise DecimalPolicyError("string inputs must not be empty")
        try:
            decimal_value = Decimal(candidate)
        except InvalidOperation as exc:
            raise DecimalPolicyError(f"invalid decimal value: {value!r}") from exc
    else:
        raise DecimalPolicyError(
            "decimal policy accepts Decimal, str, int, or float inputs only"
        )

    if not decimal_value.is_finite():
        raise DecimalPolicyError("decimal values must be finite")
    return decimal_value


def quantize_money(value: DecimalInput) -> Decimal:
    return parse_decimal(value).quantize(MONEY_QUANTUM, rounding=ROUND_HALF_EVEN)


def quantize_price(value: DecimalInput) -> Decimal:
    return parse_decimal(value).quantize(PRICE_QUANTUM, rounding=ROUND_HALF_EVEN)


def quantize_quantity(value: DecimalInput) -> Decimal:
    return parse_decimal(value).quantize(QUANTITY_QUANTUM, rounding=ROUND_HALF_EVEN)


def decimals_equal(*, left: DecimalInput, right: DecimalInput) -> bool:
    return parse_decimal(left) == parse_decimal(right)


def canonical_decimal_string(value: DecimalInput) -> str:
    normalized = parse_decimal(value).normalize()
    rendered = format(normalized, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered if rendered not in {"", "-0"} else "0"
