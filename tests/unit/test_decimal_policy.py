from decimal import Decimal

import pytest

from auto_execution_engine.common.decimal_policy import (
    DecimalPolicyError,
    canonical_decimal_string,
    decimals_equal,
    parse_decimal,
    quantize_money,
    quantize_price,
    quantize_quantity,
)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (Decimal("12.3400"), Decimal("12.3400")),
        (" 12.3400 ", Decimal("12.3400")),
        (15, Decimal("15")),
        (1.25, Decimal("1.25")),
    ],
)
def test_parse_decimal_accepts_supported_inputs(raw_value, expected) -> None:
    assert parse_decimal(raw_value) == expected


@pytest.mark.parametrize("raw_value", ["", "abc", float("inf"), float("nan")])
def test_parse_decimal_rejects_invalid_inputs(raw_value) -> None:
    with pytest.raises(DecimalPolicyError):
        parse_decimal(raw_value)


def test_quantize_money_uses_bankers_rounding() -> None:
    assert quantize_money("12.345") == Decimal("12.34")
    assert quantize_money("12.355") == Decimal("12.36")


def test_quantize_price_uses_repository_default_tick_precision() -> None:
    assert quantize_price("60250.55555") == Decimal("60250.5556")


def test_quantize_quantity_uses_repository_default_lot_precision() -> None:
    assert quantize_quantity("0.123456789") == Decimal("0.12345679")


def test_canonical_decimal_string_removes_non_significant_trailing_zeroes() -> None:
    assert canonical_decimal_string("12.3400") == "12.34"
    assert canonical_decimal_string(Decimal("0.00000000")) == "0"


def test_decimals_equal_compares_canonically_across_supported_inputs() -> None:
    assert decimals_equal(left="1.2300", right=1.23) is True
    assert decimals_equal(left="1.2301", right="1.23") is False
