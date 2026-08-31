"""Exact money conversion for authorization contracts."""

from __future__ import annotations

from decimal import ROUND_CEILING, Decimal, InvalidOperation


def usd_to_minor_units(value: object) -> int:
    """Convert a positive, finite USD ceiling with exact cent precision."""

    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("USD ceiling must be a finite decimal value") from error
    if not amount.is_finite():
        raise ValueError("USD ceiling must be finite")
    if amount <= 0:
        raise ValueError("USD ceiling must be positive")
    minor_units = amount * 100
    if minor_units != minor_units.to_integral_value():
        raise ValueError("USD ceiling must resolve to whole minor units")
    converted = int(minor_units)
    if converted <= 0:
        raise ValueError("USD ceiling must authorize at least one minor unit")
    return converted


def usd_ceiling_to_minor_units(value: object) -> int:
    """Round a computed non-negative USD authorization ceiling upward."""

    try:
        amount = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ValueError("computed USD ceiling must be a finite decimal value") from error
    if not amount.is_finite():
        raise ValueError("computed USD ceiling must be finite")
    if amount < 0:
        raise ValueError("computed USD ceiling cannot be negative")
    return int((amount * 100).to_integral_value(rounding=ROUND_CEILING))
