"""Unit tests for peer, request, and transfer identifier helpers."""

from __future__ import annotations

import uuid

import pytest

from p2p.exceptions import ValidationError
from p2p.utils.identifiers import (
    generate_peer_id,
    generate_request_id,
    generate_transfer_id,
    is_valid_peer_id,
    parse_request_id,
    request_id_to_str,
    validate_peer_id,
)


@pytest.mark.unit
def test_generate_peer_id_returns_distinct_canonical_uuids() -> None:
    first = generate_peer_id()
    second = generate_peer_id()

    assert first != second
    assert len(first) == 36
    assert uuid.UUID(first).version == 4
    assert is_valid_peer_id(first)


@pytest.mark.unit
def test_validate_peer_id_normalizes_case_and_rejects_malformed() -> None:
    upper = "A1B2C3D4-E5F6-4789-ABCD-0123456789EF"
    assert validate_peer_id(upper) == upper.lower()

    invalid_inputs = [
        "",
        "not-a-uuid",
        "a1b2c3d4e5f64789abcd0123456789ef",  # 32 hex without hyphens
        "{a1b2c3d4-e5f6-4789-abcd-0123456789ef}",
        " a1b2c3d4-e5f6-4789-abcd-0123456789ef",
    ]
    for candidate in invalid_inputs:
        assert not is_valid_peer_id(candidate)
        with pytest.raises(ValidationError):
            validate_peer_id(candidate)


@pytest.mark.unit
def test_request_id_round_trip_and_length_validation() -> None:
    raw = generate_request_id()
    assert isinstance(raw, bytes)
    assert len(raw) == 16

    rendered = request_id_to_str(raw)
    assert parse_request_id(rendered) == raw

    with pytest.raises(ValidationError):
        request_id_to_str(b"short")
    with pytest.raises(ValidationError):
        request_id_to_str(b"x" * 17)


@pytest.mark.unit
def test_generate_transfer_id_is_valid_uuid4() -> None:
    transfer_id = generate_transfer_id()
    parsed = uuid.UUID(transfer_id)
    assert parsed.version == 4
    assert str(parsed) == transfer_id
