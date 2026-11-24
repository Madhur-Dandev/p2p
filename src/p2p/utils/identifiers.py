"""Identifier generation and strict validation helpers."""

from __future__ import annotations

import uuid

from p2p.exceptions import ValidationError


def generate_peer_id() -> str:
    """Return a newly generated canonical lowercase UUIDv4 string for a peer."""
    return str(uuid.uuid4())


def validate_peer_id(value: str) -> str:
    """Validate and normalize a canonical 36-character UUID peer identifier."""
    if not isinstance(value, str):
        raise ValidationError("peer_id must be a string")
    if len(value) != 36:
        raise ValidationError(f"peer_id must be 36 characters, got {len(value)}")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise ValidationError(f"Invalid UUID peer_id: {value!r}") from exc
    normalized = str(parsed)
    if normalized != value.lower():
        raise ValidationError(f"peer_id must use canonical hyphenated UUID format: {value!r}")
    return normalized


def is_valid_peer_id(value: str) -> bool:
    """Return True if value is a valid canonical UUID peer identifier."""
    try:
        validate_peer_id(value)
    except ValidationError:
        return False
    return True


def generate_request_id() -> bytes:
    """Return a fresh 16-byte binary UUIDv4 request correlation identifier."""
    return uuid.uuid4().bytes


def request_id_to_str(request_id: bytes) -> str:
    """Format a 16-byte binary request identifier as a canonical UUID string."""
    if not isinstance(request_id, bytes) or len(request_id) != 16:
        raise ValidationError("request_id must be exactly 16 bytes")
    return str(uuid.UUID(bytes=request_id))


def parse_request_id(value: str) -> bytes:
    """Parse a canonical UUID string into a 16-byte binary request identifier."""
    normalized = validate_peer_id(value)
    return uuid.UUID(normalized).bytes


def generate_transfer_id() -> str:
    """Return a fresh canonical lowercase UUIDv4 string for local transfer tracking."""
    return str(uuid.uuid4())
