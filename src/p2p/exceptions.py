"""Domain-specific exception hierarchy for the P2P file-sharing system."""

from __future__ import annotations


class P2PError(Exception):
    """Base exception for all application-level errors."""


class ConfigError(P2PError):
    """Raised when configuration loading, serialization, or validation fails."""


class ValidationError(P2PError):
    """Raised when an identifier, field, or user input fails validation."""


class SecurityError(P2PError):
    """Raised when a security constraint or invariant is violated."""


class PathValidationError(SecurityError):
    """Raised when a path escapes authorized directories or traverses symlinks."""


class ResourceLimitError(SecurityError):
    """Raised when a configured resource ceiling (connections, size, queue) is exceeded."""


class ProtocolError(P2PError):
    """Base exception for wire protocol parsing and semantic failures."""


class FramingError(ProtocolError):
    """Raised when a binary frame header or length field is invalid or truncated."""


class UnsupportedVersionError(ProtocolError):
    """Raised when an incoming frame declares an unsupported protocol version."""


class MalformedMessageError(ProtocolError):
    """Raised when a frame payload cannot be decoded or violates message schema."""


class StorageError(P2PError):
    """Raised when SQLite persistence or migration fails."""


class DiscoveryError(P2PError):
    """Raised when peer discovery transport or registration encounters a failure."""


class PeerNotFoundError(P2PError):
    """Raised when a target peer cannot be found in the registry."""


class FileNotFoundInIndexError(P2PError):
    """Raised when a requested file ID does not exist in the authorized index."""


class TransferError(P2PError):
    """Base exception for file upload and download failures."""


class IntegrityError(TransferError):
    """Raised when a transferred file's SHA-256 digest mismatches the expected hash."""


class TransferTimeoutError(TransferError):
    """Raised when a peer transfer stalls beyond the configured idle timeout."""


class TransferCancelledError(TransferError):
    """Raised when a transfer is cancelled by user request or daemon shutdown."""


class DaemonError(P2PError):
    """Base exception for background daemon lifecycle and IPC errors."""


class DaemonNotRunningError(DaemonError):
    """Raised when a CLI command requires an active daemon that is not running."""


class DaemonAlreadyRunningError(DaemonError):
    """Raised when attempting to start a daemon while another instance holds the PID lock."""
