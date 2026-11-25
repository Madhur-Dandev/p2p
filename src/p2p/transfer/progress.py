"""Transfer state machine and progress reporting models."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from p2p.exceptions import TransferError


class TransferState(StrEnum):
    """Explicit lifecycle states for a peer-to-peer file transfer."""

    QUEUED = "queued"
    CONNECTING = "connecting"
    REQUESTING = "requesting"
    TRANSFERRING = "transferring"
    VERIFYING = "verifying"
    COMPLETED = "completed"

    CONNECTION_FAILED = "connection_failed"
    SOURCE_UNAVAILABLE = "source_unavailable"
    TRANSFER_INTERRUPTED = "transfer_interrupted"
    INTEGRITY_FAILED = "integrity_failed"
    CANCELLED = "cancelled"


TERMINAL_TRANSFER_STATES: frozenset[TransferState] = frozenset(
    {
        TransferState.COMPLETED,
        TransferState.CONNECTION_FAILED,
        TransferState.SOURCE_UNAVAILABLE,
        TransferState.TRANSFER_INTERRUPTED,
        TransferState.INTEGRITY_FAILED,
        TransferState.CANCELLED,
    }
)

LEGAL_TRANSITIONS: dict[TransferState, frozenset[TransferState]] = {
    TransferState.QUEUED: frozenset(
        {
            TransferState.CONNECTING,
            TransferState.CONNECTION_FAILED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.CONNECTING: frozenset(
        {
            TransferState.REQUESTING,
            TransferState.CONNECTION_FAILED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.REQUESTING: frozenset(
        {
            TransferState.TRANSFERRING,
            TransferState.SOURCE_UNAVAILABLE,
            TransferState.TRANSFER_INTERRUPTED,
            TransferState.CONNECTION_FAILED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.TRANSFERRING: frozenset(
        {
            TransferState.VERIFYING,
            TransferState.TRANSFER_INTERRUPTED,
            TransferState.SOURCE_UNAVAILABLE,
            TransferState.CANCELLED,
        }
    ),
    TransferState.VERIFYING: frozenset(
        {
            TransferState.COMPLETED,
            TransferState.INTEGRITY_FAILED,
            TransferState.CANCELLED,
        }
    ),
    TransferState.COMPLETED: frozenset(),
    TransferState.CONNECTION_FAILED: frozenset(),
    TransferState.SOURCE_UNAVAILABLE: frozenset(),
    TransferState.TRANSFER_INTERRUPTED: frozenset({TransferState.CONNECTING}),
    TransferState.INTEGRITY_FAILED: frozenset(),
    TransferState.CANCELLED: frozenset({TransferState.CONNECTING}),
}


class TransferStateMachine:
    """Enforces legal transfer state transitions."""

    def __init__(self, initial_state: TransferState = TransferState.QUEUED) -> None:
        self._state = initial_state

    @property
    def state(self) -> TransferState:
        """Return the current transfer state."""
        return self._state

    def transition(self, new_state: TransferState) -> TransferState:
        """Transition to new_state or raise TransferError if illegal."""
        allowed = LEGAL_TRANSITIONS[self._state]
        if new_state not in allowed:
            raise TransferError(
                f"Illegal transfer state transition: {self._state.value} -> {new_state.value}"
            )
        self._state = new_state
        return self._state


@dataclass(frozen=True, slots=True)
class TransferProgress:
    """Snapshot of download progress emitted during streaming transfers."""

    transfer_id: str
    file_id: str
    file_name: str
    state: TransferState
    bytes_transferred: int
    total_bytes: int
    elapsed_sec: float
    throughput_mbps: float

    @property
    def percent(self) -> float:
        """Return completion percentage in [0.0, 100.0]."""
        if self.total_bytes <= 0:
            return 100.0 if self.state == TransferState.COMPLETED else 0.0
        return min(100.0, (self.bytes_transferred / self.total_bytes) * 100.0)


ProgressCallback = Callable[[TransferProgress], None]
