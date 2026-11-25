"""Unit tests for the transfer state machine and progress reporting."""

from __future__ import annotations

import pytest

from p2p.exceptions import TransferError
from p2p.transfer.progress import TransferProgress, TransferState, TransferStateMachine


@pytest.mark.unit
def test_legal_and_illegal_transfer_state_transitions() -> None:
    sm = TransferStateMachine()
    assert sm.state == TransferState.QUEUED

    assert sm.transition(TransferState.CONNECTING) == TransferState.CONNECTING
    assert sm.transition(TransferState.REQUESTING) == TransferState.REQUESTING
    assert sm.transition(TransferState.TRANSFERRING) == TransferState.TRANSFERRING
    assert sm.transition(TransferState.VERIFYING) == TransferState.VERIFYING
    assert sm.transition(TransferState.COMPLETED) == TransferState.COMPLETED

    # Cannot transition out of COMPLETED
    with pytest.raises(TransferError, match="Illegal transfer state transition"):
        sm.transition(TransferState.TRANSFERRING)

    # Cannot skip directly from QUEUED to COMPLETED
    sm2 = TransferStateMachine()
    with pytest.raises(TransferError, match="Illegal transfer state transition"):
        sm2.transition(TransferState.COMPLETED)

    # Interrupted state allows resuming via CONNECTING
    sm3 = TransferStateMachine()
    sm3.transition(TransferState.CONNECTING)
    sm3.transition(TransferState.REQUESTING)
    sm3.transition(TransferState.TRANSFERRING)
    sm3.transition(TransferState.TRANSFER_INTERRUPTED)
    assert sm3.transition(TransferState.CONNECTING) == TransferState.CONNECTING


@pytest.mark.unit
def test_transfer_progress_percent_computation() -> None:
    prog = TransferProgress(
        transfer_id="t1",
        file_id="f1",
        file_name="data.bin",
        state=TransferState.TRANSFERRING,
        bytes_transferred=500,
        total_bytes=1000,
        elapsed_sec=2.0,
        throughput_mbps=0.002,
    )
    assert prog.percent == 50.0

    empty_done = TransferProgress(
        transfer_id="t2",
        file_id="f2",
        file_name="empty.bin",
        state=TransferState.COMPLETED,
        bytes_transferred=0,
        total_bytes=0,
        elapsed_sec=0.1,
        throughput_mbps=0.0,
    )
    assert empty_done.percent == 100.0
