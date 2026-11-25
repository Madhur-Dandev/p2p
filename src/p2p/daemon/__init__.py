"""Background P2P daemon lifecycle, PID lock, and Unix domain socket IPC."""

from p2p.daemon.ipc_client import call_ipc, is_daemon_running, send_ipc_request
from p2p.daemon.ipc_protocol import (
    IPC_MAGIC,
    IPC_VERSION,
    pack_ipc_message,
    read_ipc_message,
    write_ipc_message,
)
from p2p.daemon.ipc_server import IpcServer
from p2p.daemon.pidfile import PidFileLock, is_process_alive, read_pid
from p2p.daemon.service import (
    DaemonRuntimeOptions,
    P2PDaemon,
    spawn_daemon_process,
    stop_daemon_process,
)

__all__ = [
    "IPC_MAGIC",
    "IPC_VERSION",
    "DaemonRuntimeOptions",
    "IpcServer",
    "P2PDaemon",
    "PidFileLock",
    "call_ipc",
    "is_daemon_running",
    "is_process_alive",
    "pack_ipc_message",
    "read_ipc_message",
    "read_pid",
    "send_ipc_request",
    "spawn_daemon_process",
    "stop_daemon_process",
    "write_ipc_message",
]
