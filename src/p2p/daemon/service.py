"""Long-running background P2P node daemon and process-management helpers."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from p2p.config import AppConfig, load_config
from p2p.constants import DEFAULT_BIND_HOST, MAX_SEARCH_RESULTS_PER_PEER
from p2p.daemon.ipc_client import call_ipc, is_daemon_running, send_ipc_request
from p2p.daemon.ipc_server import IpcServer
from p2p.daemon.pidfile import PidFileLock, is_process_alive, read_pid
from p2p.exceptions import (
    DaemonAlreadyRunningError,
    DaemonError,
    DaemonNotRunningError,
    PeerNotFoundError,
    ValidationError,
)
from p2p.indexing.file_index import FileIndexer
from p2p.logging_config import get_context_logger, read_recent_logs
from p2p.network.discovery import DiscoveryService
from p2p.network.peer_registry import PeerInfo, PeerRegistry, PeerStatus
from p2p.network.tcp_server import TcpServer
from p2p.search.distributed_search import DistributedSearchService
from p2p.search.local_search import LocalSearchService
from p2p.storage.database import Database
from p2p.storage.repositories import FileIndexRepository, PeerRepository
from p2p.transfer.transfer_manager import TransferManager
from p2p.utils.time_utils import monotonic_now


@dataclass(frozen=True, slots=True)
class DaemonRuntimeOptions:
    """Optional runtime overrides applied when starting a daemon process or node."""

    bind_host: str | None = None
    tcp_port: int | None = None
    udp_port: int | None = None
    broadcast_enabled: bool | None = None
    udp_targets: tuple[tuple[str, int], ...] = ()
    static_peers: tuple[str, ...] = ()
    heartbeat_interval_sec: float | None = None
    stale_timeout_sec: float | None = None
    remove_timeout_sec: float | None = None


def _resolve_dir_sync(raw: str) -> Path:
    return Path(raw).expanduser().resolve(strict=False)


def _unlink_if_exists_sync(path: Path) -> None:
    with contextlib.suppress(OSError):
        path.unlink(missing_ok=True)


class P2PDaemon:
    """Orchestrates discovery, TCP server, indexing, search, transfers, and Unix IPC."""

    def __init__(
        self,
        config: AppConfig,
        *,
        options: DaemonRuntimeOptions | None = None,
    ) -> None:
        self._config = config
        self._options = options or DaemonRuntimeOptions()
        self._pid_lock = PidFileLock(config.daemon.pid_path)
        self._db: Database | None = None
        self._indexer: FileIndexer | None = None
        self._index_lock = asyncio.Lock()
        self._registry: PeerRegistry | None = None
        self._local_search: LocalSearchService | None = None
        self._distributed_search: DistributedSearchService | None = None
        self._transfer_manager: TransferManager | None = None
        self._tcp_server: TcpServer | None = None
        self._discovery: DiscoveryService | None = None
        self._ipc_server: IpcServer | None = None
        self._stop_event = asyncio.Event()
        self._started_mono: float | None = None
        self._pid: int | None = None
        self._running = False
        self._logger = get_context_logger("daemon.service", peer_id=config.node.peer_id)

    @property
    def is_running(self) -> bool:
        """Return True while all core daemon services are active."""
        return self._running

    @property
    def tcp_port(self) -> int:
        """Return bound TCP listening port."""
        if self._tcp_server is None:
            raise DaemonNotRunningError("Daemon TCP server is not started")
        return self._tcp_server.bound_port

    @property
    def udp_port(self) -> int:
        """Return bound UDP discovery port."""
        if self._discovery is None:
            raise DaemonNotRunningError("Daemon discovery service is not started")
        return self._discovery.bound_port

    @property
    def socket_path(self) -> Path:
        """Return configured Unix domain socket path."""
        return self._config.daemon.socket_path

    async def __aenter__(self) -> P2PDaemon:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    async def start(self) -> None:
        """Acquire PID lock and start storage, indexer, network, and IPC subsystems."""
        if self._running:
            return

        self._pid = await asyncio.to_thread(self._pid_lock.acquire)
        self._stop_event.clear()

        try:
            db = Database(self._config.storage.database_path)
            await asyncio.to_thread(db.initialize)
            self._db = db

            indexer = FileIndexer(
                db,
                shared_directories=self._config.sharing.directories,
                excluded_dirs=self._config.sharing.excluded_dirs,
                follow_symlinks=self._config.sharing.follow_symlinks,
                hash_chunk_size=self._config.transfer.chunk_size_bytes,
                max_filename_length=self._config.limits.max_filename_length,
            )
            self._indexer = indexer
            if self._config.sharing.directories:
                async with self._index_lock:
                    await indexer.rebuild_async()

            opts = self._options
            bind_host = (
                opts.bind_host if opts.bind_host is not None else self._config.network.bind_host
            )
            tcp_port = opts.tcp_port if opts.tcp_port is not None else self._config.network.tcp_port
            udp_port = (
                opts.udp_port if opts.udp_port is not None else self._config.discovery.udp_port
            )
            broadcast_enabled = (
                opts.broadcast_enabled
                if opts.broadcast_enabled is not None
                else self._config.discovery.enabled
            )
            heartbeat = (
                opts.heartbeat_interval_sec
                if opts.heartbeat_interval_sec is not None
                else self._config.discovery.heartbeat_interval_sec
            )
            stale_timeout = (
                opts.stale_timeout_sec
                if opts.stale_timeout_sec is not None
                else self._config.discovery.stale_timeout_sec
            )
            remove_timeout = (
                opts.remove_timeout_sec
                if opts.remove_timeout_sec is not None
                else max(self._config.discovery.remove_timeout_sec, stale_timeout)
            )
            static_peers = list(self._config.discovery.static_peers) + list(opts.static_peers)

            local_search = LocalSearchService(
                db,
                peer_id=self._config.node.peer_id,
                max_query_length=self._config.limits.max_query_length,
            )
            self._local_search = local_search

            transfer_manager = TransferManager(self._config, db, search_service=local_search)
            self._transfer_manager = transfer_manager

            registry = PeerRegistry(
                local_peer_id=self._config.node.peer_id,
                stale_timeout_sec=stale_timeout,
                remove_timeout_sec=remove_timeout,
            )
            self._registry = registry

            peer_repo = PeerRepository(db)

            def _on_peer_event(_event: str, peer: PeerInfo) -> None:
                if peer.status == PeerStatus.REMOVED:
                    peer_repo.update_status(peer.peer_id, PeerStatus.REMOVED)
                else:
                    peer_repo.upsert_peer(peer)

            tcp_server = TcpServer(
                host=bind_host,
                port=tcp_port,
                max_connections=self._config.limits.max_simultaneous_connections,
                shutdown_timeout_sec=self._config.daemon.shutdown_timeout_sec,
                control_timeout_sec=self._config.network.control_timeout_sec,
                max_payload_bytes=self._config.limits.max_control_payload_bytes,
                max_data_chunk_bytes=self._config.limits.max_data_chunk_bytes,
                peer_id=self._config.node.peer_id,
                display_name=self._config.node.display_name,
                handler=transfer_manager.serve_connection,
            )
            await tcp_server.start()
            self._tcp_server = tcp_server
            transfer_manager.upload_service.set_tcp_port(tcp_server.bound_port)

            advertise_ip = self._config.network.advertise_host or (
                "127.0.0.1" if bind_host in {DEFAULT_BIND_HOST, ""} else bind_host
            )
            distributed_search = DistributedSearchService(
                local_peer_id=self._config.node.peer_id,
                tcp_client=transfer_manager.tcp_client,
                registry=registry,
                local_search=local_search,
                display_name=self._config.node.display_name,
                local_ip=advertise_ip,
                local_tcp_port=tcp_server.bound_port,
                search_timeout_sec=self._config.transfer.search_timeout_sec,
                max_query_length=self._config.limits.max_query_length,
                max_concurrent_queries=self._config.limits.max_simultaneous_connections,
            )
            self._distributed_search = distributed_search

            discovery = DiscoveryService(
                local_peer_id=self._config.node.peer_id,
                display_name=self._config.node.display_name,
                tcp_port=tcp_server.bound_port,
                registry=registry,
                bind_host=bind_host,
                udp_port=udp_port,
                broadcast_address=self._config.discovery.broadcast_address,
                broadcast_enabled=broadcast_enabled,
                heartbeat_interval_sec=heartbeat,
                static_peers=static_peers,
                discovery_targets=list(opts.udp_targets),
                tcp_client=transfer_manager.tcp_client,
                on_event=_on_peer_event,
            )
            await discovery.start()
            self._discovery = discovery

            ipc_server = IpcServer(
                self._config.daemon.socket_path,
                handler=self._handle_ipc_request,
            )
            await ipc_server.start()
            self._ipc_server = ipc_server

            self._started_mono = monotonic_now()
            self._running = True
            self._logger.info(
                "P2P daemon started (pid=%d, tcp=%s:%d, udp=%d, ipc=%s)",
                self._pid,
                bind_host,
                tcp_server.bound_port,
                discovery.bound_port,
                ipc_server.socket_path,
            )
        except Exception:
            await self.stop()
            raise

    async def stop(self) -> None:
        """Gracefully stop transfers, IPC, discovery, TCP listener, SQLite, and PID lock."""
        self._running = False
        self._stop_event.set()

        if self._ipc_server is not None:
            with contextlib.suppress(Exception):
                await self._ipc_server.stop()
            self._ipc_server = None

        if self._transfer_manager is not None:
            self._transfer_manager.cancel_all()

        if self._discovery is not None:
            with contextlib.suppress(Exception):
                await self._discovery.stop()
            self._discovery = None

        if self._tcp_server is not None:
            with contextlib.suppress(Exception):
                await self._tcp_server.stop()
            self._tcp_server = None

        if self._db is not None:
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self._db.close)
            self._db = None

        await asyncio.to_thread(self._pid_lock.release)
        self._pid = None
        self._logger.info("P2P daemon stopped cleanly")

    async def run_forever(self) -> None:
        """Start daemon, register OS signal handlers, wait until stop, and shut down cleanly."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.request_stop)
        try:
            async with self:
                await self._stop_event.wait()
        finally:
            for sig in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError):
                    loop.remove_signal_handler(sig)

    def request_stop(self) -> None:
        """Signal the running daemon loop to shut down cleanly."""
        self._stop_event.set()

    async def _build_status_payload(self) -> dict[str, Any]:
        db = self._db
        registry = self._registry
        transfer_manager = self._transfer_manager
        tcp_server = self._tcp_server
        discovery = self._discovery
        ipc_server = self._ipc_server
        if (
            db is None
            or registry is None
            or transfer_manager is None
            or tcp_server is None
            or discovery is None
            or ipc_server is None
        ):
            raise DaemonNotRunningError("Daemon services are not active")

        indexed_count = await db.run(FileIndexRepository(db).count_files)
        all_peers = registry.list_peers(include_stale=True)
        active_peers = sum(1 for p in all_peers if p.status == PeerStatus.ACTIVE)
        stale_peers = sum(1 for p in all_peers if p.status == PeerStatus.STALE)
        uptime = (
            0.0 if self._started_mono is None else max(0.0, monotonic_now() - self._started_mono)
        )
        u_stats = transfer_manager.upload_service.stats

        return {
            "running": self._running,
            "pid": self._pid or os.getpid(),
            "peer_id": self._config.node.peer_id,
            "display_name": self._config.node.display_name,
            "uptime_sec": uptime,
            "tcp_port": tcp_server.bound_port,
            "udp_port": discovery.bound_port,
            "indexed_files": indexed_count,
            "active_peers": active_peers,
            "stale_peers": stale_peers,
            "total_peers": len(all_peers),
            "active_downloads": transfer_manager.active_download_count,
            "upload_stats": asdict(u_stats),
            "database_path": str(self._config.storage.database_path),
            "socket_path": str(ipc_server.socket_path),
        }

    async def _handle_ipc_request(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        db = self._db
        indexer = self._indexer
        registry = self._registry
        distributed_search = self._distributed_search
        transfer_manager = self._transfer_manager
        if (
            db is None
            or indexer is None
            or registry is None
            or distributed_search is None
            or transfer_manager is None
        ):
            raise DaemonNotRunningError("Daemon services are not active")

        if action == "status":
            return await self._build_status_payload()

        if action == "stop":
            asyncio.get_running_loop().call_soon(self._stop_event.set)
            return {"stopping": True, "pid": self._pid or os.getpid()}

        if action == "peers":
            include_stale = bool(params.get("include_stale", True))
            peers = registry.list_peers(include_stale=include_stale)
            return {
                "peers": [
                    {
                        "peer_id": p.peer_id,
                        "display_name": p.display_name,
                        "ip_address": p.ip_address,
                        "tcp_port": p.tcp_port,
                        "udp_port": p.udp_port,
                        "status": str(p.status),
                        "source": p.source,
                        "last_seen_utc": p.last_seen_utc,
                    }
                    for p in peers
                ],
                "total": len(peers),
            }

        if action == "index_rebuild":
            force_rehash = bool(params.get("force_rehash", False))
            async with self._index_lock:
                summary = await indexer.rebuild_async(force_rehash=force_rehash)
            return asdict(summary)

        if action == "share_reload":
            raw_dirs = params.get("directories")
            if isinstance(raw_dirs, list):
                new_dirs = [_resolve_dir_sync(str(d)) for d in raw_dirs]
            elif self._config.config_path is not None:
                reloaded = await asyncio.to_thread(load_config, self._config.config_path)
                new_dirs = reloaded.sharing.directories
            else:
                new_dirs = list(self._config.sharing.directories)

            self._config.sharing.directories = list(new_dirs)
            transfer_manager.update_shared_directories(new_dirs)
            new_indexer = FileIndexer(
                db,
                shared_directories=new_dirs,
                excluded_dirs=self._config.sharing.excluded_dirs,
                follow_symlinks=self._config.sharing.follow_symlinks,
                hash_chunk_size=self._config.transfer.chunk_size_bytes,
                max_filename_length=self._config.limits.max_filename_length,
            )
            self._indexer = new_indexer
            async with self._index_lock:
                summary = await new_indexer.rebuild_async()
            return asdict(summary)

        if action == "search":
            query = str(params.get("query", ""))
            exact = bool(params.get("exact", False))
            max_results = int(params.get("max_results", MAX_SEARCH_RESULTS_PER_PEER))
            include_local = bool(params.get("include_local", False))
            include_stale = bool(params.get("include_stale", False))
            raw_timeout = params.get("timeout_sec")
            timeout_sec = float(raw_timeout) if raw_timeout is not None else None

            report = await distributed_search.search(
                query,
                exact=exact,
                max_results=max_results,
                include_local=include_local,
                include_stale=include_stale,
                timeout_sec=timeout_sec,
            )
            return {
                "query": report.query,
                "peers_queried": report.peers_queried,
                "peers_succeeded": report.peers_succeeded,
                "peers_failed": report.peers_failed,
                "any_truncated": report.any_truncated,
                "duration_sec": report.duration_sec,
                "results": [
                    {
                        "file_id": item.primary_file_id,
                        "file_name": item.file_name,
                        "file_size": item.file_size,
                        "sha256": item.sha256,
                        "source_count": item.source_count,
                        "sources": [
                            {
                                "peer_id": src.peer_id,
                                "display_name": src.display_name,
                                "ip_address": src.ip_address,
                                "tcp_port": src.tcp_port,
                                "file_id": src.file_id,
                                "file_name": src.file_name,
                                "indexed_at": src.indexed_at,
                            }
                            for src in item.sources
                        ],
                    }
                    for item in report.results
                ],
                "peer_statuses": [asdict(st) for st in report.peer_statuses],
            }

        if action == "download":
            file_id = str(params.get("file_id", ""))
            host = params.get("host")
            port = params.get("port")
            raw_output_dir = params.get("output_dir")
            dl_dir = _resolve_dir_sync(str(raw_output_dir)) if raw_output_dir else None
            resume_flag = bool(params.get("resume", False))
            restart_if_changed = bool(params.get("restart_if_changed", False))

            if resume_flag:
                res = await transfer_manager.resume_download(
                    file_id,
                    host=str(host) if host is not None else None,
                    port=int(port) if port is not None else None,
                    download_dir=dl_dir,
                    restart_if_changed=restart_if_changed,
                )
            elif host is not None and port is not None:
                res = await transfer_manager.download_file(
                    host=str(host),
                    port=int(port),
                    file_id=file_id,
                    download_dir=dl_dir,
                )
            else:
                cached_sources = distributed_search.resolve_file_id(file_id)
                if not cached_sources:
                    raise PeerNotFoundError(
                        f"Unknown source peer for file_id={file_id!r}; "
                        "run 'p2p search <query>' first or pass --peer HOST:PORT"
                    )
                res = await transfer_manager.download_with_failover(
                    cached_sources,
                    download_dir=dl_dir,
                )

            return {
                "transfer_id": res.transfer_id,
                "file_id": res.file_id,
                "file_name": res.file_name,
                "destination_path": str(res.destination_path),
                "bytes_downloaded": res.bytes_downloaded,
                "expected_sha256": res.expected_sha256,
                "actual_sha256": res.actual_sha256,
                "state": str(res.state),
                "duration_sec": res.duration_sec,
                "throughput_mbps": res.throughput_mbps,
                "resumed": res.resumed,
                "resume_offset": res.resume_offset,
            }

        if action == "resume":
            identifier = str(params.get("identifier", ""))
            host = params.get("host")
            port = params.get("port")
            raw_output_dir = params.get("output_dir")
            dl_dir = _resolve_dir_sync(str(raw_output_dir)) if raw_output_dir else None
            restart_if_changed = bool(params.get("restart_if_changed", False))
            res = await transfer_manager.resume_download(
                identifier,
                host=str(host) if host is not None else None,
                port=int(port) if port is not None else None,
                download_dir=dl_dir,
                restart_if_changed=restart_if_changed,
            )
            return {
                "transfer_id": res.transfer_id,
                "file_id": res.file_id,
                "file_name": res.file_name,
                "destination_path": str(res.destination_path),
                "bytes_downloaded": res.bytes_downloaded,
                "expected_sha256": res.expected_sha256,
                "actual_sha256": res.actual_sha256,
                "state": str(res.state),
                "duration_sec": res.duration_sec,
                "throughput_mbps": res.throughput_mbps,
                "resumed": res.resumed,
                "resume_offset": res.resume_offset,
            }

        if action == "downloads":
            limit = int(params.get("limit", 50))
            records = await transfer_manager.list_history(limit=limit)
            checkpoints = await transfer_manager.list_checkpoints()
            return {
                "downloads": [asdict(rec) for rec in records],
                "checkpoints": {tid: asdict(cp) for tid, cp in checkpoints.items()},
            }

        if action == "cancel":
            cancel_all_flag = bool(params.get("all", False))
            identifier = params.get("identifier")
            if cancel_all_flag or not identifier:
                count = transfer_manager.active_download_count
                transfer_manager.cancel_all()
                return {"cancelled": True, "count": count}
            ok = transfer_manager.cancel_transfer(str(identifier))
            return {"cancelled": ok, "count": 1 if ok else 0}

        raise ValidationError(f"Unknown daemon IPC action: {action!r}")


def _build_daemon_subprocess_cmd(
    *,
    config_path: Path | None,
    log_level: str | None,
    options: DaemonRuntimeOptions,
) -> list[str]:
    cmd: list[str] = [sys.executable, "-m", "p2p"]
    if config_path is not None:
        cmd.extend(["--config", str(config_path)])
    if log_level is not None:
        cmd.extend(["--log-level", log_level])
    cmd.append("_daemon-run")
    if options.bind_host is not None:
        cmd.extend(["--host", options.bind_host])
    if options.tcp_port is not None:
        cmd.extend(["--tcp-port", str(options.tcp_port)])
    if options.udp_port is not None:
        cmd.extend(["--udp-port", str(options.udp_port)])
    if options.broadcast_enabled is False:
        cmd.append("--no-broadcast")
    for target_host, target_port in options.udp_targets:
        cmd.extend(["--udp-target", f"{target_host}:{target_port}"])
    for peer in options.static_peers:
        cmd.extend(["--static-peer", peer])
    if options.heartbeat_interval_sec is not None:
        cmd.extend(["--heartbeat", str(options.heartbeat_interval_sec)])
    if options.stale_timeout_sec is not None:
        cmd.extend(["--stale-timeout", str(options.stale_timeout_sec)])
    return cmd


def spawn_daemon_process(
    *,
    config_path: Path | None = None,
    log_level: str | None = None,
    options: DaemonRuntimeOptions | None = None,
    wait_timeout_sec: float = 10.0,
) -> dict[str, Any]:
    """Launch the daemon as a detached background process and wait until IPC responds."""
    cfg = load_config(config_path)
    if is_daemon_running(cfg.daemon.socket_path):
        existing_pid = read_pid(cfg.daemon.pid_path)
        raise DaemonAlreadyRunningError(
            f"P2P daemon is already running (PID {existing_pid or 'active'})"
        )

    opts = options or DaemonRuntimeOptions()
    resolved_cfg_path = cfg.config_path or config_path
    cmd = _build_daemon_subprocess_cmd(
        config_path=resolved_cfg_path,
        log_level=log_level,
        options=opts,
    )

    env = os.environ.copy()
    src_root = str(Path(__file__).resolve().parents[2])
    existing_pp = env.get("PYTHONPATH", "")
    if src_root not in existing_pp.split(os.pathsep):
        env["PYTHONPATH"] = f"{src_root}{os.pathsep}{existing_pp}" if existing_pp else src_root

    proc = subprocess.Popen(  # noqa: S603
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        start_new_session=True,
    )

    deadline = monotonic_now() + wait_timeout_sec
    while monotonic_now() < deadline:
        rc = proc.poll()
        if rc is not None:
            recent = read_recent_logs(cfg.logging.file_path, lines=10)
            tail = f" Recent logs: {' | '.join(recent[-3:])}" if recent else ""
            raise DaemonError(f"Daemon failed to start (exit code {rc}).{tail}")

        try:
            return asyncio.run(send_ipc_request(cfg.daemon.socket_path, "status", timeout_sec=0.5))
        except DaemonError:
            time.sleep(0.05)

    with contextlib.suppress(OSError):
        proc.terminate()
    raise DaemonError(f"Timed out waiting {wait_timeout_sec:.1f}s for daemon IPC readiness")


def stop_daemon_process(
    socket_path: Path,
    pid_path: Path,
    *,
    timeout_sec: float = 10.0,
) -> int | None:
    """Stop the running background daemon cleanly via IPC and wait for process exit."""
    status = call_ipc(socket_path, "status", timeout_sec=3.0)
    target_pid = int(status.get("pid", 0)) or read_pid(pid_path)

    call_ipc(socket_path, "stop", timeout_sec=3.0)
    deadline = monotonic_now() + timeout_sec
    while monotonic_now() < deadline:
        sock_gone = not socket_path.exists()
        proc_gone = target_pid is None or not is_process_alive(target_pid)
        if sock_gone and proc_gone:
            return target_pid
        time.sleep(0.05)

    raise DaemonError(f"Timed out waiting for daemon PID {target_pid} to stop")
