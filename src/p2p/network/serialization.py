"""Safe JSON and binary payload serialization/deserialization for wire frames."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from p2p.constants import (
    FLAG_END_OF_STREAM,
    FLAG_NONE,
    MAX_CONTROL_PAYLOAD_BYTES,
    MAX_DATA_CHUNK_BYTES,
    MAX_FILE_SIZE_BYTES,
    MAX_FILENAME_LENGTH,
    MAX_QUERY_LENGTH,
    MAX_SEARCH_RESULTS_PER_PEER,
)
from p2p.exceptions import MalformedMessageError, ResourceLimitError
from p2p.network.framing import Frame, pack_frame
from p2p.network.protocol import (
    MESSAGE_TYPE_FOR_PAYLOAD,
    AnnounceMessage,
    DecodedMessage,
    DiscoverMessage,
    FileDataMessage,
    FileInfoMessage,
    GetFileMessage,
    HeartbeatMessage,
    MessageType,
    PingMessage,
    PongMessage,
    ProtocolPayload,
    SearchRequestMessage,
    SearchResponseMessage,
    SearchResultRecord,
    TransferErrorMessage,
    validate_capabilities,
    validate_file_id,
    validate_file_name,
    validate_non_negative_int,
    validate_port,
    validate_positive_int,
    validate_search_query,
    validate_sha256_hex,
)


def _require_field(obj: dict[str, Any], key: str, msg_type: MessageType) -> Any:
    if key not in obj:
        raise MalformedMessageError(f"{msg_type.name} missing required field {key!r}")
    return obj[key]


def _require_str(obj: dict[str, Any], key: str, msg_type: MessageType) -> str:
    val = _require_field(obj, key, msg_type)
    if not isinstance(val, str):
        raise MalformedMessageError(f"{msg_type.name}.{key} must be a string")
    return val


def _require_bool_optional(obj: dict[str, Any], key: str, default: bool) -> bool:
    if key not in obj:
        return default
    val = obj[key]
    if not isinstance(val, bool):
        raise MalformedMessageError(f"{key!r} must be a boolean")
    return val


def _encode_discover(msg: DiscoverMessage) -> dict[str, Any]:
    data: dict[str, Any] = {
        "peer_id": msg.peer_id,
        "tcp_port": msg.tcp_port,
        "capabilities": list(msg.capabilities),
    }
    if msg.display_name:
        data["display_name"] = msg.display_name
    return data


def _encode_announce(msg: AnnounceMessage) -> dict[str, Any]:
    data: dict[str, Any] = {
        "peer_id": msg.peer_id,
        "tcp_port": msg.tcp_port,
        "capabilities": list(msg.capabilities),
    }
    if msg.display_name:
        data["display_name"] = msg.display_name
    if msg.timestamp:
        data["timestamp"] = msg.timestamp
    return data


def _encode_heartbeat(msg: HeartbeatMessage) -> dict[str, Any]:
    data: dict[str, Any] = {"peer_id": msg.peer_id, "tcp_port": msg.tcp_port}
    if msg.display_name:
        data["display_name"] = msg.display_name
    if msg.capabilities:
        data["capabilities"] = list(msg.capabilities)
    if msg.shared_file_count is not None:
        data["shared_file_count"] = msg.shared_file_count
    return data


def _encode_ping(msg: PingMessage) -> dict[str, Any]:
    data: dict[str, Any] = {"peer_id": msg.peer_id}
    if msg.sent_at_ms is not None:
        data["sent_at_ms"] = msg.sent_at_ms
    return data


def _encode_pong(msg: PongMessage) -> dict[str, Any]:
    data: dict[str, Any] = {
        "peer_id": msg.peer_id,
        "tcp_port": msg.tcp_port,
        "status": msg.status,
    }
    if msg.display_name:
        data["display_name"] = msg.display_name
    if msg.sent_at_ms is not None:
        data["sent_at_ms"] = msg.sent_at_ms
    return data


def _encode_search_request(msg: SearchRequestMessage) -> dict[str, Any]:
    return {
        "peer_id": msg.peer_id,
        "query": msg.query,
        "max_results": msg.max_results,
        "exact": msg.exact,
        "hop_count": msg.hop_count,
        "ttl": msg.ttl,
    }


def _encode_search_response(msg: SearchResponseMessage) -> dict[str, Any]:
    return {
        "peer_id": msg.peer_id,
        "results": [
            {
                "file_id": item.file_id,
                "file_name": item.file_name,
                "file_size": item.file_size,
                "sha256": item.sha256,
                "indexed_at": item.indexed_at,
            }
            for item in msg.results
        ],
        "truncated": msg.truncated,
    }


def _encode_get_file(msg: GetFileMessage) -> dict[str, Any]:
    data: dict[str, Any] = {
        "peer_id": msg.peer_id,
        "file_id": msg.file_id,
        "offset": msg.offset,
    }
    if msg.expected_sha256 is not None:
        data["expected_sha256"] = msg.expected_sha256
    if msg.chunk_size is not None:
        data["chunk_size"] = msg.chunk_size
    return data


def _encode_file_info(msg: FileInfoMessage) -> dict[str, Any]:
    return {
        "file_id": msg.file_id,
        "file_name": msg.file_name,
        "file_size": msg.file_size,
        "sha256": msg.sha256,
        "offset": msg.offset,
        "chunk_size": msg.chunk_size,
    }


def _encode_transfer_error(msg: TransferErrorMessage) -> dict[str, Any]:
    return {
        "code": msg.code,
        "message": msg.message,
        "retryable": msg.retryable,
    }


_JSON_ENCODERS: dict[MessageType, Callable[[Any], dict[str, Any]]] = {
    MessageType.DISCOVER: _encode_discover,
    MessageType.ANNOUNCE: _encode_announce,
    MessageType.HEARTBEAT: _encode_heartbeat,
    MessageType.PING: _encode_ping,
    MessageType.PONG: _encode_pong,
    MessageType.SEARCH_REQUEST: _encode_search_request,
    MessageType.SEARCH_RESPONSE: _encode_search_response,
    MessageType.GET_FILE: _encode_get_file,
    MessageType.FILE_INFO: _encode_file_info,
    MessageType.TRANSFER_ERROR: _encode_transfer_error,
}


def encode_message(
    payload: ProtocolPayload,
    *,
    request_id: bytes | None = None,
    max_control_bytes: int = MAX_CONTROL_PAYLOAD_BYTES,
    max_data_bytes: int = MAX_DATA_CHUNK_BYTES,
) -> bytes:
    """Serialize a typed protocol payload into a complete 26-byte header + payload frame."""
    msg_type = MESSAGE_TYPE_FOR_PAYLOAD.get(type(payload))
    if msg_type is None:
        raise MalformedMessageError(f"Unsupported protocol payload type: {type(payload).__name__}")

    if isinstance(payload, FileDataMessage):
        flags = FLAG_END_OF_STREAM if payload.end_of_stream else FLAG_NONE
        return pack_frame(
            MessageType.FILE_DATA,
            payload.data,
            request_id=request_id,
            flags=flags,
            max_control_bytes=max_control_bytes,
            max_data_bytes=max_data_bytes,
        )

    encoder = _JSON_ENCODERS[msg_type]
    raw_dict = encoder(payload)
    body = json.dumps(raw_dict, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return pack_frame(
        msg_type,
        body,
        request_id=request_id,
        flags=FLAG_NONE,
        max_control_bytes=max_control_bytes,
        max_data_bytes=max_data_bytes,
    )


def _decode_discover(
    obj: dict[str, Any],
    **_limits: int,
) -> DiscoverMessage:
    return DiscoverMessage(
        peer_id=_require_str(obj, "peer_id", MessageType.DISCOVER),
        tcp_port=validate_port(_require_field(obj, "tcp_port", MessageType.DISCOVER)),
        display_name=obj.get("display_name", ""),
        capabilities=validate_capabilities(obj.get("capabilities", ())),
    )


def _decode_announce(
    obj: dict[str, Any],
    **_limits: int,
) -> AnnounceMessage:
    return AnnounceMessage(
        peer_id=_require_str(obj, "peer_id", MessageType.ANNOUNCE),
        tcp_port=validate_port(_require_field(obj, "tcp_port", MessageType.ANNOUNCE)),
        display_name=obj.get("display_name", ""),
        capabilities=validate_capabilities(obj.get("capabilities", ())),
        timestamp=obj.get("timestamp", ""),
    )


def _decode_heartbeat(
    obj: dict[str, Any],
    **_limits: int,
) -> HeartbeatMessage:
    raw_count = obj.get("shared_file_count")
    return HeartbeatMessage(
        peer_id=_require_str(obj, "peer_id", MessageType.HEARTBEAT),
        tcp_port=validate_port(_require_field(obj, "tcp_port", MessageType.HEARTBEAT)),
        display_name=obj.get("display_name", ""),
        capabilities=validate_capabilities(obj.get("capabilities", ())),
        shared_file_count=(
            validate_non_negative_int(raw_count, "shared_file_count")
            if raw_count is not None
            else None
        ),
    )


def _decode_ping(obj: dict[str, Any], **_limits: int) -> PingMessage:
    raw_sent = obj.get("sent_at_ms")
    return PingMessage(
        peer_id=_require_str(obj, "peer_id", MessageType.PING),
        sent_at_ms=(
            validate_non_negative_int(raw_sent, "sent_at_ms") if raw_sent is not None else None
        ),
    )


def _decode_pong(obj: dict[str, Any], **_limits: int) -> PongMessage:
    raw_sent = obj.get("sent_at_ms")
    return PongMessage(
        peer_id=_require_str(obj, "peer_id", MessageType.PONG),
        tcp_port=validate_port(_require_field(obj, "tcp_port", MessageType.PONG)),
        status=_require_str(obj, "status", MessageType.PONG),
        display_name=obj.get("display_name", ""),
        sent_at_ms=(
            validate_non_negative_int(raw_sent, "sent_at_ms") if raw_sent is not None else None
        ),
    )


def _decode_search_request(
    obj: dict[str, Any],
    *,
    max_query_length: int = MAX_QUERY_LENGTH,
    **_limits: int,
) -> SearchRequestMessage:
    query = validate_search_query(
        _require_str(obj, "query", MessageType.SEARCH_REQUEST),
        max_length=max_query_length,
    )
    raw_max = obj.get("max_results", MAX_SEARCH_RESULTS_PER_PEER)
    raw_hop = obj.get("hop_count", 0)
    raw_ttl = obj.get("ttl", 1)
    return SearchRequestMessage(
        peer_id=_require_str(obj, "peer_id", MessageType.SEARCH_REQUEST),
        query=query,
        max_results=validate_positive_int(raw_max, "max_results"),
        exact=_require_bool_optional(obj, "exact", False),
        hop_count=validate_non_negative_int(raw_hop, "hop_count"),
        ttl=validate_positive_int(raw_ttl, "ttl"),
    )


def _decode_search_response(
    obj: dict[str, Any],
    *,
    max_filename_length: int = MAX_FILENAME_LENGTH,
    max_file_size_bytes: int = MAX_FILE_SIZE_BYTES,
    **_limits: int,
) -> SearchResponseMessage:
    raw_results = _require_field(obj, "results", MessageType.SEARCH_RESPONSE)
    if not isinstance(raw_results, list):
        raise MalformedMessageError("SEARCH_RESPONSE.results must be a list")
    if len(raw_results) > MAX_SEARCH_RESULTS_PER_PEER:
        raise ResourceLimitError(
            f"SEARCH_RESPONSE result count {len(raw_results)} > {MAX_SEARCH_RESULTS_PER_PEER}"
        )

    records: list[SearchResultRecord] = []
    for idx, entry in enumerate(raw_results):
        if not isinstance(entry, dict):
            raise MalformedMessageError(f"SEARCH_RESPONSE.results[{idx}] must be an object")
        size = validate_non_negative_int(
            _require_field(entry, "file_size", MessageType.SEARCH_RESPONSE), "file_size"
        )
        if max_file_size_bytes > 0 and size > max_file_size_bytes:
            raise ResourceLimitError(f"Result file_size {size} exceeds limit {max_file_size_bytes}")
        name = validate_file_name(
            _require_str(entry, "file_name", MessageType.SEARCH_RESPONSE),
            max_length=max_filename_length,
        )
        records.append(
            SearchResultRecord(
                file_id=validate_file_id(
                    _require_str(entry, "file_id", MessageType.SEARCH_RESPONSE)
                ),
                file_name=name,
                file_size=size,
                sha256=validate_sha256_hex(
                    _require_str(entry, "sha256", MessageType.SEARCH_RESPONSE)
                ),
                indexed_at=_require_str(entry, "indexed_at", MessageType.SEARCH_RESPONSE),
            )
        )

    return SearchResponseMessage(
        peer_id=_require_str(obj, "peer_id", MessageType.SEARCH_RESPONSE),
        results=tuple(records),
        truncated=_require_bool_optional(obj, "truncated", False),
    )


def _decode_get_file(obj: dict[str, Any], **_limits: int) -> GetFileMessage:
    raw_offset = _require_field(obj, "offset", MessageType.GET_FILE)
    raw_expected = obj.get("expected_sha256")
    raw_chunk = obj.get("chunk_size")
    return GetFileMessage(
        peer_id=_require_str(obj, "peer_id", MessageType.GET_FILE),
        file_id=validate_file_id(_require_str(obj, "file_id", MessageType.GET_FILE)),
        offset=validate_non_negative_int(raw_offset, "offset"),
        expected_sha256=(
            validate_sha256_hex(raw_expected, "expected_sha256")
            if raw_expected is not None
            else None
        ),
        chunk_size=(
            validate_positive_int(raw_chunk, "chunk_size") if raw_chunk is not None else None
        ),
    )


def _decode_file_info(
    obj: dict[str, Any],
    *,
    max_filename_length: int = MAX_FILENAME_LENGTH,
    max_file_size_bytes: int = MAX_FILE_SIZE_BYTES,
    **_limits: int,
) -> FileInfoMessage:
    size = validate_non_negative_int(
        _require_field(obj, "file_size", MessageType.FILE_INFO), "file_size"
    )
    if max_file_size_bytes > 0 and size > max_file_size_bytes:
        raise ResourceLimitError(f"FILE_INFO file_size {size} exceeds limit {max_file_size_bytes}")
    return FileInfoMessage(
        file_id=validate_file_id(_require_str(obj, "file_id", MessageType.FILE_INFO)),
        file_name=validate_file_name(
            _require_str(obj, "file_name", MessageType.FILE_INFO),
            max_length=max_filename_length,
        ),
        file_size=size,
        sha256=validate_sha256_hex(_require_str(obj, "sha256", MessageType.FILE_INFO)),
        offset=validate_non_negative_int(
            _require_field(obj, "offset", MessageType.FILE_INFO), "offset"
        ),
        chunk_size=validate_positive_int(
            _require_field(obj, "chunk_size", MessageType.FILE_INFO), "chunk_size"
        ),
    )


def _decode_transfer_error(obj: dict[str, Any], **_limits: int) -> TransferErrorMessage:
    return TransferErrorMessage(
        code=_require_str(obj, "code", MessageType.TRANSFER_ERROR),
        message=_require_str(obj, "message", MessageType.TRANSFER_ERROR),
        retryable=_require_bool_optional(obj, "retryable", False),
    )


_JSON_DECODERS: dict[MessageType, Callable[..., ProtocolPayload]] = {
    MessageType.DISCOVER: _decode_discover,
    MessageType.ANNOUNCE: _decode_announce,
    MessageType.HEARTBEAT: _decode_heartbeat,
    MessageType.PING: _decode_ping,
    MessageType.PONG: _decode_pong,
    MessageType.SEARCH_REQUEST: _decode_search_request,
    MessageType.SEARCH_RESPONSE: _decode_search_response,
    MessageType.GET_FILE: _decode_get_file,
    MessageType.FILE_INFO: _decode_file_info,
    MessageType.TRANSFER_ERROR: _decode_transfer_error,
}


def decode_message(
    frame: Frame,
    *,
    max_query_length: int = MAX_QUERY_LENGTH,
    max_filename_length: int = MAX_FILENAME_LENGTH,
    max_file_size_bytes: int = MAX_FILE_SIZE_BYTES,
) -> DecodedMessage:
    """Decode and validate a framed wire message into a typed DecodedMessage."""
    msg_type = frame.header.msg_type
    if msg_type == MessageType.FILE_DATA:
        payload: ProtocolPayload = FileDataMessage(
            data=frame.payload,
            end_of_stream=bool(frame.header.flags & FLAG_END_OF_STREAM),
        )
        return DecodedMessage(
            msg_type=msg_type,
            request_id=frame.header.request_id,
            flags=frame.header.flags,
            payload=payload,
        )

    try:
        raw_obj = json.loads(frame.payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MalformedMessageError(f"Invalid UTF-8/JSON in {msg_type.name}: {exc}") from exc

    if not isinstance(raw_obj, dict):
        raise MalformedMessageError(
            f"{msg_type.name} payload must be a JSON object, got {type(raw_obj).__name__}"
        )

    decoder = _JSON_DECODERS[msg_type]
    parsed = decoder(
        raw_obj,
        max_query_length=max_query_length,
        max_filename_length=max_filename_length,
        max_file_size_bytes=max_file_size_bytes,
    )
    return DecodedMessage(
        msg_type=msg_type,
        request_id=frame.header.request_id,
        flags=frame.header.flags,
        payload=parsed,
    )
