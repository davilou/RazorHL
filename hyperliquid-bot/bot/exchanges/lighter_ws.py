"""LighterCandleManager — live candle streaming from Lighter WebSocket.

Substitui o BinanceCandleManager no caminho Lighter. Usa o canal nativo
`candle/{market_id}/{resolution}` que empurra updates em batches de 500ms
a cada trade. Detecta candle close por mudança do campo `t` (timestamp).
"""

import json
import re
import time

_WS_URL_MAINNET = "wss://mainnet.zklighter.elliot.ai/stream"
_WS_URL_TESTNET = "wss://testnet.zklighter.elliot.ai/stream"

_INTERVAL_MS: dict[str, int] = {
    "1m":    60_000,
    "5m":    300_000,
    "15m":   900_000,
    "30m":   1_800_000,
    "1h":    3_600_000,
    "4h":    14_400_000,
    "12h":   43_200_000,
    "1d":    86_400_000,
}

_CHANNEL_RE = re.compile(r"^candle[:/](\d+)[:/]([0-9]+[mhd])$")


def _parse_channel(channel: str) -> tuple[int, str] | None:
    """Parse 'candle:0:5m' or 'candle/0/5m' → (market_id, resolution).

    Returns None for unknown channels or unsupported resolutions.
    """
    m = _CHANNEL_RE.match(channel)
    if not m:
        return None
    market_id = int(m.group(1))
    resolution = m.group(2)
    if resolution not in _INTERVAL_MS:
        return None
    return market_id, resolution


def _next_boundary_ms(now_ms: int, interval: str) -> int:
    """Next candle boundary (close timestamp) after now_ms for given interval.

    If now_ms is exactly on a boundary, returns now_ms + tf_ms.
    Raises KeyError for unknown intervals.
    """
    tf_ms = _INTERVAL_MS[interval]
    return ((now_ms // tf_ms) + 1) * tf_ms


import pandas as pd


def _candle_payload_to_row(c: dict) -> dict:
    """Convert Lighter candle dict (t/o/h/l/c/v) to internal row format."""
    return {
        "timestamp": int(c["t"]),
        "open":      float(c["o"]),
        "high":      float(c["h"]),
        "low":       float(c["l"]),
        "close":     float(c["c"]),
        "volume":    float(c["v"]),
    }


def _apply_candle_update(
    buffer: pd.DataFrame,
    candle: dict,
) -> tuple[pd.DataFrame, bool]:
    """Merge an incoming Lighter candle into the buffer.

    Returns (new_buffer, emitted_close_event):
    - `emitted_close_event = True` when the incoming `t` is greater than the
      last `t` in the buffer (a new candle started, so the previous one closed).
    - `False` for the very first candle (nothing to close yet), for same-t
      updates (in-place OHLC refresh), and for out-of-order updates (ignored).
    """
    row = _candle_payload_to_row(candle)
    if buffer.empty:
        new_buf = pd.DataFrame([row])
        new_buf["datetime"] = pd.to_datetime(new_buf["timestamp"], unit="ms", utc=True)
        new_buf.set_index("datetime", inplace=True)
        return new_buf, False

    last_t = int(buffer.iloc[-1]["timestamp"])
    incoming_t = row["timestamp"]

    if incoming_t < last_t:
        # out-of-order: ignora
        return buffer, False

    if incoming_t == last_t:
        # mesma vela em formação: substitui OHLCV in-place
        new_buf = buffer.copy()
        for col in ("open", "high", "low", "close", "volume"):
            new_buf.iloc[-1, new_buf.columns.get_loc(col)] = row[col]
        return new_buf, False

    # incoming_t > last_t: nova vela → anterior fechou
    new_row = pd.DataFrame([row])
    new_row["datetime"] = pd.to_datetime(new_row["timestamp"], unit="ms", utc=True)
    new_row.set_index("datetime", inplace=True)
    new_buf = pd.concat([buffer, new_row])
    new_buf = new_buf[~new_buf.index.duplicated(keep="last")].sort_index()
    return new_buf, True


import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable

from bot.logger import get_logger

log = get_logger(__name__)

_QUEUE_MAXSIZE = 50
_SEED_COUNT = 500


class LighterCandleManager:
    """Live candle streaming from Lighter native WebSocket.

    Threading model (matches BinanceCandleManager):
    - _ws_thread:        WebSocket reader, parses messages, updates buffer
    - _worker_thread:    drains queue and dispatches to thread pool
    - _watchdog_thread:  monitors global silence, reconnects
    - _boundary_thread:  fires per-TF boundary REST fallback for silent channels

    Callback signature: on_candle_close(asset: str, interval: str) -> None
    """

    def __init__(
        self,
        client,
        assets: list[str],
        on_candle_close: Callable[[str, str], None],
        intervals: list[str] | None = None,
        ws_url: str = _WS_URL_MAINNET,
    ):
        self._client = client
        self._assets = list(assets)
        self._on_candle_close = on_candle_close
        self._intervals: list[str] = list(intervals) if intervals else ["5m"]
        self._ws_url = ws_url

        self._buffer: dict[tuple[str, str], pd.DataFrame] = {}
        self._lock = threading.RLock()

        self._queue: queue.Queue = queue.Queue(maxsize=_QUEUE_MAXSIZE)
        self._paused = False
        self._stop_event = threading.Event()

        self._ws = None
        self._last_msg_ts: float = 0.0
        self._ts_lock = threading.Lock()
        self._last_update_ms: dict[tuple[str, str], int] = {}

        # subscriptions: (asset, tf) → market_id
        self._subscriptions: dict[tuple[str, str], int] = {}

        self._ws_thread: threading.Thread | None = None
        self._worker_thread: threading.Thread | None = None
        self._watchdog_thread: threading.Thread | None = None
        self._boundary_thread: threading.Thread | None = None
        self._executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="lighter-asset-worker")

    @property
    def intervals(self) -> list[str]:
        return list(self._intervals)

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        self._paused = False

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        self._executor.shutdown(wait=False)
        log.info("LighterCandleManager: stopped.")

    def start(self) -> None:
        """Wired in Task 5 — seeds buffer, starts WS + worker + watchdog + boundary threads."""
        raise NotImplementedError("Wired in Task 5")

    def update_assets(self, new_assets: list[str]) -> None:
        """Wired in Task 8."""
        raise NotImplementedError("Wired in Task 8")

    def get_candles(self, asset: str, interval: str, count: int = 100) -> pd.DataFrame:
        """Read last `count` candles from buffer. Wired in Task 6."""
        raise NotImplementedError("Wired in Task 6")

    def _market_to_asset(self, market_id: int, interval: str) -> str | None:
        """Reverse lookup market_id → asset via _subscriptions map."""
        for (asset, tf), mid in self._subscriptions.items():
            if mid == market_id and tf == interval:
                return asset
        return None

    def _on_message(self, ws, raw: str) -> None:
        with self._ts_lock:
            self._last_msg_ts = time.time()

        try:
            msg = json.loads(raw)
        except Exception:
            return

        msg_type = msg.get("type", "")
        if not msg_type.endswith("/candle"):
            return

        channel = msg.get("channel", "")
        parsed = _parse_channel(channel)
        if parsed is None:
            return
        market_id, interval = parsed

        asset = self._market_to_asset(market_id, interval)
        if asset is None:
            return  # canal recebido mas não subscrito (race ou bug)

        candles = msg.get("candles") or []
        if not candles:
            return

        key = (asset, interval)
        is_snapshot = msg_type == "subscribed/candle"

        # Para snapshot inicial pode vir múltiplas velas; aplica todas sem emitir
        # evento. Para update, sempre processa a última.
        candles_to_apply = candles if is_snapshot else [candles[-1]]

        for c in candles_to_apply:
            with self._lock:
                buf = self._buffer.get(key, pd.DataFrame())
                new_buf, emitted_close = _apply_candle_update(buf, c)
                self._buffer[key] = new_buf

            self._last_update_ms[key] = int(c["t"])

            if is_snapshot:
                continue  # snapshot nunca emite

            if emitted_close and not self._paused:
                # dedup: só emite se ainda não emitimos esse close
                last_emitted = getattr(self, "_last_emitted_t", {}).get(key, 0)
                # O close é da vela ANTERIOR. last_emitted guarda o t da vela
                # cujo close já anunciamos. Se o t anterior (= incoming - tf) for
                # > last_emitted, ainda não anunciamos esse close.
                prev_t = int(c["t"]) - _INTERVAL_MS[interval]
                if prev_t > last_emitted:
                    if not hasattr(self, "_last_emitted_t"):
                        self._last_emitted_t: dict[tuple[str, str], int] = {}
                    self._last_emitted_t[key] = prev_t
                    try:
                        self._queue.put_nowait((asset, interval))
                    except queue.Full:
                        pass
