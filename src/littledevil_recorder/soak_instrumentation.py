"""Instrumentation hooks for the scale/backpressure soak harness.

Collects real metrics from the live Recorder during a measurement run:
- message counts per channel
- event normalization counts
- write latencies
- queue depths
- reconnect/retry counts
- memory usage
- storage written

Metrics are collected via callback registration in the soak harness.
"""

from __future__ import annotations

import logging
import psutil
import os
from dataclasses import dataclass, field
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


@dataclass
class SoakMetrics:
    """Collected metrics from a soak run."""
    duration_seconds: float
    start_time: str
    end_time: str

    # Channel message/event counts
    trades_messages_received: int = 0
    depth_messages_received: int = 0
    oi_polls_completed: int = 0
    funding_polls_completed: int = 0
    mark_index_polls_completed: int = 0
    basis_polls_completed: int = 0
    liquidation_events_received: int = 0

    # Quality metrics
    duplicate_trades: int = 0
    duplicate_liquidations: int = 0
    processing_errors: int = 0
    rest_errors: int = 0
    websocket_reconnects: int = 0

    # Latency measurements (milliseconds)
    avg_write_latency_ms: float = 0.0
    max_write_latency_ms: float = 0.0
    avg_flush_latency_ms: float = 0.0
    max_flush_latency_ms: float = 0.0

    # Memory/storage (MB, bytes)
    memory_peak_mb: float = 0.0
    storage_written_bytes: int = 0

    # Capacity extrapolations
    events_per_hour: float = 0.0
    projected_gb_per_day: float = 0.0

    # Internal tracking for latency histograms
    write_latencies_ms: list[float] = field(default_factory=list)
    flush_latencies_ms: list[float] = field(default_factory=list)


class SoakInstrument:
    """Collects metrics during soak run."""

    def __init__(self) -> None:
        self.metrics = SoakMetrics(
            duration_seconds=0.0,
            start_time=datetime.now(UTC).isoformat(),
            end_time=datetime.now(UTC).isoformat(),
        )
        self._process = psutil.Process(os.getpid())
        self._memory_peak = 0.0

    def record_trade_message(self, symbol: str) -> None:
        """Record incoming trade message."""
        self.metrics.trades_messages_received += 1

    def record_depth_message(self, symbol: str) -> None:
        """Record incoming depth message."""
        self.metrics.depth_messages_received += 1

    def record_oi_poll(self, symbol: str) -> None:
        """Record completed OI poll."""
        self.metrics.oi_polls_completed += 1

    def record_funding_poll(self, symbol: str) -> None:
        """Record completed funding poll."""
        self.metrics.funding_polls_completed += 1

    def record_mark_index_poll(self, symbol: str) -> None:
        """Record completed mark/index poll."""
        self.metrics.mark_index_polls_completed += 1

    def record_basis_poll(self, symbol: str) -> None:
        """Record completed basis poll."""
        self.metrics.basis_polls_completed += 1

    def record_liquidation_event(self, symbol: str) -> None:
        """Record liquidation stream event."""
        self.metrics.liquidation_events_received += 1

    def record_write_latency_ms(self, latency: float) -> None:
        """Record a write latency measurement."""
        self.metrics.write_latencies_ms.append(latency)

    def record_flush_latency_ms(self, latency: float) -> None:
        """Record a flush latency measurement."""
        self.metrics.flush_latencies_ms.append(latency)

    def record_rest_error(self) -> None:
        """Record REST API error."""
        self.metrics.rest_errors += 1

    def record_websocket_reconnect(self) -> None:
        """Record WebSocket reconnection."""
        self.metrics.websocket_reconnects += 1

    def record_storage_written_bytes(self, byte_count: int) -> None:
        """Record bytes written to storage."""
        self.metrics.storage_written_bytes += byte_count

    def finalize_metrics(self, duration_seconds: float) -> SoakMetrics:
        """Compute final metrics and aggregations."""
        self.metrics.duration_seconds = duration_seconds
        self.metrics.end_time = datetime.now(UTC).isoformat()

        # Compute latency aggregations
        if self.metrics.write_latencies_ms:
            self.metrics.avg_write_latency_ms = sum(self.metrics.write_latencies_ms) / len(self.metrics.write_latencies_ms)
            self.metrics.max_write_latency_ms = max(self.metrics.write_latencies_ms)

        if self.metrics.flush_latencies_ms:
            self.metrics.avg_flush_latency_ms = sum(self.metrics.flush_latencies_ms) / len(self.metrics.flush_latencies_ms)
            self.metrics.max_flush_latency_ms = max(self.metrics.flush_latencies_ms)

        # Measure peak memory
        try:
            self.metrics.memory_peak_mb = self._process.memory_info().rss / (1024 * 1024)
        except Exception as exc:
            logger.warning("failed to measure process memory: %s", exc)

        # Compute extrapolations
        total_events = (
            self.metrics.trades_messages_received
            + self.metrics.depth_messages_received
            + self.metrics.oi_polls_completed
            + self.metrics.funding_polls_completed
            + self.metrics.mark_index_polls_completed
            + self.metrics.basis_polls_completed
            + self.metrics.liquidation_events_received
        )

        if duration_seconds > 0:
            self.metrics.events_per_hour = (total_events / duration_seconds) * 3600

        if duration_seconds > 0 and self.metrics.storage_written_bytes > 0:
            mb_per_second = self.metrics.storage_written_bytes / (1024 * 1024) / duration_seconds
            self.metrics.projected_gb_per_day = (mb_per_second * 86400) / 1024

        return self.metrics
