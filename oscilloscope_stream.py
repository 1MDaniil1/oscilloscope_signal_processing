from __future__ import annotations

import argparse
import csv
import ipaddress
import socket
import struct
import threading
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from statistics import median
from typing import Protocol

from peak_counter import find_signal_peaks, read_oscilloscope_csv


DEFAULT_SCPI_PORTS = (5025, 5024, 5555, 111)
DEFAULT_CHECK_PORTS = (80, 111, 443, 5024, 5025, 5555, 3000, 8080)
DEFAULT_DISCOVERY_WORKERS = 64


def parse_bool(value: str | bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected boolean value, got {value!r}")


@dataclass(frozen=True)
class Waveform:
    times: list[float]
    values: list[float]
    idn: str


@dataclass(frozen=True)
class FrameStats:
    frame_index: int
    samples: int
    frame_peaks: int
    counted_peaks: int
    total_peaks: int
    elapsed_seconds: float
    count_rate_hz: float
    first_peak_time: float | None
    time_step: float | None
    voltage_min: float
    voltage_max: float
    baseline: float
    duplicate_frame: bool
    status: str


@dataclass(frozen=True)
class FrameAnalysis:
    waveform: Waveform
    stats: FrameStats
    baseline: float
    threshold_voltage: float
    peak_indices: list[int]


class ScpiError(RuntimeError):
    pass


class ScpiTransport(Protocol):
    def write(self, command: str) -> None:
        pass

    def query(self, command: str) -> str:
        pass

    def query_block(self, command: str) -> bytes:
        pass


class ScpiSocket:
    def __init__(self, host: str, port: int = 5025, timeout: float = 3.0) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._sock: socket.socket | None = None

    def __enter__(self) -> ScpiSocket:
        self._sock = socket.create_connection((self.host, self.port), self.timeout)
        self._sock.settimeout(self.timeout)
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def write(self, command: str) -> None:
        if self._sock is None:
            raise ScpiError("SCPI socket is not connected")
        self._sock.sendall(command.encode("ascii") + b"\n")

    def query(self, command: str) -> str:
        self.write(command)
        return self.read_line()

    def read_line(self) -> str:
        data = bytearray()
        while True:
            chunk = self._recv_exact(1)
            if chunk in {b"\n", b""}:
                break
            data.extend(chunk)
        return data.decode("ascii", errors="replace").strip()

    def query_block(self, command: str) -> bytes:
        self.write(command)
        start = self._recv_exact(1)
        if start != b"#":
            rest = start + self._recv_some()
            raise ScpiError(f"expected SCPI binary block, got {rest[:80]!r}")

        ndigits = int(self._recv_exact(1))
        if ndigits == 0:
            raise ScpiError("indefinite SCPI blocks are not supported")

        size = int(self._recv_exact(ndigits).decode("ascii"))
        payload = self._recv_exact(size)
        self._drain_line_end()
        return payload

    def _recv_exact(self, size: int) -> bytes:
        if self._sock is None:
            raise ScpiError("SCPI socket is not connected")
        data = bytearray()
        while len(data) < size:
            chunk = self._sock.recv(size - len(data))
            if not chunk:
                raise ScpiError("connection closed while reading from oscilloscope")
            data.extend(chunk)
        return bytes(data)

    def _recv_some(self) -> bytes:
        if self._sock is None:
            return b""
        try:
            return self._sock.recv(4096)
        except socket.timeout:
            return b""

    def _drain_line_end(self) -> None:
        if self._sock is None:
            return
        old_timeout = self._sock.gettimeout()
        self._sock.settimeout(0.05)
        try:
            while True:
                if self._sock.recv(1) not in {b"\n", b"\r"}:
                    break
        except (TimeoutError, socket.timeout, OSError):
            pass
        finally:
            self._sock.settimeout(old_timeout)


class VisaScpi:
    def __init__(
        self,
        resource_name: str,
        timeout: float = 5.0,
        visa_backend: str = "@py",
    ) -> None:
        self.resource_name = resource_name
        self.timeout = timeout
        self.visa_backend = visa_backend
        self._resource_manager: object | None = None
        self._instrument: object | None = None

    def __enter__(self) -> VisaScpi:
        try:
            import pyvisa
        except ImportError as exc:
            raise ScpiError(
                "pyvisa is not installed; run: python -m pip install pyvisa pyvisa-py"
            ) from exc

        self._resource_manager = pyvisa.ResourceManager(self.visa_backend)
        self._instrument = self._resource_manager.open_resource(self.resource_name)
        self._instrument.timeout = int(self.timeout * 1000)
        return self

    def __exit__(self, *_exc: object) -> None:
        if self._instrument is not None:
            self._instrument.close()
            self._instrument = None
        if self._resource_manager is not None:
            self._resource_manager.close()
            self._resource_manager = None

    def write(self, command: str) -> None:
        if self._instrument is None:
            raise ScpiError("VISA instrument is not connected")
        self._instrument.write(command)

    def query(self, command: str) -> str:
        if self._instrument is None:
            raise ScpiError("VISA instrument is not connected")
        return str(self._instrument.query(command)).strip()

    def query_block(self, command: str) -> bytes:
        if self._instrument is None:
            raise ScpiError("VISA instrument is not connected")
        self._instrument.write(command)
        return _read_visa_definite_block(self._instrument)


def visa_resource_name(host: str) -> str:
    if "::" in host:
        return host
    return f"TCPIP::{host}::INSTR"


def _read_visa_definite_block(instrument: object) -> bytes:
    start = instrument.read_bytes(1)
    if start != b"#":
        rest = start + instrument.read_raw()
        raise ScpiError(f"expected SCPI binary block, got {rest[:80]!r}")

    ndigits = int(instrument.read_bytes(1).decode("ascii"))
    if ndigits == 0:
        raise ScpiError("indefinite SCPI blocks are not supported")

    size = int(instrument.read_bytes(ndigits).decode("ascii"))
    payload = instrument.read_bytes(size)
    _drain_visa_line_end(instrument)
    return payload


def _drain_visa_line_end(instrument: object) -> None:
    old_timeout = instrument.timeout
    instrument.timeout = 50
    try:
        while True:
            if instrument.read_bytes(1) not in {b"\n", b"\r"}:
                break
    except Exception:
        pass
    finally:
        instrument.timeout = old_timeout


def discover_scpi_hosts(
    networks: Iterable[str],
    ports: Iterable[int] = DEFAULT_SCPI_PORTS,
    timeout: float = 0.25,
    workers: int = DEFAULT_DISCOVERY_WORKERS,
) -> list[tuple[str, int, str]]:
    probes = [
        (str(ip), port)
        for network_text in networks
        for ip in ipaddress.ip_network(network_text, strict=False).hosts()
        for port in ports
    ]
    hits_by_host: dict[str, tuple[str, int, str]] = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(_probe_scpi_idn, host, port, timeout): (host, port)
            for host, port in probes
        }
        for future in as_completed(futures):
            hit = future.result()
            if hit is None:
                continue
            host, _port, _idn = hit
            hits_by_host.setdefault(host, hit)

    return sorted(hits_by_host.values(), key=lambda item: ipaddress.ip_address(item[0]))


def _probe_scpi_idn(host: str, port: int, timeout: float) -> tuple[str, int, str] | None:
    try:
        with ScpiSocket(host, port, timeout=timeout) as scpi:
            idn = scpi.query("*IDN?")
    except (OSError, ScpiError, UnicodeError, ValueError):
        return None
    if not idn:
        return None
    return host, port, idn


def check_tcp_ports(
    host: str,
    ports: Iterable[int] = DEFAULT_CHECK_PORTS,
    timeout: float = 1.0,
) -> list[tuple[int, str]]:
    results: list[tuple[int, str]] = []
    for port in ports:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                results.append((port, "open"))
        except ConnectionRefusedError:
            results.append((port, "closed/refused"))
        except TimeoutError:
            results.append((port, "filtered/timeout"))
        except OSError as exc:
            results.append((port, f"error: {exc}"))
    return results


def read_waveform(
    host: str,
    port: int,
    channel: str = "CHAN1",
    timeout: float = 5.0,
    backend: str = "raw",
    visa_backend: str = "@py",
) -> Waveform:
    with _open_scpi(host, port, timeout, backend, visa_backend) as scpi:
        idn = scpi.query("*IDN?")
        _configure_common_waveform(scpi, channel)
        preamble = _query_preamble(scpi)
        payload = _query_waveform_data(scpi)
    values = _decode_waveform(payload, preamble)
    times = [preamble.x_origin + i * preamble.x_increment for i in range(len(values))]
    return Waveform(times=times, values=values, idn=idn)


def query_idn(
    host: str,
    port: int,
    timeout: float = 3.0,
    backend: str = "raw",
    visa_backend: str = "@py",
) -> str:
    with _open_scpi(host, port, timeout, backend, visa_backend) as scpi:
        return scpi.query("*IDN?")


def _open_scpi(
    host: str,
    port: int,
    timeout: float,
    backend: str,
    visa_backend: str,
) -> ScpiSocket | VisaScpi:
    if backend == "raw":
        return ScpiSocket(host, port, timeout=timeout)
    if backend == "visa":
        return VisaScpi(visa_resource_name(host), timeout=timeout, visa_backend=visa_backend)
    raise ValueError(f"unsupported backend: {backend}")


def stream_waveforms(
    host: str,
    port: int,
    channel: str,
    interval: float,
    timeout: float = 5.0,
    backend: str = "raw",
    visa_backend: str = "@py",
) -> Iterator[Waveform]:
    while True:
        yield read_waveform(
            host,
            port,
            channel=channel,
            timeout=timeout,
            backend=backend,
            visa_backend=visa_backend,
        )
        time.sleep(interval)


def stream_dummy_waveforms(csv_path: str | Path, interval: float) -> Iterator[Waveform]:
    waveform = read_waveform_csv_file(csv_path, idn="DUMMY")
    zero_waveform = Waveform(
        times=waveform.times,
        values=[0.0 for _ in waveform.values],
        idn="DUMMY_ZERO",
    )
    use_signal = True
    while True:
        yield waveform if use_signal else zero_waveform
        use_signal = not use_signal
        time.sleep(interval)


@dataclass(frozen=True)
class Preamble:
    x_increment: float
    x_origin: float
    y_increment: float
    y_origin: float
    y_reference: float
    byte_width: int
    byte_order: str


def _configure_common_waveform(scpi: ScpiTransport, channel: str) -> None:
    commands = [
        f":WAV:SOUR {channel}",
        f":WAVEFORM:SOURCE {channel}",
        ":WAV:MODE NORM",
        ":WAVEFORM:MODE NORM",
        ":WAV:FORM BYTE",
        ":WAVEFORM:FORMAT BYTE",
        ":WAV:BYT LSBF",
        ":WAVEFORM:BYTEORDER LSBFirst",
    ]
    for command in commands:
        try:
            scpi.write(command)
        except (OSError, ScpiError):
            pass


def _query_preamble(scpi: ScpiTransport) -> Preamble:
    for command in (":WAV:PRE?", ":WAVEFORM:PREAMBLE?"):
        try:
            raw = scpi.query(command)
            if raw:
                return _parse_preamble(raw)
        except (OSError, ScpiError, ValueError):
            continue
    raise ScpiError("oscilloscope did not return a supported waveform preamble")


def _query_waveform_data(scpi: ScpiTransport) -> bytes:
    for command in (":WAV:DATA?", ":WAVEFORM:DATA?"):
        try:
            return scpi.query_block(command)
        except (OSError, ScpiError):
            continue
    raise ScpiError("oscilloscope did not return waveform data")


def _parse_preamble(raw: str) -> Preamble:
    fields = [field.strip().strip('"') for field in raw.split(",")]
    numbers = [_to_float(field) for field in fields]

    # Rigol/Siglent style:
    # format,type,points,count,xinc,xorigin,xref,yinc,yorigin,yref
    if len(numbers) >= 10:
        return Preamble(
            x_increment=numbers[4],
            x_origin=numbers[5],
            y_increment=numbers[7],
            y_origin=numbers[8],
            y_reference=numbers[9],
            byte_width=1,
            byte_order="<",
        )

    raise ValueError(f"unsupported preamble: {raw!r}")


def _to_float(value: str) -> float:
    try:
        return float(value)
    except ValueError:
        return 0.0


def _decode_waveform(payload: bytes, preamble: Preamble) -> list[float]:
    if preamble.byte_width == 1:
        raw_values = payload
    elif preamble.byte_width == 2:
        fmt = f"{preamble.byte_order}{len(payload) // 2}h"
        raw_values = struct.unpack(fmt, payload)
    else:
        raise ScpiError(f"unsupported waveform byte width: {preamble.byte_width}")

    return [
        (sample - preamble.y_reference) * preamble.y_increment + preamble.y_origin
        for sample in raw_values
    ]


def write_waveform_csv(path: str | Path, waveform: Waveform) -> None:
    with Path(path).open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Time(s)", "CH1(V)"])
        writer.writerows(zip(waveform.times, waveform.values, strict=True))


def read_waveform_csv_file(path: str | Path, idn: str = "CSV") -> Waveform:
    times, values = read_oscilloscope_csv(path)
    return Waveform(times=times, values=values, idn=idn)


def calculate_frame_stats(
    waveform: Waveform,
    frame_index: int,
    threshold: float,
    total_peaks_before: int,
    started_at: float,
    polarity: str,
    min_distance_samples: int,
    baseline_mode: str,
    interval_seconds: float,
    count_frame: bool = True,
) -> FrameStats:
    baseline = estimate_baseline(waveform.values, baseline_mode)
    peak_values = [value - baseline for value in waveform.values]
    peaks = find_signal_peaks(
        peak_values,
        threshold,
        times=waveform.times,
        min_distance_samples=min_distance_samples,
        polarity=polarity,
    )
    counted_peaks = len(peaks) if count_frame else 0
    total_peaks = total_peaks_before + counted_peaks
    elapsed_seconds = max(time.monotonic() - started_at, 1e-9)
    time_step = waveform.times[1] - waveform.times[0] if len(waveform.times) > 1 else None
    return FrameStats(
        frame_index=frame_index,
        samples=len(waveform.values),
        frame_peaks=len(peaks),
        counted_peaks=counted_peaks,
        total_peaks=total_peaks,
        elapsed_seconds=elapsed_seconds,
        count_rate_hz=counted_peaks / max(interval_seconds, 1e-9),
        first_peak_time=peaks[0].time if peaks else None,
        time_step=time_step,
        voltage_min=min(waveform.values) if waveform.values else float("nan"),
        voltage_max=max(waveform.values) if waveform.values else float("nan"),
        baseline=baseline,
        duplicate_frame=not count_frame,
        status="duplicate frame" if not count_frame else "running",
    )


def analyze_frame(
    waveform: Waveform,
    frame_index: int,
    threshold: float,
    total_peaks_before: int,
    started_at: float,
    polarity: str,
    min_distance_samples: int,
    baseline_mode: str,
    interval_seconds: float,
    count_frame: bool = True,
) -> FrameAnalysis:
    baseline = estimate_baseline(waveform.values, baseline_mode)
    peak_values = [value - baseline for value in waveform.values]
    peaks = find_signal_peaks(
        peak_values,
        threshold,
        times=waveform.times,
        min_distance_samples=min_distance_samples,
        polarity=polarity,
    )
    counted_peaks = len(peaks) if count_frame else 0
    total_peaks = total_peaks_before + counted_peaks
    elapsed_seconds = max(time.monotonic() - started_at, 1e-9)
    time_step = waveform.times[1] - waveform.times[0] if len(waveform.times) > 1 else None
    stats = FrameStats(
        frame_index=frame_index,
        samples=len(waveform.values),
        frame_peaks=len(peaks),
        counted_peaks=counted_peaks,
        total_peaks=total_peaks,
        elapsed_seconds=elapsed_seconds,
        count_rate_hz=counted_peaks / max(interval_seconds, 1e-9),
        first_peak_time=peaks[0].time if peaks else None,
        time_step=time_step,
        voltage_min=min(waveform.values) if waveform.values else float("nan"),
        voltage_max=max(waveform.values) if waveform.values else float("nan"),
        baseline=baseline,
        duplicate_frame=not count_frame,
        status="duplicate frame" if not count_frame else "running",
    )
    threshold_voltage = baseline + threshold if polarity == "positive" else baseline - threshold
    return FrameAnalysis(
        waveform=waveform,
        stats=stats,
        baseline=baseline,
        threshold_voltage=threshold_voltage,
        peak_indices=[peak.index for peak in peaks],
    )


def estimate_baseline(values: list[float], mode: str) -> float:
    if not values or mode == "none":
        return 0.0
    if mode == "median":
        return float(median(values))
    if mode == "edges":
        edge_size = max(1, len(values) // 10)
        return float(median([*values[:edge_size], *values[-edge_size:]]))
    raise ValueError(f"unsupported baseline mode: {mode}")


def waveform_signature(waveform: Waveform) -> tuple[float, ...]:
    return tuple(waveform.values)


def run_live_counter_gui(args: argparse.Namespace) -> int:
    import tkinter as tk
    from tkinter import ttk

    updates: Queue[FrameAnalysis | Exception] = Queue()
    stop_event = threading.Event()

    def worker() -> None:
        total_peaks = 0
        started_at = time.monotonic()
        last_signature: tuple[float, ...] | None = None
        waveform_source = (
            stream_dummy_waveforms(args.dummy_csv, args.interval)
            if args.dummy
            else stream_waveforms(
                args.host,
                args.port,
                args.channel,
                args.interval,
                args.timeout,
                backend=args.backend,
                visa_backend=args.visa_backend,
            )
        )
        try:
            for frame_index, waveform in enumerate(waveform_source, start=1):
                if stop_event.is_set():
                    break
                if args.csv:
                    write_waveform_csv(args.csv, waveform)
                signature = waveform_signature(waveform)
                count_frame = not (args.dedupe_frames and signature == last_signature)
                analysis = analyze_frame(
                    waveform,
                    frame_index,
                    args.threshold,
                    total_peaks,
                    started_at,
                    args.polarity,
                    args.min_distance_samples,
                    args.baseline,
                    args.interval,
                    count_frame=count_frame,
                )
                stats = analysis.stats
                total_peaks = stats.total_peaks
                last_signature = signature
                updates.put(analysis)
                if args.frames is not None and frame_index >= args.frames:
                    break
        except Exception as exc:
            updates.put(exc)

    root = tk.Tk()
    root.title("Oscilloscope Live Counter")
    root.geometry("760x820")
    root.minsize(620, 620)

    interval_count_var = tk.StringVar(value="0")
    frame_var = tk.StringVar(value="0")
    rate_var = tk.StringVar(value="0.00 Hz")
    samples_var = tk.StringVar(value="-")
    range_var = tk.StringVar(value="-")
    baseline_var = tk.StringVar(value="-")
    dt_var = tk.StringVar(value="-")
    first_peak_var = tk.StringVar(value="-")
    status_var = tk.StringVar(value="connecting...")

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    container = ttk.Frame(root, padding=16)
    container.grid(row=0, column=0, sticky="nsew")
    container.columnconfigure(1, weight=1)
    container.rowconfigure(11, weight=1)
    container.rowconfigure(12, weight=1)

    ttk.Label(container, text="Counts / interval").grid(row=0, column=0, columnspan=2, sticky="w")
    ttk.Label(container, textvariable=interval_count_var, font=("Segoe UI", 64, "bold")).grid(
        row=1, column=0, columnspan=2, sticky="w"
    )

    rows = [
        ("Last frame", frame_var),
        ("Rate", rate_var),
        ("Samples", samples_var),
        ("Voltage min/max", range_var),
        ("Baseline", baseline_var),
        ("Time step", dt_var),
        ("First peak time", first_peak_var),
        ("Status", status_var),
    ]
    for row_index, (label, variable) in enumerate(rows, start=2):
        ttk.Label(container, text=label).grid(row=row_index, column=0, sticky="w", pady=3)
        ttk.Label(container, textvariable=variable).grid(row=row_index, column=1, sticky="w", pady=3)

    graph = tk.Canvas(container, height=220, bg="white", highlightthickness=1, highlightbackground="#999")
    graph.grid(row=11, column=0, columnspan=2, sticky="nsew", pady=(14, 0))
    waveform_graph = tk.Canvas(container, height=240, bg="white", highlightthickness=1, highlightbackground="#999")
    waveform_graph.grid(row=12, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
    history: list[tuple[float, int]] = []
    latest_analysis: FrameAnalysis | None = None

    def redraw_graph() -> None:
        graph.delete("all")
        width = max(graph.winfo_width(), 1)
        height = max(graph.winfo_height(), 1)
        left = 48
        right = 12
        top = 16
        bottom = 34
        plot_width = max(width - left - right, 1)
        plot_height = max(height - top - bottom, 1)
        graph.create_line(left, top, left, top + plot_height, fill="#666")
        graph.create_line(left, top + plot_height, left + plot_width, top + plot_height, fill="#666")
        graph.create_text(8, top, text="counts", anchor="nw", fill="#333")
        graph.create_text(left + plot_width, height - 18, text="time", anchor="e", fill="#333")
        if not history:
            graph.create_text(width / 2, height / 2, text="waiting for frames", fill="#777")
            return

        visible = history[-200:]
        start_t = visible[0][0]
        end_t = visible[-1][0]
        span_t = max(end_t - start_t, 1e-9)
        max_count = max(1, max(count for _t, count in visible))
        graph.create_text(left - 8, top, text=str(max_count), anchor="e", fill="#333")
        graph.create_text(left - 8, top + plot_height, text="0", anchor="e", fill="#333")

        points: list[tuple[float, float]] = []
        for timestamp, count in visible:
            x = left + ((timestamp - start_t) / span_t) * plot_width
            y = top + plot_height - (count / max_count) * plot_height
            points.append((x, y))
        for x, y in points:
            graph.create_oval(x - 2, y - 2, x + 2, y + 2, fill="#1f77b4", outline="")
        if len(points) > 1:
            flat_points = [coord for point in points for coord in point]
            graph.create_line(*flat_points, fill="#1f77b4", width=2)

    def redraw_waveform_graph() -> None:
        waveform_graph.delete("all")
        width = max(waveform_graph.winfo_width(), 1)
        height = max(waveform_graph.winfo_height(), 1)
        left = 56
        right = 12
        top = 16
        bottom = 34
        plot_width = max(width - left - right, 1)
        plot_height = max(height - top - bottom, 1)
        waveform_graph.create_line(left, top, left, top + plot_height, fill="#666")
        waveform_graph.create_line(left, top + plot_height, left + plot_width, top + plot_height, fill="#666")
        waveform_graph.create_text(8, top, text="V", anchor="nw", fill="#333")
        waveform_graph.create_text(left + plot_width, height - 18, text="time", anchor="e", fill="#333")
        if latest_analysis is None:
            waveform_graph.create_text(width / 2, height / 2, text="waiting for waveform", fill="#777")
            return

        waveform = latest_analysis.waveform
        if not waveform.times or not waveform.values:
            waveform_graph.create_text(width / 2, height / 2, text="empty waveform", fill="#777")
            return

        min_t = waveform.times[0]
        max_t = waveform.times[-1]
        span_t = max(max_t - min_t, 1e-18)
        y_values = [*waveform.values, latest_analysis.baseline, latest_analysis.threshold_voltage]
        min_v = min(y_values)
        max_v = max(y_values)
        span_v = max(max_v - min_v, 1e-18)

        def x_for(t: float) -> float:
            return left + ((t - min_t) / span_t) * plot_width

        def y_for(v: float) -> float:
            return top + plot_height - ((v - min_v) / span_v) * plot_height

        waveform_graph.create_text(left - 8, y_for(max_v), text=f"{max_v:.3g}", anchor="e", fill="#333")
        waveform_graph.create_text(left - 8, y_for(min_v), text=f"{min_v:.3g}", anchor="e", fill="#333")
        baseline_y = y_for(latest_analysis.baseline)
        threshold_y = y_for(latest_analysis.threshold_voltage)
        waveform_graph.create_line(left, baseline_y, left + plot_width, baseline_y, fill="#777", dash=(4, 3))
        waveform_graph.create_line(left, threshold_y, left + plot_width, threshold_y, fill="#d62728", dash=(4, 3))

        points = [coord for t, v in zip(waveform.times, waveform.values, strict=True) for coord in (x_for(t), y_for(v))]
        if len(points) >= 4:
            waveform_graph.create_line(*points, fill="#1f77b4", width=1)
        for index in latest_analysis.peak_indices:
            x = x_for(waveform.times[index])
            y = y_for(waveform.values[index])
            waveform_graph.create_oval(x - 3, y - 3, x + 3, y + 3, fill="#ff7f0e", outline="")

    def poll_updates() -> None:
        nonlocal latest_analysis
        try:
            while True:
                update = updates.get_nowait()
                if isinstance(update, Exception):
                    status_var.set(f"error: {update}")
                    continue
                latest_analysis = update
                stats = update.stats
                interval_count_var.set(str(stats.counted_peaks))
                frame_var.set(
                    f"{stats.frame_index}: {stats.counted_peaks} counted "
                    f"({stats.frame_peaks} detected)"
                )
                rate_var.set(f"{stats.count_rate_hz:.2f} Hz")
                samples_var.set(str(stats.samples))
                range_var.set(f"{stats.voltage_min:.6g} .. {stats.voltage_max:.6g} V")
                baseline_var.set(f"{stats.baseline:.6g} V")
                dt_var.set("-" if stats.time_step is None else f"{stats.time_step:.6g} s")
                first_peak_var.set(
                    "-" if stats.first_peak_time is None else f"{stats.first_peak_time:.6g} s"
                )
                status_var.set(stats.status)
                history.append((stats.elapsed_seconds, stats.counted_peaks))
                redraw_graph()
                redraw_waveform_graph()
        except Empty:
            pass
        if not stop_event.is_set():
            root.after(100, poll_updates)

    def close() -> None:
        stop_event.set()
        root.destroy()

    graph.bind("<Configure>", lambda _event: redraw_graph())
    waveform_graph.bind("<Configure>", lambda _event: redraw_waveform_graph())
    threading.Thread(target=worker, daemon=True).start()
    root.protocol("WM_DELETE_WINDOW", close)
    root.after(100, poll_updates)
    root.mainloop()
    return 0


def run_csv_test(args: argparse.Namespace) -> int:
    waveform = read_waveform_csv_file(args.test_csv or args.plot_csv)
    stats = calculate_frame_stats(
        waveform,
        frame_index=1,
        threshold=args.threshold,
        total_peaks_before=0,
        started_at=time.monotonic(),
        polarity=args.polarity,
        min_distance_samples=args.min_distance_samples,
        baseline_mode=args.baseline,
        interval_seconds=args.interval,
    )
    print(f"file={args.test_csv}")
    print(f"samples={stats.samples}")
    print(f"time_step={stats.time_step}")
    print(f"voltage_min={stats.voltage_min}")
    print(f"voltage_max={stats.voltage_max}")
    print(f"baseline={stats.baseline}")
    print(f"threshold={args.threshold}")
    print(f"polarity={args.polarity}")
    print(f"peaks={stats.frame_peaks}")
    print(f"first_peak_time={stats.first_peak_time}")
    return 0


def find_csv_peaks_for_plot(args: argparse.Namespace, waveform: Waveform) -> tuple[list[float], list[float], float]:
    baseline = estimate_baseline(waveform.values, args.baseline)
    peak_values = [value - baseline for value in waveform.values]
    peaks = find_signal_peaks(
        peak_values,
        args.threshold,
        times=waveform.times,
        min_distance_samples=args.min_distance_samples,
        polarity=args.polarity,
    )
    peak_times = [peak.time for peak in peaks if peak.time is not None]
    peak_voltages = [waveform.values[peak.index] for peak in peaks]
    return peak_times, peak_voltages, baseline


def run_csv_plot(args: argparse.Namespace) -> int:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is not installed; run: python -m pip install matplotlib")
        return 2

    waveform = read_waveform_csv_file(args.plot_csv)
    peak_times, peak_voltages, baseline = find_csv_peaks_for_plot(args, waveform)
    threshold_voltage = baseline + args.threshold if args.polarity == "positive" else baseline - args.threshold

    plt.figure(figsize=(11, 6))
    plt.plot(waveform.times, waveform.values, linewidth=1, label="waveform")
    plt.axhline(baseline, color="tab:gray", linestyle="--", linewidth=1, label=f"baseline {baseline:.6g} V")
    plt.axhline(
        threshold_voltage,
        color="tab:red",
        linestyle="--",
        linewidth=1,
        label=f"threshold {threshold_voltage:.6g} V",
    )
    if peak_times:
        plt.scatter(peak_times, peak_voltages, color="tab:orange", zorder=3, label=f"peaks: {len(peak_times)}")
    plt.xlabel("Time (s)")
    plt.ylabel("CH1 (V)")
    plt.title(f"{args.plot_csv}: {len(peak_times)} peaks")
    plt.legend()
    plt.tight_layout()
    if args.plot_out:
        plt.savefig(args.plot_out, dpi=150)
        print(f"saved plot: {args.plot_out}")
    else:
        plt.show()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream oscilloscope waveforms over LAN SCPI.")
    parser.add_argument("--host", help="Oscilloscope IPv4 address.")
    parser.add_argument("--backend", choices=("raw", "visa"), default="raw", help="SCPI transport backend.")
    parser.add_argument("--visa-backend", default="@py", help="PyVISA backend, e.g. @py.")
    parser.add_argument("--port", type=int, default=5025, help="SCPI TCP port.")
    parser.add_argument("--channel", default="CHAN1", help="Oscilloscope channel name.")
    parser.add_argument("--threshold", type=float, default=0.2, help="Peak threshold in volts.")
    parser.add_argument("--polarity", choices=("positive", "negative"), default="positive", help="Pulse polarity.")
    parser.add_argument("--baseline", choices=("none", "median", "edges"), default="none", help="Subtract baseline before peak detection.")
    parser.add_argument("--min-distance-samples", type=int, default=1, help="Minimum distance between counted peaks.")
    parser.add_argument("--interval", type=float, default=0.5, help="Delay between waveform reads.")
    parser.add_argument("--frames", type=int, help="Stop after this many waveform frames.")
    parser.add_argument("--csv", help="Write each latest waveform frame to this CSV path.")
    parser.add_argument("--test-csv", help="Read one waveform CSV file and count peaks without connecting to the oscilloscope.")
    parser.add_argument("--plot-csv", help="Plot one waveform CSV file and mark counted peaks.")
    parser.add_argument("--plot-out", help="Save --plot-csv figure to an image file instead of showing a window.")
    parser.add_argument("--gui", action="store_true", help="Show a live counter window.")
    parser.add_argument(
        "--dummy",
        nargs="?",
        const=True,
        default=False,
        type=parse_bool,
        help="Use dummy source alternating --dummy-csv waveform and a zero waveform.",
    )
    parser.add_argument("--dummy-csv", default="live_signal.csv", help="CSV waveform used by --dummy.")
    parser.add_argument("--dedupe-frames", action="store_true", help="Do not add counts from an identical repeated waveform frame.")
    parser.add_argument("--network", action="append", help="CIDR network to scan, e.g. 169.254.154.0/24.")
    parser.add_argument("--discover", action="store_true", help="Only discover SCPI hosts and exit.")
    parser.add_argument("--check", action="store_true", help="Check common oscilloscope TCP ports and exit.")
    parser.add_argument("--idn", action="store_true", help="Only query *IDN? and exit.")
    parser.add_argument("--timeout", type=float, default=5.0, help="TCP/SCPI timeout in seconds.")
    args = parser.parse_args()

    if args.discover:
        networks = args.network or ["169.254.154.0/24"]
        for host, port, idn in discover_scpi_hosts(networks):
            print(f"{host}:{port}\t{idn}")
        return 0

    if args.test_csv:
        return run_csv_test(args)

    if args.plot_csv:
        return run_csv_plot(args)

    if not args.host and not args.dummy:
        parser.error("--host is required unless --discover or --dummy is used")

    if args.check:
        for port, status in check_tcp_ports(args.host, timeout=min(args.timeout, 2.0)):
            print(f"{args.host}:{port}\t{status}")
        return 0

    if args.idn:
        try:
            print(
                query_idn(
                    args.host,
                    args.port,
                    timeout=args.timeout,
                    backend=args.backend,
                    visa_backend=args.visa_backend,
                )
            )
        except ConnectionRefusedError:
            print(
                f"{args.host}:{args.port} refused the connection. "
                "The oscilloscope is reachable, but raw SCPI socket is closed or disabled."
            )
            return 2
        except TimeoutError:
            print(f"{args.host}:{args.port} timed out.")
            return 2
        except OSError as exc:
            print(f"{args.host}:{args.port} connection failed: {exc}")
            return 2
        except ScpiError as exc:
            print(f"{args.host} SCPI failed: {exc}")
            return 2
        except Exception as exc:
            print(f"{args.host} VISA/SCPI failed: {exc}")
            return 2
        return 0

    if args.gui:
        return run_live_counter_gui(args)

    try:
        total_peaks = 0
        started_at = time.monotonic()
        last_signature: tuple[float, ...] | None = None
        waveform_source = (
            stream_dummy_waveforms(args.dummy_csv, args.interval)
            if args.dummy
            else stream_waveforms(
                args.host,
                args.port,
                args.channel,
                args.interval,
                args.timeout,
                backend=args.backend,
                visa_backend=args.visa_backend,
            )
        )
        for frame_index, waveform in enumerate(waveform_source, start=1):
            stats = calculate_frame_stats(
                waveform,
                frame_index,
                args.threshold,
                total_peaks,
                started_at,
                args.polarity,
                args.min_distance_samples,
                args.baseline,
                args.interval,
                count_frame=not (args.dedupe_frames and waveform_signature(waveform) == last_signature),
            )
            total_peaks = stats.total_peaks
            last_signature = waveform_signature(waveform)
            if args.csv:
                write_waveform_csv(args.csv, waveform)
            print(
                f"frame={frame_index} samples={stats.samples} "
                f"counted={stats.counted_peaks} detected={stats.frame_peaks} "
                f"rate_hz={stats.count_rate_hz:.2f} first_peak_time={stats.first_peak_time}"
            )
            if args.frames is not None and frame_index >= args.frames:
                break
    except Exception as exc:
        print(f"stream failed: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
