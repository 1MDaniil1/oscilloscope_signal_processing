from __future__ import annotations

import argparse
import csv
import ipaddress
import math
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

from peak_counter import Peak, read_oscilloscope_csv


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


def parse_frames(value: str) -> int | None:
    normalized = value.strip().lower()
    if normalized in {"live", "inf", "infinite", "forever"}:
        return None
    frames = int(value)
    if frames < 1:
        raise argparse.ArgumentTypeError("frames must be a positive integer or 'live'")
    return frames


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
    waveform_span: float | None
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


def find_comparator_events(
    values: list[float],
    threshold: float,
    *,
    times: list[float] | None = None,
    min_distance_samples: int = 1,
    polarity: str = "positive",
) -> list[Peak]:
    """Return detector-like threshold crossings with sample-based holdoff."""
    if times is not None and len(times) != len(values):
        raise ValueError("times and values must have the same length")
    if min_distance_samples < 1:
        raise ValueError("min_distance_samples must be at least 1")
    if polarity not in {"positive", "negative"}:
        raise ValueError("polarity must be 'positive' or 'negative'")

    events: list[Peak] = []
    last_event_index = -min_distance_samples

    for index in range(1, len(values)):
        previous = values[index - 1]
        current = values[index]
        if polarity == "positive":
            crossed = previous <= threshold < current
        else:
            crossed = previous >= threshold > current
        if not crossed or index - last_event_index < min_distance_samples:
            continue
        events.append(Peak(index=index, value=values[index], time=None if times is None else times[index]))
        last_event_index = index

    return events


def find_threshold_width_events(
    values: list[float],
    threshold: float,
    *,
    times: list[float] | None = None,
    min_width_s: float | None = None,
    max_width_s: float | None = None,
    polarity: str = "positive",
) -> list[Peak]:
    """Return threshold regions whose width at threshold is within configured bounds."""
    if times is not None and len(times) != len(values):
        raise ValueError("times and values must have the same length")
    if min_width_s is not None and min_width_s < 0:
        raise ValueError("min_width_s must be non-negative")
    if max_width_s is not None and max_width_s < 0:
        raise ValueError("max_width_s must be non-negative")
    if polarity not in {"positive", "negative"}:
        raise ValueError("polarity must be 'positive' or 'negative'")

    def over_threshold(value: float) -> bool:
        return value > threshold if polarity == "positive" else value < threshold

    events: list[Peak] = []
    index = 0
    n_values = len(values)
    while index < n_values:
        while index < n_values and not over_threshold(values[index]):
            index += 1
        if index >= n_values:
            break

        region_start = index
        while index < n_values and over_threshold(values[index]):
            index += 1
        region_end = index - 1

        if region_start == 0 or region_end == n_values - 1:
            continue

        if times is None:
            region_width_s = float(region_end - region_start)
        else:
            region_width_s = abs(times[region_end] - times[region_start])
        if min_width_s is not None and region_width_s < min_width_s:
            continue
        if max_width_s is not None and region_width_s > max_width_s:
            continue

        if polarity == "positive":
            event_index = max(range(region_start, region_end + 1), key=values.__getitem__)
        else:
            event_index = min(range(region_start, region_end + 1), key=values.__getitem__)
        events.append(
            Peak(
                index=event_index,
                value=values[event_index],
                time=None if times is None else times[event_index],
            )
        )

    return events


def find_above_threshold_samples(
    values: list[float],
    threshold: float,
    *,
    times: list[float] | None = None,
    polarity: str = "positive",
) -> list[Peak]:
    """Return every sample above/below threshold as its own count."""
    if times is not None and len(times) != len(values):
        raise ValueError("times and values must have the same length")
    if polarity not in {"positive", "negative"}:
        raise ValueError("polarity must be 'positive' or 'negative'")

    events: list[Peak] = []
    for index, value in enumerate(values):
        if polarity == "positive":
            counted = value > threshold
        else:
            counted = value < threshold
        if counted:
            events.append(Peak(index=index, value=value, time=None if times is None else times[index]))
    return events


def find_events(
    values: list[float],
    threshold: float,
    *,
    times: list[float] | None,
    min_distance_samples: int,
    polarity: str,
    detection_mode: str,
    min_peak_width_s: float | None,
    max_peak_width_s: float | None,
) -> list[Peak]:
    if detection_mode == "crossing":
        return find_comparator_events(
            values,
            threshold,
            times=times,
            min_distance_samples=min_distance_samples,
            polarity=polarity,
        )
    if detection_mode == "threshold-width":
        return find_threshold_width_events(
            values,
            threshold,
            times=times,
            min_width_s=min_peak_width_s,
            max_width_s=max_peak_width_s,
            polarity=polarity,
        )
    if detection_mode == "above-threshold-samples":
        return find_above_threshold_samples(
            values,
            threshold,
            times=times,
            polarity=polarity,
        )
    raise ValueError(f"unsupported detection mode: {detection_mode}")


def resolve_min_distance_samples(
    waveform: Waveform,
    min_distance_samples: int,
    holdoff_ns: float | None,
) -> int:
    if holdoff_ns is None:
        return min_distance_samples
    if len(waveform.times) < 2:
        return min_distance_samples
    time_step = abs(waveform.times[1] - waveform.times[0])
    if time_step <= 0:
        return min_distance_samples
    return max(1, math.ceil((holdoff_ns * 1e-9) / time_step))


def resolve_max_peak_width_s(args: argparse.Namespace) -> float | None:
    if args.max_peak_width_ns is None:
        return None
    return args.max_peak_width_ns * 1e-9


def resolve_min_peak_width_s(args: argparse.Namespace) -> float | None:
    if args.min_peak_width_ns is None:
        return None
    return args.min_peak_width_ns * 1e-9


def format_seconds(value: float | None) -> str:
    if value is None:
        return "-"
    abs_value = abs(value)
    if abs_value >= 1:
        return f"{value:.6g} s"
    if abs_value >= 1e-3:
        return f"{value * 1e3:.6g} ms"
    if abs_value >= 1e-6:
        return f"{value * 1e6:.6g} us"
    if abs_value >= 1e-9:
        return f"{value * 1e9:.6g} ns"
    if abs_value >= 1e-12:
        return f"{value * 1e12:.6g} ps"
    return f"{value:.6g} s"


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
            try:
                self._instrument.close()
            except Exception:
                pass
            self._instrument = None
        if self._resource_manager is not None:
            try:
                self._resource_manager.close()
            except Exception:
                pass
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
    waveform_mode: str = "NORM",
    waveform_points: int | None = None,
    waveform_points_mode: str | None = None,
    waveform_start: int | None = None,
    waveform_stop: int | None = None,
    acquisition_memory_depth: str | None = None,
    stop_read_run: bool = False,
    run_stop_read: bool = False,
    acquire_seconds: float = 0.0,
    stop_settle: float = 0.05,
) -> Waveform:
    with _open_scpi(host, port, timeout, backend, visa_backend) as scpi:
        idn = scpi.query("*IDN?")
        return _read_waveform_from_scpi(
            scpi,
            idn,
            channel,
            waveform_mode,
            waveform_points,
            waveform_points_mode,
            waveform_start,
            waveform_stop,
            acquisition_memory_depth,
            stop_read_run,
            run_stop_read,
            acquire_seconds,
            stop_settle,
        )


def _read_waveform_from_scpi(
    scpi: ScpiTransport,
    idn: str,
    channel: str,
    waveform_mode: str,
    waveform_points: int | None,
    waveform_points_mode: str | None,
    waveform_start: int | None,
    waveform_stop: int | None,
    acquisition_memory_depth: str | None,
    stop_read_run: bool,
    run_stop_read: bool,
    acquire_seconds: float,
    stop_settle: float,
) -> Waveform:
    try:
        _configure_acquisition(scpi, acquisition_memory_depth)
        if run_stop_read:
            _write_first_supported(scpi, (":RUN", ":RUN:START"))
            time.sleep(max(acquire_seconds, 0.0))
            _write_first_supported(scpi, (":STOP", ":RUN:STOP"))
            time.sleep(max(stop_settle, 0.0))
        if stop_read_run:
            _write_first_supported(scpi, (":STOP", ":RUN:STOP"))
            time.sleep(max(stop_settle, 0.0))
        _configure_common_waveform(
            scpi,
            channel,
            waveform_mode,
            waveform_points,
            waveform_points_mode,
            waveform_start,
            waveform_stop,
        )
        preamble = _query_preamble(scpi)
        payload = _query_waveform_data(scpi)
    finally:
        if stop_read_run or run_stop_read:
            _write_first_supported(scpi, (":RUN", ":RUN:START"))
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
    waveform_mode: str = "NORM",
    waveform_points: int | None = None,
    waveform_points_mode: str | None = None,
    waveform_start: int | None = None,
    waveform_stop: int | None = None,
    acquisition_memory_depth: str | None = None,
    stop_read_run: bool = False,
    run_stop_read: bool = False,
    acquire_seconds: float = 0.0,
    stop_settle: float = 0.05,
) -> Iterator[Waveform]:
    with _open_scpi(host, port, timeout, backend, visa_backend) as scpi:
        idn = scpi.query("*IDN?")
        while True:
            yield _read_waveform_from_scpi(
                scpi,
                idn,
                channel,
                waveform_mode,
                waveform_points,
                waveform_points_mode,
                waveform_start,
                waveform_stop,
                acquisition_memory_depth,
                stop_read_run,
                run_stop_read,
                acquire_seconds,
                stop_settle,
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


def _configure_common_waveform(
    scpi: ScpiTransport,
    channel: str,
    waveform_mode: str,
    waveform_points: int | None,
    waveform_points_mode: str | None,
    waveform_start: int | None,
    waveform_stop: int | None,
) -> None:
    waveform_mode = waveform_mode.upper()
    if waveform_points and waveform_start is None and waveform_stop is None:
        waveform_start = 1
        waveform_stop = waveform_points
    commands = [
        f":WAV:SOUR {channel}",
        f":WAVEFORM:SOURCE {channel}",
        f":WAV:MODE {waveform_mode}",
        f":WAVEFORM:MODE {waveform_mode}",
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
    if waveform_points_mode:
        for command in (f":WAV:POIN:MODE {waveform_points_mode}", f":WAVEFORM:POINTS:MODE {waveform_points_mode}"):
            try:
                scpi.write(command)
            except (OSError, ScpiError):
                pass
    if waveform_points:
        for command in (f":WAV:POIN {waveform_points}", f":WAVEFORM:POINTS {waveform_points}"):
            try:
                scpi.write(command)
            except (OSError, ScpiError):
                pass
    if waveform_start is not None:
        for command in (f":WAV:STAR {waveform_start}", f":WAVEFORM:START {waveform_start}"):
            try:
                scpi.write(command)
            except (OSError, ScpiError):
                pass
    if waveform_stop is not None:
        for command in (f":WAV:STOP {waveform_stop}", f":WAVEFORM:STOP {waveform_stop}"):
            try:
                scpi.write(command)
            except (OSError, ScpiError):
                pass


def _configure_acquisition(scpi: ScpiTransport, memory_depth: str | None) -> None:
    if not memory_depth:
        return
    value = memory_depth.strip()
    if not value:
        return
    for command in (f":ACQ:MDEP {value}", f":ACQUIRE:MDEPTH {value}"):
        try:
            scpi.write(command)
            return
        except (OSError, ScpiError):
            continue


def _write_first_supported(scpi: ScpiTransport, commands: tuple[str, ...]) -> None:
    for command in commands:
        try:
            scpi.write(command)
            return
        except (OSError, ScpiError):
            continue


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
    detection_mode: str = "crossing",
    min_peak_width_s: float | None = None,
    max_peak_width_s: float | None = None,
    count_frame: bool = True,
) -> FrameStats:
    baseline = estimate_baseline(waveform.values, baseline_mode)
    threshold_voltage = baseline + threshold if polarity == "positive" else baseline - threshold
    events = find_events(
        waveform.values,
        threshold_voltage,
        times=waveform.times,
        min_distance_samples=min_distance_samples,
        polarity=polarity,
        detection_mode=detection_mode,
        min_peak_width_s=min_peak_width_s,
        max_peak_width_s=max_peak_width_s,
    )
    counted_peaks = len(events) if count_frame else 0
    total_peaks = total_peaks_before + counted_peaks
    elapsed_seconds = max(time.monotonic() - started_at, 1e-9)
    time_step = waveform.times[1] - waveform.times[0] if len(waveform.times) > 1 else None
    waveform_span = waveform.times[-1] - waveform.times[0] if len(waveform.times) > 1 else None
    return FrameStats(
        frame_index=frame_index,
        samples=len(waveform.values),
        frame_peaks=len(events),
        counted_peaks=counted_peaks,
        total_peaks=total_peaks,
        elapsed_seconds=elapsed_seconds,
        count_rate_hz=counted_peaks / max(interval_seconds, 1e-9),
        first_peak_time=events[0].time if events else None,
        time_step=time_step,
        waveform_span=waveform_span,
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
    detection_mode: str = "crossing",
    min_peak_width_s: float | None = None,
    max_peak_width_s: float | None = None,
    count_frame: bool = True,
) -> FrameAnalysis:
    baseline = estimate_baseline(waveform.values, baseline_mode)
    threshold_voltage = baseline + threshold if polarity == "positive" else baseline - threshold
    events = find_events(
        waveform.values,
        threshold_voltage,
        times=waveform.times,
        min_distance_samples=min_distance_samples,
        polarity=polarity,
        detection_mode=detection_mode,
        min_peak_width_s=min_peak_width_s,
        max_peak_width_s=max_peak_width_s,
    )
    counted_peaks = len(events) if count_frame else 0
    total_peaks = total_peaks_before + counted_peaks
    elapsed_seconds = max(time.monotonic() - started_at, 1e-9)
    time_step = waveform.times[1] - waveform.times[0] if len(waveform.times) > 1 else None
    waveform_span = waveform.times[-1] - waveform.times[0] if len(waveform.times) > 1 else None
    stats = FrameStats(
        frame_index=frame_index,
        samples=len(waveform.values),
        frame_peaks=len(events),
        counted_peaks=counted_peaks,
        total_peaks=total_peaks,
        elapsed_seconds=elapsed_seconds,
        count_rate_hz=counted_peaks / max(interval_seconds, 1e-9),
        first_peak_time=events[0].time if events else None,
        time_step=time_step,
        waveform_span=waveform_span,
        voltage_min=min(waveform.values) if waveform.values else float("nan"),
        voltage_max=max(waveform.values) if waveform.values else float("nan"),
        baseline=baseline,
        duplicate_frame=not count_frame,
        status="duplicate frame" if not count_frame else "running",
    )
    return FrameAnalysis(
        waveform=waveform,
        stats=stats,
        baseline=baseline,
        threshold_voltage=threshold_voltage,
        peak_indices=[event.index for event in events],
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
                waveform_mode=args.waveform_mode,
                waveform_points=args.waveform_points,
                waveform_points_mode=args.waveform_points_mode,
                waveform_start=args.waveform_start,
                waveform_stop=args.waveform_stop,
                acquisition_memory_depth=args.acquire_memory_depth,
                stop_read_run=args.stop_read_run,
                run_stop_read=args.run_stop_read,
                acquire_seconds=args.acquire_seconds,
                stop_settle=args.stop_settle,
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
                min_distance_samples = resolve_min_distance_samples(
                    waveform,
                    args.min_distance_samples,
                    args.holdoff_ns,
                )
                max_peak_width_s = resolve_max_peak_width_s(args)
                analysis = analyze_frame(
                    waveform,
                    frame_index,
                    args.threshold,
                    total_peaks,
                    started_at,
                    args.polarity,
                    min_distance_samples,
                    args.baseline,
                    args.interval,
                    detection_mode=args.detection_mode,
                    min_peak_width_s=resolve_min_peak_width_s(args),
                    max_peak_width_s=max_peak_width_s,
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
    root.geometry("980x980")
    root.minsize(820, 760)

    colors = {
        "bg": "#111827",
        "panel": "#0f172a",
        "plot": "#020617",
        "fg": "#e5e7eb",
        "muted": "#94a3b8",
        "axis": "#64748b",
        "border": "#334155",
        "signal": "#38bdf8",
        "threshold": "#f87171",
        "baseline": "#a3a3a3",
        "peak": "#f59e0b",
    }
    root.configure(bg=colors["bg"])
    style = ttk.Style(root)
    style.configure("TFrame", background=colors["bg"])
    style.configure("TLabel", background=colors["bg"], foreground=colors["fg"])

    raw_gate_count_var = tk.StringVar(value="0")
    corrected_rate_big_var = tk.StringVar(value="-")
    frame_var = tk.StringVar(value="0")
    rate_var = tk.StringVar(value="0.00 Hz")
    frames_per_second_var = tk.StringVar(value="-")
    coverage_var = tk.StringVar(value="-")
    corrected_rate_var = tk.StringVar(value="-")
    samples_var = tk.StringVar(value="-")
    range_var = tk.StringVar(value="-")
    baseline_var = tk.StringVar(value="-")
    comparator_var = tk.StringVar(value="-")
    polarity_var = tk.StringVar(value=args.polarity)
    dt_var = tk.StringVar(value="-")
    span_var = tk.StringVar(value="-")
    first_peak_var = tk.StringVar(value="-")
    status_var = tk.StringVar(value="connecting...")

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    container = ttk.Frame(root, padding=16)
    container.grid(row=0, column=0, sticky="nsew")
    container.columnconfigure(1, weight=1)

    header = ttk.Frame(container)
    header.grid(row=0, column=0, columnspan=2, sticky="ew")
    header.columnconfigure(0, weight=1)
    header.columnconfigure(1, weight=1)
    ttk.Label(header, text="Raw counts / 1 s gate").grid(row=0, column=0, sticky="w")
    ttk.Label(header, text="Corrected rate").grid(row=0, column=1, sticky="w")
    ttk.Label(header, textvariable=raw_gate_count_var, font=("Segoe UI", 64, "bold")).grid(row=1, column=0, sticky="w")
    ttk.Label(header, textvariable=corrected_rate_big_var, font=("Segoe UI", 64, "bold")).grid(row=1, column=1, sticky="w")

    rows = [
        ("Last frame", frame_var),
        ("Rate", rate_var),
        ("Frames / s", frames_per_second_var),
        ("Coverage / s", coverage_var),
        ("Corrected rate", corrected_rate_var),
        ("Samples", samples_var),
        ("Voltage min/max", range_var),
        ("Baseline", baseline_var),
        ("Comparator level", comparator_var),
        ("Polarity", polarity_var),
        ("Time step", dt_var),
        ("Waveform span", span_var),
        ("First count time", first_peak_var),
        ("Status", status_var),
    ]
    for row_index, (label, variable) in enumerate(rows, start=2):
        ttk.Label(container, text=label).grid(row=row_index, column=0, sticky="w", pady=3)
        ttk.Label(container, textvariable=variable).grid(row=row_index, column=1, sticky="w", pady=3)

    graph_row = len(rows) + 2
    waveform_graph_row = graph_row + 1
    container.rowconfigure(graph_row, weight=1)
    container.rowconfigure(waveform_graph_row, weight=1)

    graph = tk.Canvas(
        container,
        height=300,
        bg=colors["plot"],
        highlightthickness=1,
        highlightbackground=colors["border"],
    )
    graph.grid(row=graph_row, column=0, columnspan=2, sticky="nsew", pady=(14, 0))
    waveform_graph = tk.Canvas(
        container,
        height=360,
        bg=colors["plot"],
        highlightthickness=1,
        highlightbackground=colors["border"],
    )
    waveform_graph.grid(row=waveform_graph_row, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
    history: list[tuple[float, int]] = []
    gate_start = 0.0
    gate_count = 0
    gate_coverage = 0.0
    gate_frames = 0
    completed_gate_count = 0
    completed_gate_coverage = 0.0
    completed_gate_frames = 0
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
        graph.create_line(left, top, left, top + plot_height, fill=colors["axis"])
        graph.create_line(left, top + plot_height, left + plot_width, top + plot_height, fill=colors["axis"])
        graph.create_text(8, top, text="counts", anchor="nw", fill=colors["fg"])
        graph.create_text(left + plot_width, height - 18, text="time, last 30 s", anchor="e", fill=colors["fg"])
        if not history:
            graph.create_text(width / 2, height / 2, text="waiting for frames", fill=colors["muted"])
            return

        end_t = history[-1][0]
        start_t = max(0.0, end_t - 30.0)
        visible = [(timestamp, count) for timestamp, count in history if timestamp >= start_t]
        span_t = max(end_t - start_t, 1e-9)
        max_count = max(1, max(count for _t, count in visible))
        graph.create_text(left - 8, top, text=str(max_count), anchor="e", fill=colors["fg"])
        graph.create_text(left - 8, top + plot_height, text="0", anchor="e", fill=colors["fg"])
        graph.create_text(left, top + plot_height + 14, text=f"-{span_t:.0f} s", anchor="w", fill=colors["muted"])
        graph.create_text(left + plot_width, top + plot_height + 14, text="0 s", anchor="e", fill=colors["muted"])

        points: list[tuple[float, float]] = []
        for timestamp, count in visible:
            x = left + ((timestamp - start_t) / span_t) * plot_width
            y = top + plot_height - (count / max_count) * plot_height
            points.append((x, y))
        for x, y in points:
            graph.create_oval(x - 2, y - 2, x + 2, y + 2, fill=colors["signal"], outline="")
        if len(points) > 1:
            flat_points = [coord for point in points for coord in point]
            graph.create_line(*flat_points, fill=colors["signal"], width=2)

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
        waveform_graph.create_line(left, top, left, top + plot_height, fill=colors["axis"])
        waveform_graph.create_line(left, top + plot_height, left + plot_width, top + plot_height, fill=colors["axis"])
        waveform_graph.create_text(8, top, text="V", anchor="nw", fill=colors["fg"])
        waveform_graph.create_text(left + plot_width, height - 18, text="time", anchor="e", fill=colors["fg"])
        if latest_analysis is None:
            waveform_graph.create_text(width / 2, height / 2, text="waiting for waveform", fill=colors["muted"])
            return

        waveform = latest_analysis.waveform
        if not waveform.times or not waveform.values:
            waveform_graph.create_text(width / 2, height / 2, text="empty waveform", fill=colors["muted"])
            return

        min_t = waveform.times[0]
        max_t = waveform.times[-1]
        span_t = max(max_t - min_t, 1e-18)
        mid_t = min_t + span_t / 2
        y_values = [*waveform.values, latest_analysis.baseline, latest_analysis.threshold_voltage]
        min_v = min(y_values)
        max_v = max(y_values)
        span_v = max(max_v - min_v, 1e-18)

        def x_for(t: float) -> float:
            return left + ((t - min_t) / span_t) * plot_width

        def y_for(v: float) -> float:
            return top + plot_height - ((v - min_v) / span_v) * plot_height

        waveform_graph.create_text(left - 8, y_for(max_v), text=f"{max_v:.3g}", anchor="e", fill=colors["fg"])
        waveform_graph.create_text(left - 8, y_for(min_v), text=f"{min_v:.3g}", anchor="e", fill=colors["fg"])
        waveform_graph.create_text(
            left,
            top + plot_height + 14,
            text=format_seconds(min_t),
            anchor="w",
            fill=colors["muted"],
        )
        waveform_graph.create_text(
            left + plot_width / 2,
            top + plot_height + 14,
            text=format_seconds(mid_t),
            anchor="center",
            fill=colors["muted"],
        )
        waveform_graph.create_text(
            left + plot_width,
            top + plot_height + 14,
            text=format_seconds(max_t),
            anchor="e",
            fill=colors["muted"],
        )
        waveform_graph.create_text(
            left,
            top + 2,
            text=(
                f"window {format_seconds(latest_analysis.stats.waveform_span)}, "
                f"dt {format_seconds(latest_analysis.stats.time_step)}, "
                f"{latest_analysis.stats.samples} samples"
            ),
            anchor="nw",
            fill=colors["muted"],
        )
        baseline_y = y_for(latest_analysis.baseline)
        threshold_y = y_for(latest_analysis.threshold_voltage)
        waveform_graph.create_line(
            left, baseline_y, left + plot_width, baseline_y, fill=colors["baseline"], dash=(4, 3)
        )
        waveform_graph.create_line(
            left, threshold_y, left + plot_width, threshold_y, fill=colors["threshold"], dash=(4, 3)
        )

        points = [coord for t, v in zip(waveform.times, waveform.values, strict=True) for coord in (x_for(t), y_for(v))]
        if len(points) >= 4:
            waveform_graph.create_line(*points, fill=colors["signal"], width=1)
        for index in latest_analysis.peak_indices:
            x = x_for(waveform.times[index])
            y = y_for(waveform.values[index])
            waveform_graph.create_oval(x - 3, y - 3, x + 3, y + 3, fill=colors["peak"], outline="")

    def poll_updates() -> None:
        nonlocal completed_gate_count, completed_gate_coverage, completed_gate_frames
        nonlocal gate_count, gate_coverage, gate_frames, gate_start, latest_analysis
        try:
            while True:
                update = updates.get_nowait()
                if isinstance(update, Exception):
                    status_var.set(f"error: {update}")
                    continue
                latest_analysis = update
                stats = update.stats
                while stats.elapsed_seconds >= gate_start + 1.0:
                    completed_gate_count = gate_count
                    completed_gate_coverage = gate_coverage
                    completed_gate_frames = gate_frames
                    history.append((gate_start + 1.0, completed_gate_count))
                    gate_start += 1.0
                    gate_count = 0
                    gate_coverage = 0.0
                    gate_frames = 0
                gate_count += stats.counted_peaks
                gate_coverage += max(stats.waveform_span or 0.0, 0.0)
                gate_frames += 1
                raw_gate_count_var.set(str(completed_gate_count))
                frame_var.set(
                    f"{stats.frame_index}: {stats.counted_peaks} counted "
                    f"({stats.frame_peaks} crossings)"
                )
                rate_var.set(f"{completed_gate_count:.2f} Hz")
                frames_per_second_var.set(str(completed_gate_frames))
                coverage_var.set(f"{completed_gate_coverage:.6g} s/s")
                corrected_rate = (
                    completed_gate_count / completed_gate_coverage
                    if completed_gate_coverage > 0
                    else None
                )
                corrected_rate_text = "-" if corrected_rate is None else f"{corrected_rate:.2f} Hz"
                corrected_rate_big_var.set(corrected_rate_text)
                corrected_rate_var.set(corrected_rate_text)
                samples_var.set(str(stats.samples))
                range_var.set(f"{stats.voltage_min:.6g} .. {stats.voltage_max:.6g} V")
                baseline_var.set(f"{stats.baseline:.6g} V")
                comparator_var.set(f"{update.threshold_voltage:.6g} V")
                polarity_var.set(args.polarity)
                dt_var.set("-" if stats.time_step is None else f"{stats.time_step:.6g} s")
                span_var.set("-" if stats.waveform_span is None else f"{stats.waveform_span:.6g} s")
                first_peak_var.set(
                    "-" if stats.first_peak_time is None else f"{stats.first_peak_time:.6g} s"
                )
                status_var.set(stats.status)
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
    min_distance_samples = resolve_min_distance_samples(
        waveform,
        args.min_distance_samples,
        args.holdoff_ns,
    )
    stats = calculate_frame_stats(
        waveform,
        frame_index=1,
        threshold=args.threshold,
        total_peaks_before=0,
        started_at=time.monotonic(),
        polarity=args.polarity,
        min_distance_samples=min_distance_samples,
        baseline_mode=args.baseline,
        interval_seconds=args.interval,
        detection_mode=args.detection_mode,
        min_peak_width_s=resolve_min_peak_width_s(args),
        max_peak_width_s=resolve_max_peak_width_s(args),
    )
    print(f"file={args.test_csv}")
    print(f"samples={stats.samples}")
    print(f"time_step={stats.time_step}")
    print(f"voltage_min={stats.voltage_min}")
    print(f"voltage_max={stats.voltage_max}")
    print(f"baseline={stats.baseline}")
    print(f"threshold={args.threshold}")
    print(f"polarity={args.polarity}")
    print(f"min_distance_samples={min_distance_samples}")
    print(f"waveform_span={stats.waveform_span}")
    print(f"counts={stats.frame_peaks}")
    print(f"first_count_time={stats.first_peak_time}")
    return 0


def find_csv_events_for_plot(args: argparse.Namespace, waveform: Waveform) -> tuple[list[float], list[float], float]:
    baseline = estimate_baseline(waveform.values, args.baseline)
    threshold_voltage = baseline + args.threshold if args.polarity == "positive" else baseline - args.threshold
    min_distance_samples = resolve_min_distance_samples(
        waveform,
        args.min_distance_samples,
        args.holdoff_ns,
    )
    events = find_events(
        waveform.values,
        threshold_voltage,
        times=waveform.times,
        min_distance_samples=min_distance_samples,
        polarity=args.polarity,
        detection_mode=args.detection_mode,
        min_peak_width_s=resolve_min_peak_width_s(args),
        max_peak_width_s=resolve_max_peak_width_s(args),
    )
    event_times = [event.time for event in events if event.time is not None]
    event_voltages = [waveform.values[event.index] for event in events]
    return event_times, event_voltages, baseline


def run_csv_plot(args: argparse.Namespace) -> int:
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib is not installed; run: python -m pip install matplotlib")
        return 2

    waveform = read_waveform_csv_file(args.plot_csv)
    event_times, event_voltages, baseline = find_csv_events_for_plot(args, waveform)
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
    if event_times:
        plt.scatter(
            event_times,
            event_voltages,
            color="tab:orange",
            zorder=3,
            label=f"counts: {len(event_times)}",
        )
    plt.xlabel("Time (s)")
    plt.ylabel("CH1 (V)")
    plt.title(f"{args.plot_csv}: {len(event_times)} counts")
    plt.legend()
    plt.tight_layout()
    if args.plot_out:
        plt.savefig(args.plot_out, dpi=150)
        print(f"saved plot: {args.plot_out}")
    else:
        plt.show()
    return 0


def iter_thresholds(start: float, stop: float, step: float) -> list[float]:
    if step <= 0:
        raise ValueError("--plateau-step must be greater than zero")
    if stop < start:
        raise ValueError("--plateau-stop must be greater than or equal to --plateau-start")

    thresholds: list[float] = []
    value = start
    while value <= stop + step * 1e-9:
        thresholds.append(value)
        value += step
    return thresholds


@dataclass(frozen=True)
class PlateauPoint:
    threshold: float
    counts: int
    frames: int
    errors: int
    elapsed: float
    baseline: float
    comparator: float
    waveform_span: float | None

    @property
    def counts_per_s(self) -> float:
        return self.counts / max(self.elapsed, 1e-9)


def collect_plateau_points(
    args: argparse.Namespace,
    thresholds: list[float],
    *,
    show_progress: bool = False,
) -> list[PlateauPoint]:
    dummy_source = stream_dummy_waveforms(args.dummy_csv, args.interval) if args.dummy else None
    points: list[PlateauPoint] = []

    for threshold_index, threshold in enumerate(thresholds, start=1):
        if show_progress:
            print(
                f"plateau {threshold_index}/{len(thresholds)}: "
                f"threshold={threshold:.9g} V, gate={args.plateau_gate:g} s",
                flush=True,
            )
        gate_started = time.monotonic()
        deadline = gate_started + args.plateau_gate
        frames = 0
        errors = 0
        counts = 0
        last_analysis: FrameAnalysis | None = None

        while True:
            try:
                waveform = (
                    next(dummy_source)
                    if dummy_source is not None
                    else read_waveform(
                        args.host,
                        args.port,
                        channel=args.channel,
                        timeout=args.timeout,
                        backend=args.backend,
                        visa_backend=args.visa_backend,
                        waveform_mode=args.waveform_mode,
                        waveform_points=args.waveform_points,
                        waveform_points_mode=args.waveform_points_mode,
                        waveform_start=args.waveform_start,
                        waveform_stop=args.waveform_stop,
                        acquisition_memory_depth=args.acquire_memory_depth,
                        stop_read_run=args.stop_read_run,
                        run_stop_read=args.run_stop_read,
                        acquire_seconds=args.acquire_seconds,
                        stop_settle=args.stop_settle,
                    )
                )
            except Exception as exc:
                errors += 1
                print(f"threshold {threshold:.9g}: read failed ({exc}); retrying", flush=True)
                if time.monotonic() >= deadline:
                    break
                time.sleep(max(args.interval, 0.05))
                continue

            if args.csv:
                write_waveform_csv(args.csv, waveform)
            min_distance_samples = resolve_min_distance_samples(
                waveform,
                args.min_distance_samples,
                args.holdoff_ns,
            )
            analysis = analyze_frame(
                waveform,
                frames + 1,
                threshold,
                counts,
                gate_started,
                args.polarity,
                min_distance_samples,
                args.baseline,
                args.interval,
                detection_mode=args.detection_mode,
                min_peak_width_s=resolve_min_peak_width_s(args),
                max_peak_width_s=resolve_max_peak_width_s(args),
            )
            frames += 1
            counts += analysis.stats.counted_peaks
            last_analysis = analysis
            if time.monotonic() >= deadline:
                break
            time.sleep(max(args.interval, 0.0))

        elapsed = max(time.monotonic() - gate_started, 1e-9)
        point = PlateauPoint(
            threshold=threshold,
            counts=counts,
            frames=frames,
            errors=errors,
            elapsed=elapsed,
            baseline=float("nan") if last_analysis is None else last_analysis.baseline,
            comparator=float("nan") if last_analysis is None else last_analysis.threshold_voltage,
            waveform_span=None if last_analysis is None else last_analysis.stats.waveform_span,
        )
        points.append(point)
        if show_progress:
            print(
                f"plateau {threshold_index}/{len(thresholds)} done: "
                f"counts={point.counts}, frames={point.frames}, errors={point.errors}, "
                f"rate={point.counts_per_s:.2f} Hz",
                flush=True,
            )

    return points


def select_plateau_threshold(points: list[PlateauPoint], tolerance: float) -> tuple[float, list[PlateauPoint]]:
    usable = [point for point in points if point.frames > 0 and point.counts > 0]
    if not usable:
        raise ValueError("no nonzero plateau points; check baseline, polarity, and threshold range")

    best_run = [usable[0]]
    current_run = [usable[0]]
    for previous, current in zip(usable, usable[1:]):
        reference = max(previous.counts, current.counts, 1)
        relative_change = abs(current.counts - previous.counts) / reference
        if relative_change <= tolerance:
            current_run.append(current)
        else:
            if len(current_run) > len(best_run):
                best_run = current_run
            current_run = [current]
    if len(current_run) > len(best_run):
        best_run = current_run

    middle = best_run[len(best_run) // 2]
    return middle.threshold, best_run


def write_plateau_csv(path: str | Path, points: list[PlateauPoint]) -> None:
    with Path(path).open("w", newline="") as output_file:
        writer = csv.writer(output_file)
        writer.writerow(
            [
                "threshold_V",
                "counts",
                "frames",
                "errors",
                "gate_s",
                "counts_per_s",
                "last_baseline_V",
                "last_comparator_level_V",
                "last_waveform_span_s",
            ]
        )
        for point in points:
            writer.writerow(
                [
                    point.threshold,
                    point.counts,
                    point.frames,
                    point.errors,
                    point.elapsed,
                    point.counts_per_s,
                    point.baseline,
                    point.comparator,
                    point.waveform_span,
                ]
            )


def print_plateau_points(points: list[PlateauPoint]) -> None:
    print("threshold_V\tcounts\tframes\terrors\tgate_s\tcounts_per_s\tlast_baseline_V\tlast_comparator_level_V")
    for point in points:
        print(
            f"{point.threshold:.9g}\t{point.counts}\t{point.frames}\t{point.errors}\t{point.elapsed:.3f}\t"
            f"{point.counts_per_s:.2f}\t{point.baseline:.9g}\t{point.comparator:.9g}"
        )


def run_plateau_scan(args: argparse.Namespace) -> int:
    thresholds = iter_thresholds(args.plateau_start, args.plateau_stop, args.plateau_step)

    try:
        points = collect_plateau_points(args, thresholds, show_progress=True)
        print_plateau_points(points)
        selected, plateau = select_plateau_threshold(points, args.plateau_tolerance)
        print(
            f"selected_threshold={selected:.9g} V "
            f"plateau={plateau[0].threshold:.9g}..{plateau[-1].threshold:.9g} V "
            f"tolerance={args.plateau_tolerance:.3g}"
        )
        if args.plateau_csv:
            write_plateau_csv(args.plateau_csv, points)
    except KeyboardInterrupt:
        print("plateau scan interrupted")
        return 130
    except Exception as exc:
        print(f"plateau scan failed: {exc}")
        return 2
    return 0


def apply_auto_threshold(args: argparse.Namespace) -> bool:
    thresholds = iter_thresholds(args.plateau_start, args.plateau_stop, args.plateau_step)
    print("auto-threshold: running plateau scan")
    try:
        points = collect_plateau_points(args, thresholds, show_progress=True)
        print_plateau_points(points)
        selected, plateau = select_plateau_threshold(points, args.plateau_tolerance)
    except Exception as exc:
        print(f"auto-threshold failed: {exc}")
        return False

    if args.plateau_csv:
        write_plateau_csv(args.plateau_csv, points)
    args.threshold = selected
    print(
        f"auto-threshold selected {selected:.9g} V "
        f"from plateau {plateau[0].threshold:.9g}..{plateau[-1].threshold:.9g} V"
    )
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Stream oscilloscope waveforms over LAN SCPI.")
    parser.add_argument("--host", help="Oscilloscope IPv4 address.")
    parser.add_argument("--backend", choices=("raw", "visa"), default="raw", help="SCPI transport backend.")
    parser.add_argument("--visa-backend", default="@py", help="PyVISA backend, e.g. @py.")
    parser.add_argument("--port", type=int, default=5025, help="SCPI TCP port.")
    parser.add_argument("--channel", default="CHAN1", help="Oscilloscope channel name.")
    parser.add_argument(
        "--waveform-mode",
        choices=("NORM", "RAW", "MAX"),
        default="NORM",
        type=str.upper,
        help="Oscilloscope waveform readout mode. NORM is usually screen points; RAW/MAX can use acquisition memory.",
    )
    parser.add_argument("--waveform-points", type=int, help="Requested waveform points, e.g. 10000 or 100000.")
    parser.add_argument("--waveform-start", type=int, help="First waveform point to read; defaults to 1 when --waveform-points is set.")
    parser.add_argument("--waveform-stop", type=int, help="Last waveform point to read; defaults to --waveform-points when set.")
    parser.add_argument(
        "--acquire-memory-depth",
        help="Set oscilloscope acquisition memory depth before reading, e.g. AUTO, 10M, 20M, 200M.",
    )
    parser.add_argument(
        "--stop-read-run",
        action="store_true",
        help="For each frame: stop acquisition, read waveform memory, then run again.",
    )
    parser.add_argument(
        "--run-stop-read",
        action="store_true",
        help=(
            "For each frame: run acquisition, wait --acquire-seconds, stop, read waveform memory, "
            "then run again. Use this for long RAW pseudo-live frames."
        ),
    )
    parser.add_argument(
        "--acquire-seconds",
        type=float,
        default=0.0,
        help="Seconds to acquire before STOP in --run-stop-read mode.",
    )
    parser.add_argument(
        "--stop-settle",
        type=float,
        default=0.05,
        help="Seconds to wait after STOP before waveform read in --stop-read-run/--run-stop-read modes.",
    )
    parser.add_argument(
        "--waveform-points-mode",
        choices=("NORM", "RAW", "MAX"),
        type=str.upper,
        help="Requested waveform points mode for oscilloscopes that support WAV:POIN:MODE.",
    )
    parser.add_argument("--threshold", type=float, default=0.2, help="Comparator level relative to baseline in volts.")
    parser.add_argument("--polarity", choices=("positive", "negative"), default="positive", help="Pulse polarity.")
    parser.add_argument(
        "--baseline",
        choices=("none", "median", "edges"),
        default="none",
        help="Estimate baseline before comparator detection.",
    )
    parser.add_argument(
        "--min-distance-samples",
        type=int,
        default=1,
        help="Detector holdoff/dead time in oscilloscope samples.",
    )
    parser.add_argument(
        "--holdoff-ns",
        type=float,
        help="Detector holdoff/dead time in ns; overrides --min-distance-samples for each waveform.",
    )
    parser.add_argument(
        "--detection-mode",
        choices=("crossing", "threshold-width", "above-threshold-samples"),
        default="crossing",
        help=(
            "Count mode: crossing uses holdoff; threshold-width counts threshold regions "
            "with width filtering; above-threshold-samples counts every sample past threshold."
        ),
    )
    parser.add_argument(
        "--max-peak-width-ns",
        type=float,
        help="Maximum threshold-level pulse width in ns for --detection-mode threshold-width.",
    )
    parser.add_argument(
        "--min-peak-width-ns",
        type=float,
        default=5.0,
        help="Minimum threshold-level pulse width in ns for --detection-mode threshold-width.",
    )
    parser.add_argument("--interval", type=float, default=0.5, help="Delay between waveform reads.")
    parser.add_argument(
        "--frames",
        type=parse_frames,
        help="Stop after this many waveform frames, or use 'live' for an endless capture/read loop.",
    )
    parser.add_argument("--csv", help="Write each latest waveform frame to this CSV path.")
    parser.add_argument("--test-csv", help="Read one waveform CSV file and count peaks without connecting to the oscilloscope.")
    parser.add_argument("--plot-csv", help="Plot one waveform CSV file and mark counted crossings.")
    parser.add_argument("--plot-out", help="Save --plot-csv figure to an image file instead of showing a window.")
    parser.add_argument("--gui", action="store_true", help="Show a live counter window.")
    parser.add_argument("--auto-threshold", action="store_true", help="Run plateau scan first and use the selected threshold.")
    parser.add_argument("--plateau", action="store_true", help="Sweep comparator threshold and print counts table.")
    parser.add_argument("--plateau-start", type=float, default=0.005, help="Plateau scan start threshold in volts.")
    parser.add_argument("--plateau-stop", type=float, default=0.05, help="Plateau scan stop threshold in volts.")
    parser.add_argument("--plateau-step", type=float, default=0.005, help="Plateau scan threshold step in volts.")
    parser.add_argument("--plateau-gate", type=float, default=1.0, help="Counting time per threshold in seconds.")
    parser.add_argument("--plateau-tolerance", type=float, default=0.15, help="Relative count tolerance for plateau selection.")
    parser.add_argument("--plateau-csv", help="Write plateau scan table to this CSV path.")
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

    if args.detection_mode == "threshold-width" and args.max_peak_width_ns is None:
        parser.error("--max-peak-width-ns is required with --detection-mode threshold-width")
    if args.stop_read_run and args.run_stop_read:
        parser.error("--stop-read-run and --run-stop-read cannot be used together")
    if args.acquire_seconds < 0:
        parser.error("--acquire-seconds must be non-negative")

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

    if args.plateau:
        return run_plateau_scan(args)

    if args.auto_threshold and not apply_auto_threshold(args):
        return 2

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
                waveform_mode=args.waveform_mode,
                waveform_points=args.waveform_points,
                waveform_points_mode=args.waveform_points_mode,
                waveform_start=args.waveform_start,
                waveform_stop=args.waveform_stop,
                acquisition_memory_depth=args.acquire_memory_depth,
                stop_read_run=args.stop_read_run,
                run_stop_read=args.run_stop_read,
                acquire_seconds=args.acquire_seconds,
                stop_settle=args.stop_settle,
            )
        )
        for frame_index, waveform in enumerate(waveform_source, start=1):
            min_distance_samples = resolve_min_distance_samples(
                waveform,
                args.min_distance_samples,
                args.holdoff_ns,
            )
            stats = calculate_frame_stats(
                waveform,
                frame_index,
                args.threshold,
                total_peaks,
                started_at,
                args.polarity,
                min_distance_samples,
                args.baseline,
                args.interval,
                detection_mode=args.detection_mode,
                min_peak_width_s=resolve_min_peak_width_s(args),
                max_peak_width_s=resolve_max_peak_width_s(args),
                count_frame=not (args.dedupe_frames and waveform_signature(waveform) == last_signature),
            )
            total_peaks = stats.total_peaks
            last_signature = waveform_signature(waveform)
            if args.csv:
                write_waveform_csv(args.csv, waveform)
            time_start = waveform.times[0] if waveform.times else None
            time_stop = waveform.times[-1] if waveform.times else None
            comparator = stats.baseline + args.threshold if args.polarity == "positive" else stats.baseline - args.threshold
            print(
                f"frame={frame_index} samples={stats.samples} "
                f"counted={stats.counted_peaks} crossings={stats.frame_peaks} "
                f"time_start={time_start} time_stop={time_stop} dt={stats.time_step} "
                f"waveform_span={stats.waveform_span} "
                f"baseline={stats.baseline} comparator={comparator} "
                f"voltage_min={stats.voltage_min} voltage_max={stats.voltage_max} "
                f"rate_hz={stats.count_rate_hz:.2f} first_count_time={stats.first_peak_time}"
            )
            if args.frames is not None and frame_index >= args.frames:
                break
    except Exception as exc:
        print(f"stream failed: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
