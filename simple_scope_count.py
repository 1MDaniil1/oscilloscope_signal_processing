from __future__ import annotations

import argparse
import csv
import statistics
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Preamble:
    points: int
    x_increment: float
    x_origin: float
    y_increment: float
    y_origin: float
    y_reference: float


def visa_name(host: str) -> str:
    if "::" in host:
        return host
    return f"TCPIP::{host}::INSTR"


def read_definite_block(instrument: object) -> bytes:
    start = instrument.read_bytes(1)
    if start != b"#":
        rest = start + instrument.read_raw()
        raise RuntimeError(f"expected SCPI binary block, got {rest[:80]!r}")

    ndigits = int(instrument.read_bytes(1).decode("ascii"))
    if ndigits <= 0:
        raise RuntimeError("indefinite SCPI blocks are not supported")

    size = int(instrument.read_bytes(ndigits).decode("ascii"))
    payload = instrument.read_bytes(size)

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

    return payload


def parse_preamble(raw: str) -> Preamble:
    fields = [field.strip().strip('"') for field in raw.split(",")]
    nums = [float(field) if field else 0.0 for field in fields]
    if len(nums) < 10:
        raise RuntimeError(f"unsupported preamble: {raw!r}")

    # RIGOL common format:
    # format,type,points,count,xinc,xorigin,xref,yinc,yorigin,yref
    return Preamble(
        points=int(nums[2]),
        x_increment=nums[4],
        x_origin=nums[5],
        y_increment=nums[7],
        y_origin=nums[8],
        y_reference=nums[9],
    )


def voltage_from_byte(sample: int, preamble: Preamble) -> float:
    return (sample - preamble.y_reference) * preamble.y_increment + preamble.y_origin


def estimate_baseline(payload: bytes, preamble: Preamble, mode: str, explicit_value: float) -> float:
    if mode == "none":
        return 0.0
    if mode == "value":
        return explicit_value
    if not payload:
        return 0.0
    if mode == "edges":
        edge_count = max(1, min(len(payload) // 10, 100_000))
        edge_samples = payload[:edge_count] + payload[-edge_count:]
        return statistics.median(voltage_from_byte(sample, preamble) for sample in edge_samples)
    if mode == "median":
        return statistics.median(voltage_from_byte(sample, preamble) for sample in payload)
    raise RuntimeError(f"unsupported baseline mode: {mode}")


def count_above_threshold(payload: bytes, preamble: Preamble, comparator: float, polarity: str) -> int:
    if polarity == "positive":
        return sum(1 for sample in payload if voltage_from_byte(sample, preamble) > comparator)
    return sum(1 for sample in payload if voltage_from_byte(sample, preamble) < comparator)


def write_csv(path: str | Path, payload: bytes, preamble: Preamble) -> None:
    with Path(path).open("w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Time(s)", "Voltage(V)"])
        for index, sample in enumerate(payload):
            writer.writerow(
                [
                    preamble.x_origin + index * preamble.x_increment,
                    voltage_from_byte(sample, preamble),
                ]
            )


def configure_waveform(
    instrument: object,
    *,
    channel: str,
    mode: str,
    points: int | None,
    start: int | None,
    stop: int | None,
    points_mode: str | None,
) -> None:
    instrument.write(f":WAV:SOUR {channel}")
    instrument.write(f":WAV:MODE {mode}")
    instrument.write(":WAV:FORM BYTE")
    instrument.write(":WAV:BYT LSBF")
    if points_mode:
        instrument.write(f":WAV:POIN:MODE {points_mode}")
    if points:
        instrument.write(f":WAV:POIN {points}")
    if start is not None:
        instrument.write(f":WAV:STAR {start}")
    if stop is not None:
        instrument.write(f":WAV:STOP {stop}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Read one oscilloscope waveform and count samples past threshold.")
    parser.add_argument("--host", required=True, help="Oscilloscope IP or VISA resource.")
    parser.add_argument("--channel", default="CHAN1", help="Example: CHAN1 or CHAN2.")
    parser.add_argument("--mode", choices=("NORM", "RAW", "MAX"), default="RAW", help="Waveform mode.")
    parser.add_argument("--points-mode", choices=("NORM", "RAW", "MAX"), help="WAV:POIN:MODE value.")
    parser.add_argument("--points", type=int, help="Requested number of points.")
    parser.add_argument("--start", type=int, help="First waveform point.")
    parser.add_argument("--stop", type=int, help="Last waveform point.")
    parser.add_argument("--threshold", type=float, required=True, help="Threshold relative to baseline, volts.")
    parser.add_argument("--polarity", choices=("positive", "negative"), default="positive")
    parser.add_argument("--baseline", choices=("none", "edges", "median", "value"), default="none")
    parser.add_argument("--baseline-value", type=float, default=0.0, help="Used with --baseline value.")
    parser.add_argument("--timeout", type=float, default=60.0, help="VISA timeout, seconds.")
    parser.add_argument("--visa-backend", default="@py")
    parser.add_argument("--stop-first", action="store_true", help="Send STOP before reading.")
    parser.add_argument("--run-seconds", type=float, help="Send RUN, wait this many seconds, then STOP and read.")
    parser.add_argument("--csv", help="Optional CSV output. Avoid for huge frames.")
    args = parser.parse_args()

    if args.points and args.start is None and args.stop is None:
        args.start = 1
        args.stop = args.points

    try:
        import pyvisa
    except ImportError:
        print("pyvisa is not installed. Run: python -m pip install pyvisa pyvisa-py")
        return 2

    started = time.monotonic()
    rm = pyvisa.ResourceManager(args.visa_backend)
    instrument = rm.open_resource(visa_name(args.host))
    instrument.timeout = int(args.timeout * 1000)
    try:
        idn = str(instrument.query("*IDN?")).strip()
        if args.run_seconds is not None:
            instrument.write(":RUN")
            time.sleep(max(args.run_seconds, 0.0))
            instrument.write(":STOP")
            time.sleep(0.05)
        elif args.stop_first:
            instrument.write(":STOP")
            time.sleep(0.05)

        configure_waveform(
            instrument,
            channel=args.channel,
            mode=args.mode,
            points=args.points,
            start=args.start,
            stop=args.stop,
            points_mode=args.points_mode,
        )
        preamble = parse_preamble(str(instrument.query(":WAV:PRE?")).strip())
        read_started = time.monotonic()
        instrument.write(":WAV:DATA?")
        payload = read_definite_block(instrument)
        read_seconds = time.monotonic() - read_started
    finally:
        try:
            instrument.close()
        finally:
            rm.close()

    baseline = estimate_baseline(payload, preamble, args.baseline, args.baseline_value)
    comparator = baseline + args.threshold if args.polarity == "positive" else baseline - args.threshold
    count = count_above_threshold(payload, preamble, comparator, args.polarity)
    span = preamble.x_increment * max(len(payload) - 1, 0)

    if args.csv:
        write_csv(args.csv, payload, preamble)

    print(f"idn={idn}")
    print(f"samples={len(payload)}")
    print(f"dt_s={preamble.x_increment:.12g}")
    print(f"span_s={span:.12g}")
    print(f"baseline_V={baseline:.12g}")
    print(f"threshold_relative_V={args.threshold:.12g}")
    print(f"comparator_V={comparator:.12g}")
    print(f"counted_samples={count}")
    print(f"fraction_above={count / max(len(payload), 1):.12g}")
    print(f"read_seconds={read_seconds:.6g}")
    print(f"total_seconds={time.monotonic() - started:.6g}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
