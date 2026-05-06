from __future__ import annotations

import csv
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Peak:
    index: int
    value: float
    time: float | None = None


def count_peaks(values: Sequence[float], threshold: float = float("-inf")) -> int:
    """Count local maxima whose height is greater than threshold.

    A peak is a point that is higher than both neighboring parts of the signal.
    Flat tops are counted as one peak, for example [0, 2, 2, 1] has one peak.
    Boundary points are not counted because they do not have two neighbors.
    """
    return len(find_peaks(values, threshold))


def find_peaks(values: Sequence[float], threshold: float = float("-inf")) -> list[int]:
    """Return indices of local maxima above threshold.

    For a flat-top peak, the returned index is the middle of the plateau.
    """
    peaks: list[int] = []
    n = len(values)
    i = 1

    while i < n - 1:
        if values[i] <= threshold or values[i] <= values[i - 1]:
            i += 1
            continue

        plateau_start = i
        plateau_end = i
        while plateau_end + 1 < n and values[plateau_end + 1] == values[i]:
            plateau_end += 1

        if plateau_end < n - 1 and values[plateau_end] > values[plateau_end + 1]:
            peaks.append((plateau_start + plateau_end) // 2)

        i = plateau_end + 1

    return peaks


def read_oscilloscope_csv(
    path: str | Path,
    time_column: str = "Time(s)",
    value_column: str = "CH1(V)",
) -> tuple[list[float], list[float]]:
    """Read oscilloscope CSV data as separate time and voltage arrays."""
    times: list[float] = []
    values: list[float] = []

    with Path(path).open(newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            times.append(float(row[time_column]))
            values.append(float(row[value_column]))

    return times, values


def count_signal_peaks(
    values: Sequence[float],
    threshold: float,
    *,
    min_distance_samples: int = 1,
    polarity: str = "positive",
) -> int:
    """Count oscilloscope events above threshold."""
    return len(
        find_signal_peaks(
            values,
            threshold,
            min_distance_samples=min_distance_samples,
            polarity=polarity,
        )
    )


def find_signal_peaks(
    values: Sequence[float],
    threshold: float,
    *,
    times: Sequence[float] | None = None,
    min_distance_samples: int = 1,
    polarity: str = "positive",
) -> list[Peak]:
    """Return one peak for each threshold-crossing event.

    This is usually better for oscilloscope data than counting every local
    maximum, because one physical pulse may contain small noisy local maxima.
    For positive pulses, an event is a continuous region where value > threshold
    bounded by samples at or below threshold on both sides. For negative pulses,
    the same rule is applied to regions where value < threshold.
    """
    if times is not None and len(times) != len(values):
        raise ValueError("times and values must have the same length")
    if min_distance_samples < 1:
        raise ValueError("min_distance_samples must be at least 1")
    if polarity not in {"positive", "negative"}:
        raise ValueError("polarity must be 'positive' or 'negative'")

    if polarity == "positive":
        transformed = values
        transformed_threshold = threshold
    else:
        transformed = [-value for value in values]
        transformed_threshold = -threshold

    peaks: list[Peak] = []
    n = len(values)
    i = 0

    while i < n:
        if transformed[i] <= transformed_threshold:
            i += 1
            continue

        region_start = i
        best_index = i
        best_value = transformed[i]

        while i + 1 < n and transformed[i + 1] > transformed_threshold:
            i += 1
            if transformed[i] > best_value:
                best_index = i
                best_value = transformed[i]

        region_end = i
        has_left_crossing = region_start > 0
        has_right_crossing = region_end < n - 1
        if has_left_crossing and has_right_crossing:
            peak = Peak(
                index=best_index,
                value=values[best_index],
                time=None if times is None else times[best_index],
            )
            _append_with_min_distance(peaks, peak, min_distance_samples, polarity)
        i = region_end + 1

    return peaks


def _append_with_min_distance(
    peaks: list[Peak],
    peak: Peak,
    min_distance_samples: int,
    polarity: str,
) -> None:
    if not peaks or peak.index - peaks[-1].index >= min_distance_samples:
        peaks.append(peak)
        return

    current_score = peak.value if polarity == "positive" else -peak.value
    previous_score = peaks[-1].value if polarity == "positive" else -peaks[-1].value
    if current_score > previous_score:
        peaks[-1] = peak


if __name__ == "__main__":
    csv_path = Path("SSPD Signal0.csv")
    threshold = 0.2

    if csv_path.exists():
        signal_times, signal_values = read_oscilloscope_csv(csv_path)
        signal_peaks = find_signal_peaks(
            signal_values,
            threshold,
            times=signal_times,
            min_distance_samples=1,
        )
        print(f"Peak count: {len(signal_peaks)}")
        print(f"First peaks: {signal_peaks[:10]}")
    else:
        signal = [0, 1, 4, 2, 1, 5, 5, 3, 0, 2, 1]
        signal_peaks = find_signal_peaks(signal, threshold=3)
        print(f"Peak count: {len(signal_peaks)}")
        print(f"Peak indices: {[peak.index for peak in signal_peaks]}")
