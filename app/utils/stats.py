from typing import List, Optional, Union

import numpy as np
from pydantic import StrictFloat, StrictInt

from app.models.pydantic.statistics import Histogram


def _reconstruct_bins(
    min_val: Union[StrictInt, float],
    max_val: Union[StrictInt, float],
    num_bins: StrictInt,
) -> List[Union[StrictInt, float]]:
    return np.linspace(min_val, max_val, num=num_bins)


def _extract_vals(histo: Histogram) -> List[Union[StrictInt, StrictFloat]]:
    reconstructed_bins = _reconstruct_bins(histo.min, histo.max, histo.bin_count)
    reconstructed_values = [
        [d] * c for c, d in zip(histo.value_count, reconstructed_bins)
    ]
    # Return flattened list.
    return [z for s in reconstructed_values for z in s]


def _extract_bin_resolution(reconstructed_values: List[Union[StrictInt, StrictFloat]]):
    return reconstructed_values[1] - reconstructed_values[0]


def _generate_num_bins(minval, maxval, bin_res) -> StrictInt:
    return StrictInt(int(np.ceil((maxval - minval) / bin_res)))


def merge_n_histograms(histos: List[Histogram]) -> Optional[Histogram]:
    """Merge multiple histograms, preserving as much accuracy as possible.

    Code adapted from this Stack Overflow answer:
    https://stackoverflow.com/a/47183002.
    """

    if not histos:
        return None
    if len(histos) == 1:
        return histos[0]

    all_vals: List[Union[StrictInt, StrictFloat]] = []
    for histo in histos:
        all_vals.extend(_extract_vals(histo))

    all_bins = [
        _reconstruct_bins(histo.min, histo.max, histo.bin_count) for histo in histos
    ]

    min_bin_resolution = min([_extract_bin_resolution(bins) for bins in all_bins])
    # print(f"New histogram bin resolution is: {min_bin_resolution}")

    new_min = min(*[histo.min for histo in histos])
    # print(f"New histogram min is: {new_min}")

    new_max = max(*[histo.max for histo in histos])
    # print(f"New histogram max is: {new_max}")

    num_bins = _generate_num_bins(new_min, new_max, min_bin_resolution)
    # print(f"New histogram size is: {num_bins}")

    np_histo = np.histogram(all_vals, bins=num_bins)

    # np.histogram actually produces an array of type numpy.int64, but
    # our Histograms are picky about datatype
    return Histogram(
        min=new_min,
        max=new_max,
        bin_count=num_bins,
        value_count=[StrictInt(x) for x in np_histo[0]],
    )
