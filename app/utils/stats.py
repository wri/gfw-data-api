from typing import List, Union

import numpy as np
from pydantic import StrictFloat, StrictInt

from app.models.pydantic.statistics import Histogram


def merge_histograms(histo1: Histogram, histo2: Histogram) -> Histogram:
    """Intelligently merge multiple histograms preserving as much accuracy as
    possible Code adapted from this Stack Overflow answer:

    https://stackoverflow.com/a/47183002.
    """

    def reconstruct_bins(histo: Histogram) -> List[Union[StrictInt, StrictFloat]]:
        return np.linspace(histo.min, histo.max, num=histo.bin_count)

    def extract_vals(histo: Histogram):
        # Recover values based on assumption 1.
        reconstructed_bins = reconstruct_bins(histo)
        reconstructed_values = [
            [d] * c for c, d in zip(histo.value_count, reconstructed_bins)
        ]
        # Return flattened list.
        return [z for s in reconstructed_values for z in s]

    def extract_bin_resolution(values: List[Union[StrictInt, float]]):
        return values[1] - values[0]

    def generate_num_bins(minval, maxval, bin_res):
        # Generate number of bins necessary to satisfy assumption 2
        return int(np.ceil((maxval - minval) / bin_res))

    vals = extract_vals(histo1) + extract_vals(histo2)
    reconstructed_bins1 = reconstruct_bins(histo1)
    reconstructed_bins2 = reconstruct_bins(histo2)
    bin_resolution = min(
        map(extract_bin_resolution, [reconstructed_bins1, reconstructed_bins2])
    )

    new_min = min(histo1.min, histo2.min)
    new_max = max(histo1.max, histo2.max)
    num_bins = generate_num_bins(new_min, new_max, bin_resolution)

    np_histo = np.histogram(vals, bins=num_bins)

    return Histogram(
        min=new_min, max=new_max, bin_count=num_bins, value_count=list(np_histo[0])
    )
