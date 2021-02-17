from typing import List, Optional, Union

import numpy as np
from pydantic import StrictFloat, StrictInt

from app.models.pydantic.statistics import Histogram


def _reconstruct_edges(
    min_val: Union[StrictInt, float],
    max_val: Union[StrictInt, float],
    num_bins: StrictInt,
) -> List[Union[StrictInt, float]]:
    """Returns the reconstructed edges of a histogram."""
    return np.linspace(min_val, max_val, num=num_bins)


def _extract_vals(histo: Histogram) -> List[Union[StrictInt, StrictFloat]]:
    """Returns an *approximation* of the original values represented in the
    histogram."""
    reconstructed_edges = _reconstruct_edges(histo.min, histo.max, histo.bin_count)
    reconstructed_values = [
        [d] * c for c, d in zip(histo.value_count, reconstructed_edges)
    ]
    # Return a flattened list.
    return [z for s in reconstructed_values for z in s]


def _extract_bin_resolution(reconstructed_edges: List[Union[StrictInt, StrictFloat]]):
    """Return the 'size' of a bin, assumes bins are uniformly spaced."""
    return reconstructed_edges[1] - reconstructed_edges[0]


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

    # Find the min and max of all histograms to be merged, to be used as the
    # new min and max of the final histogram
    new_min = min(*[histo.min for histo in histos])
    # print(f"New histogram min is: {new_min}")

    new_max = max(*[histo.max for histo in histos])
    # print(f"New histogram max is: {new_max}")

    # Approximate the structure of the original histograms in order to find
    # the overall minimum bin spacing needed to accurately differentiate
    # bins. Use this to figure out how many bins will be required in the
    # final histogram
    all_bins = [
        _reconstruct_edges(histo.min, histo.max, histo.bin_count) for histo in histos
    ]
    min_bin_resolution = min([_extract_bin_resolution(bins) for bins in all_bins])
    # print(f"New histogram bin size is: {min_bin_resolution}")

    num_bins = _generate_num_bins(new_min, new_max, min_bin_resolution)
    # print(f"Number of bins in new histogram is: {num_bins}")

    # Reconstruct approximate values for each feature and create a histogram
    # with the new min, max, and number of bins. Then because the parameters
    # are the same we can add the histogram, element-wise, into a final
    # histogram.
    final_np_histo = np.histogram([], bins=num_bins, range=(new_min, new_max))
    for histo in histos:
        histo_vals = _extract_vals(histo)
        np_histo = np.histogram(histo_vals, bins=num_bins, range=(new_min, new_max))
        for i, new_vals in enumerate(np_histo[0]):
            final_np_histo[0][i] += new_vals

    # np.histogram actually produces an array of type numpy.int64, but
    # our Histograms are picky about datatype
    return Histogram(
        min=new_min,
        max=new_max,
        bin_count=num_bins,
        value_count=[StrictInt(x) for x in final_np_histo[0]],
    )
