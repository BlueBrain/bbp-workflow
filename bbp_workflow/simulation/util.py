# SPDX-License-Identifier: Apache-2.0

"""Simulation utils."""

import itertools
from pathlib import Path

import pandas as pd
import xarray as xr


def _xr_get_multi_index(array):
    """Return index name if array coords are specified by MultiIndex else None."""
    k, v = next(iter(array.indexes.items()))
    if isinstance(v, pd.core.indexes.multi.MultiIndex):
        return k
    else:
        return None


def xr_to_dict(array):
    """Return dict representation of the xarray."""
    index_name = _xr_get_multi_index(array)
    if index_name:
        return array.reset_index(index_name).to_dict()
    return array.to_dict()


def xr_from_dict(array_dict):
    """Restore xarray from it's dict representation.

    Args:
        array_dict (dict): Dictionary representation of the simulation configuration,
                           probably loaded from json.

    Returns:
        xarray.DataArray: Simulation configuration.
    """
    array = xr.DataArray.from_dict(array_dict)
    # check if it is MultiIndex
    if len(array_dict["dims"]) == 1:
        index_name_tuple = array_dict["dims"]
        if all(v["dims"] == index_name_tuple for _, v in array_dict["coords"].items()):
            array = array.set_index(**{index_name_tuple[0]: list(array_dict["coords"].keys())})
    return array


def read_config(config_dict, coords_filter=None):
    """Read the simulation campaign configuration from dictionary and filter selected conditions.

    Args:
        config_dict (dict): Simulation campaign configuration as dictionary loaded from nexus or
                            directly from `config.json` file.
        coords_filter (dict): Optional subset of conditions selected for filtering.

    Returns:
        xarray.DataArray: Simulation configuration with filter applied.
    """
    config = xr_from_dict(config_dict)
    if coords_filter:
        # fix scalar values
        coords_filter = {
            k: list(v) if isinstance(v, tuple) else [v]
            for k, v in coords_filter.items()  # pylint: disable=no-member
        }
        config = config.sel(coords_filter)
    return config


def campaign_sims(config, include_empty=False):
    """Iterate simulations from the campaign.

    Campaign sim can be empty if it was filtered out by `coords_filter_func`.

    Args:
        config (xarray.DataArray): Simulation campaign configuration.
        include_empty (bool): If true, all sims are included(even empty ones).

    Yields:
        tuple(dict, str): Simulation conditions and full path to individual simulation.
                          Path can be `None` if sim folder is empty.
    """
    idx_names = config.to_series().index.names  # ex. ('depolarization', 'ca')
    for idx, sim_path in config.to_series().items():  # idx ex. (75.0, 1.15)
        if not isinstance(idx, tuple):
            idx = (idx,)  # if index is atomic(just a number) make tuple out of it
        loc = dict(zip(idx_names, idx))  # loc ex. {'depolarization': 75.0, 'ca': 1.15}
        if not sim_path:  # skip coords_filter_func fildered out sims
            if include_empty:
                yield loc, None
            continue
        sim_path = Path(sim_path)  # support old absolute paths
        if not sim_path.is_absolute():
            sim_path = Path(config.path_prefix) / sim_path
        yield loc, str(sim_path)


def _campaign_sims_with_indices(config):
    """Iterate all simulations from the non-coupled coords campaign with indices.

    Args:
        config (xarray.DataArray): Simulation campaign configuration.

    Yields:
        tuple(dict, dict, str): Simulation conditions with locs/indices and full path to
                                individual simulations.
    """
    assert _xr_get_multi_index(config) is None, "Please provide non-coupled coords sim campaign!"
    idx = 0

    def _iter(series, loc, indices):
        nonlocal idx
        group_by_dim = series.groupby(level=0, sort=False)
        group_by_dim_len = len(group_by_dim)
        for coord_idx, (coord_value, grouped) in enumerate(group_by_dim):
            indices = dict(indices)
            loc = dict(loc)
            dim = series.index.names[0]
            loc[dim] = coord_value
            indices[f"{dim}_idx"] = coord_idx
            indices[f"{dim}_total"] = group_by_dim_len
            if series.index.nlevels > 1:
                grouped = grouped.droplevel(0)
                yield from _iter(grouped, loc, indices)
            else:
                indices["idx"] = idx
                idx += 1
                assert grouped.shape == (1,)
                yield loc, indices, grouped.iloc[0]

    yield from _iter(config.to_series(), {}, {})


def _campaign_sim_indices(config):
    """Non-empty simulation indices of the campaign."""
    return [i for i, (_, path) in enumerate(campaign_sims(config, include_empty=True)) if path]


def _campaign_sim_index_to_coords(config):
    """Index to coords dict of the campaign sims."""
    return {
        i: coords
        for i, (coords, path) in enumerate(campaign_sims(config, include_empty=True))
        if path
    }


def _indices_to_ranges(indices):
    """Sequence of indices to slurm job array ranges."""
    ranges = []
    # diff between enumerated i and consecutive val will be the same(making group key)
    for _, groups in itertools.groupby(enumerate(indices), lambda i_val: i_val[1] - i_val[0]):
        groups = [g[1] for g in groups]  # take second tuple element which is index
        if len(groups) > 1:
            ranges.append(f"{groups[0]}-{groups[-1]}")
        else:
            ranges.append(str(groups[0]))
    assert len(ranges) > 0
    return ",".join(ranges)


def _sim_indices_to_list(vals_or_ranges):
    """Slurm index ranges to list of ints."""
    indices = []
    if vals_or_ranges:
        for val_or_range in vals_or_ranges.split(","):
            if "-" in val_or_range:
                range_from, range_to = val_or_range.split("-")
                indices.extend(list(range(int(range_from), int(range_to) + 1)))
            else:
                indices.append(int(val_or_range))
    return sorted(indices)
