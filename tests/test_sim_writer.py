"""Tests."""

import json
from collections import namedtuple

import numpy as np
import pandas as pd
import pint
import xarray as xr
from entity_management.core import DataDownload
from entity_management.simulation import DetailedCircuit, SimulationCampaignConfiguration

from bbp_workflow.simulation.task import GenerateSimulationCampaign
from bbp_workflow.simulation.util import (
    _campaign_sims_with_indices,
    _indices_to_ranges,
    _sim_indices_to_list,
    _xr_get_multi_index,
    xr_from_dict,
    xr_to_dict,
)

UREG = pint.get_application_registry()


def test_iter_with_index():
    """Test MultiIndex detection."""
    da = xr.DataArray(
        np.array(list(range(6))).reshape(2, 3),
        dims=["dim1", "dim2"],
        coords={"dim1": [2, 1], "dim2": [5, 4, 3]},
    )
    coords_with_indices = list(_campaign_sims_with_indices(da))
    assert all(a[1]["idx"] == a[2] for a in coords_with_indices)
    assert coords_with_indices[0][0]["dim1"] == 2
    assert coords_with_indices[0][1]["dim1_idx"] == 0
    assert coords_with_indices[0][1]["dim1_total"] == 2
    assert coords_with_indices[0][1]["dim2_total"] == 3
    assert coords_with_indices[5][0]["dim1"] == 1
    assert coords_with_indices[5][0]["dim2"] == 3
    assert coords_with_indices[5][1]["dim1_idx"] == 1


def test_is_multi_index():
    """Test MultiIndex detection."""
    da = xr.DataArray(dims=["dim1", "dim2"], coords={"dim1": [1, 2], "dim2": [3, 4]})

    idx = pd.MultiIndex.from_arrays([[1, 2], [3, 4]], names=("dim1", "dim2"))
    da_multi_indix = xr.DataArray([None] * 2, dims="idx", coords={"idx": idx})

    assert not _xr_get_multi_index(da)
    assert _xr_get_multi_index(da_multi_indix) == "idx"


def test_xr_to_dict():
    """Test xarray to dictionary."""
    da = xr.DataArray(dims=["dim1", "dim2"], coords={"dim1": [1, 2], "dim2": [3, 4]})

    restored = xr_from_dict(xr_to_dict(da))
    assert da.equals(restored)


def test_xr_to_dict_multi_index():
    """Test MultiIndex xarray to dictionary."""
    idx = pd.MultiIndex.from_arrays([[1, 2], [3, 4]], names=("dim1", "dim2"))
    da_multi_indix = xr.DataArray([None] * 2, dims="idx", coords={"idx": idx})

    restored = xr_from_dict(xr_to_dict(da_multi_indix))
    assert da_multi_indix.equals(restored)


def _input_mock_sonata():
    return namedtuple("_", "entity")(
        DetailedCircuit(
            circuitConfigPath=DataDownload(
                url="file://libsonata/tests/data/config/circuit_config.json"
            )
        )
    )


def _monkeypatch_task(monkeypatch):
    monkeypatch.setattr(
        GenerateSimulationCampaign, "from_json_str", lambda *_, **__: DataDownload(url=".")
    )
    monkeypatch.setattr(
        GenerateSimulationCampaign, "from_file", lambda *_, **__: DataDownload(url=".")
    )
    monkeypatch.setattr(
        GenerateSimulationCampaign, "publish", classmethod(lambda _self, x, *a, **b: x)
    )
    monkeypatch.setattr(
        SimulationCampaignConfiguration, "get_id", classmethod(lambda _self: "abc/uuid")
    )
    monkeypatch.setattr(
        SimulationCampaignConfiguration, "get_url", classmethod(lambda _self: "xyz/uuid")
    )


def _ca_dep_scan_campaign_generation_sonata(monkeypatch, tmp_path, max_workers=None):
    """Test Ca/Depolarization scan generation task."""
    task = GenerateSimulationCampaign(
        max_workers=max_workers,
        circuit_url="dummy",
        coords={"ca": [1, 2.0] * UREG.ms, "depolarization": [3.0, 4] * UREG.m},
        attrs={
            "path_prefix": str(tmp_path),
            "blue_config_template": "tests/data/simulation_config.tmpl",
            "mg": 1.0 * UREG.mM,
            "duration": 10.0 * UREG.ms,
            "seed": 1234,
        },
    )
    task.input = _input_mock_sonata
    _monkeypatch_task(monkeypatch)
    task.run()
    config = tmp_path / "uuid" / "config.json"
    assert config.exists()
    config = xr_from_dict(json.loads(config.read_text()))
    assert all((tmp_path / sim_path).exists() for sim_path in np.ravel(config.data))


def test_ca_dep_scan_campaign_generation_sonata(monkeypatch, tmp_path):
    """Test Ca/Depolarization scan generation task."""
    _ca_dep_scan_campaign_generation_sonata(monkeypatch, tmp_path)


def test_indices_to_ranges():
    """Test when seed already present in attrs."""
    indices = [1, 2, 3, 7, 8, 10]
    assert _indices_to_ranges(indices) == "1-3,7-8,10"


def test_indices_to_list():
    """Test when seed already present in attrs."""
    assert _sim_indices_to_list(None) == []
    assert _sim_indices_to_list("") == []
    assert _sim_indices_to_list("0") == [0]
    assert _sim_indices_to_list("0-1") == [0, 1]
    assert _sim_indices_to_list("5,1,2-4,5") == [1, 2, 3, 4, 5, 5]
