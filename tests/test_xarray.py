"""Task tests."""

import numpy as np
import pandas as pd
import xarray as xr
from luigi import Task

from bbp_workflow.luigi import Requirement, Requires


def test_requirements():
    """Test requirement composition."""

    class _OtherTask(Task):
        def output(self):
            return None

    class _MyTask(Task):
        requires = Requires()
        other = Requirement(_OtherTask)

        def run(self):
            pass

    print(_MyTask().requires())
    # {'other': OtherTask()}


def test_combine_multi_index_data_arrays():
    """Test xarray multidims concat."""
    da = xr.DataArray(
        dims=["param1", "param2", "param3"],
        coords={"param1": [1, 2], "param2": [3, 4], "param3": [5, 6]},
    )
    # idx = pd.MultiIndex.from_arrays([[1, 2], [3, 4]], names=('param1', 'param2'))
    # da_multi_indix = xr.DataArray([None] * 2, dims='idx', coords={'idx': idx})
    shape = (2, 2, 5, 3)
    da = xr.DataArray(
        np.arange(np.prod(shape)).reshape(shape),
        dims=["param1", "param2", "t", "gid"],
        coords={"param1": [1, 2], "param2": [3, 4], "t": [0, 1, 2, 3, 4], "gid": [10, 11, 12]},
    )
    arr0 = xr.DataArray(name="temperature", data=np.arange(4).reshape(2, 2), dims=["x", "y"])
    arr1 = xr.DataArray(name="temperature", data=np.arange(4, 8).reshape(2, 2), dims=["x", "y"])
    arr2 = xr.DataArray(name="temperature", data=np.arange(8, 12).reshape(2, 2), dims=["x", "y"])
    arr3 = xr.DataArray(name="temperature", data=np.arange(12, 16).reshape(2, 2), dims=["x", "y"])
    ds_grid = [[arr0, arr1], [arr2, arr3]]
    arr = xr.combine_nested(ds_grid, concat_dim=["x", "y"])
    da_0_0 = da[0, 0, ...]
    assert da_0_0.equals(da.sel(param1=1, param2=3))
    da_0_1 = da[0, 1, ...]
    assert da_0_1.equals(da.sel(param1=1, param2=4))
    da_1_0 = da[1, 0, ...]
    assert da_1_0.equals(da.sel(param1=2, param2=3))
    da_1_1 = da[1, 1, ...]
    assert da_1_1.equals(da.sel(param1=2, param2=4))
    da_0 = xr.combine_nested([da_0_0, da_0_1], concat_dim=["param2"])
    da_1 = xr.combine_nested([da_1_0, da_1_1], concat_dim=["param2"])
    da_01 = xr.combine_nested([da_0, da_1], concat_dim=["param1"])
    assert da_01.equals(da)

    # import pdb; pdb.set_trace()
    combined = xr.combine_nested(
        [[da_0_0, da_1_0], [da_0_1, da_1_1]], concat_dim=["param2", "param1"]
    )

    # combined = xr.combine_nested([[da_0_0, da_0_1],
    #                               [da_1_0, da_1_1]],
    #                              concat_dim=['param1', 'param2'])
    # import pdb; pdb.set_trace()
    # combined = xr.combine_nested([[da_0_0, da_0_1], [da_1_0, da_1_1]], concat_dim=['param1', 'param2'])
    # combined = xr.combine_nested([[da_1_0, da_1_1], [da_0_0, da_0_1]], concat_dim=['param1', 'param2'])
    # xr_concat(da, dims=['param1', 'param2'])
    assert combined.equals(da)
    # assert da.sel(param1=1, param2=3, t=0, gid=0) == combined.sel(param1=1, param2=3, t=0, gid=0)
    # assert da.sel(param1=2, param2=3, t=0, gid=0) == combined.sel(param1=2, param2=3, t=0, gid=0)
    # assert da.sel(param1=1, param2=4, t=0, gid=0) == combined.sel(param1=1, param2=4, t=0, gid=0)
    # assert da.sel(param1=2, param2=4, t=0, gid=0) == combined.sel(param1=2, param2=4, t=0, gid=0)
