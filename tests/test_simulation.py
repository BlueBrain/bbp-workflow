"""Simulation tests."""

import json
from pathlib import Path

import luigi
import numpy as np
from luigi_tools.util import set_luigi_config
from pint import get_application_registry

from bbp_workflow.sbo.sim.task import RunSimCampaign
from bbp_workflow.simulation.task import (
    GenerateCoupledCoordsSimulationCampaign,
    GenerateSimulationCampaign,
    SimCampaign,
    SimulationCampaign,
    _process,
)
from bbp_workflow.simulation.util import xr_from_dict

UREG = get_application_registry()


def test_xr_from_dict():
    """Test xr_from_dict."""
    config_dict = {
        "dims": ["ca", "depolarization"],
        "attrs": {
            "path_prefix": "/tmp/test_ca_dep_scan_campaign_gene0",
            "blue_config_template": "tests/data/BlueConfig.tmpl",
            "user_target": "tests/data/my_user.target",
            "mg": 1.0,
            "duration": 10.0,
            "seed": 1234,
            "target": "test/data/my_user.target",
            "circuit_config": "tests/data/CircuitConfig",
        },
        "data": [["uuid/0", "uuid/1"], ["uuid/2", "uuid/3"]],
        "coords": {
            "ca": {"dims": ["ca"], "attrs": {}, "data": [1.0, 2.0]},
            "depolarization": {"dims": ["depolarization"], "attrs": {}, "data": [3.0, 4.0]},
        },
        "name": "uuid",
    }
    config = xr_from_dict(config_dict)
    assert config.name == "uuid"


def test_xr_from_dict_coupled():
    """Test xr_from_dict coupled."""
    config_dict = {
        "dims": ["coupled"],
        "attrs": {
            "path_prefix": "/tmp/test_coupled_ca_dep_scan_campa0",
            "blue_config_template": "tests/data/BlueConfig.tmpl",
            "user_target": "tests/data/my_user.target",
            "mg": 1.0,
            "duration": 10.0,
            "seed": 1234,
            "target": "test/data/my_user.target",
            "circuit_config": "tests/data/CircuitConfig",
        },
        "data": ["uuid/0", "uuid/1"],
        "coords": {
            "ca": {"dims": ["coupled"], "attrs": {}, "data": [1.0, 2.0]},
            "depolarization": {"dims": ["coupled"], "attrs": {}, "data": [3.0, 4.0]},
        },
        "name": "uuid",
    }
    config = xr_from_dict(config_dict)
    assert config.name == "uuid"


def test_xr_from_json():
    """Test xr_from_dict on the config json file."""
    rnd = np.random.default_rng()
    task = GenerateSimulationCampaign(
        circuit_url="dummy",
        coords={"ca": [1.0, 2.0], "depolarization": [3.0, 4.0]},
        attrs={
            "path_prefix": "path_prefix",
            "blue_config_template": "tests/data/BlueConfig.tmpl",
            "user_target": "tests/data/my_user.target",
            "mg": 1.0,
            "duration": 10.0,
            "seed": 1234,
            "target": "test/data/my_user.target",
        },
    )
    config = task.declare_campaign("uuid", "tests/data/CircuitConfig", rnd)
    config.data = [["uuid/0", "uuid/1"], ["uuid/2", "uuid/3"]]
    array = xr_from_dict(json.loads(Path("tests/data/config.json").read_text("utf8")))
    assert array.to_dict() == config.to_dict()


def test_xr_from_json_coupled():
    """Test xr_from_dict on the config json file."""
    rnd = np.random.default_rng()
    task = GenerateCoupledCoordsSimulationCampaign(
        circuit_url="dummy",
        coords={"ca": [1.0, 2.0], "depolarization": [3.0, 4.0]},
        attrs={
            "path_prefix": "path_prefix",
            "blue_config_template": "tests/data/BlueConfig.tmpl",
            "user_target": "tests/data/my_user.target",
            "mg": 1.0,
            "duration": 10.0,
            "seed": 1234,
            "target": "test/data/my_user.target",
        },
    )
    config = task.declare_campaign("uuid", "tests/data/CircuitConfig", rnd)
    config.data = ["uuid/0", "uuid/1"]
    array = xr_from_dict(json.loads(Path("tests/data/config_coupled.json").read_text("utf8")))
    assert array.to_dict() == config.to_dict()


def test_sim_campaign_cfg():
    """FIXME."""
    luigi_config = luigi.configuration.get_config()
    luigi_config.clear()
    try:
        luigi_config.read("tests/data/sim-campaign.cfg")
        sim_campaign = GenerateSimulationCampaign()
        assert sim_campaign.coords["depolarization"][0].magnitude == 85.0
        assert sim_campaign.coords["ca"].units == UREG.s
        assert sim_campaign.attrs["duration"] == 1000 * UREG.ms
    finally:
        luigi_config.clear()


def test_sim_array_scontrol_output():
    """"""
    scontrol_output = (
        "JobId=123 ArrayJobId=123 ArrayTaskId=0 ArrayTaskThrottle=10 JobName=CortexNrdmsPySim "
        "UserId=user(123) GroupId=bbp(10067) MCS_label=N/A Priority=406 Nice=0 Account=proj30 "
        "QOS=normal JobState=RUNNING Reason=None Dependency=(null) Requeue=1 Restarts=0 "
        "BatchFlag=1 Reboot=0 ExitCode=0:0 RunTime=03:51:56 TimeLimit=06:00:00 TimeMin=N/A "
        "SubmitTime=11:12:18 EligibleTime=11:12:19 AccrueTime=11:12:19 StartTime=11:12:19 "
        "EndTime=17:12:19 Deadline=N/A SuspendTime=None SecsPreSuspend=0 LastSchedEval=11:12:19 "
        "Scheduler=Main Partition=prod AllocNode:Sid=bbpv1:14198 ReqNodeList=(null) "
        "ExcNodeList=(null) NodeList=r2i2n[4-17],r5i2n[0-17] BatchHost=r2i2n4 NumNodes=32 "
        "NumCPUs=2560 NumTasks=1152 CPUs/Task=2 ReqB:S:C:T=0:0:*:* "
        "TRES=cpu=2560,mem=12327872M,node=32,billing=2560 Socks/Node=* NtasksPerN:B:S:C=36:0:*:* "
        "CoreSpec=* MinCPUsNode=72 MinMemoryNode=385246M "
        "MinTmpDiskNode=0 Features=[skl|clx] DelayBoot=00:00:00 OverSubscribe=NO Contiguous=0 "
        "Licenses=(null) Network=(null) Command=(null) WorkDir=/work_dir "
        "StdErr=/work_dir/0/.123_0.log StdIn=/dev/null StdOut=/work_dir/0/.123_0.log Power="
        "\n"
        "JobId=123 ArrayJobId=123 ArrayTaskId=0%10 ArrayTaskThrottle=10 JobName=CortexNrdmsPySim "
        "UserId=user(123) GroupId=bbp(10067) MCS_label=N/A Priority=863 Nice=0 Account=proj30 "
        "QOS=normal JobState=PENDING Reason=None Dependency=(null) Requeue=1 Restarts=0 "
        "BatchFlag=1 Reboot=0 ExitCode=0:0 RunTime=00:00:00 TimeLimit=01:00:00 TimeMin=N/A "
        "SubmitTime=13:15:48 EligibleTime=13:15:51 AccrueTime=13:15:51 StartTime=14:30:00 "
        "EndTime=15:30:00 Deadline=N/A SuspendTime=None SecsPreSuspend=0 LastSchedEval=13:16:48 "
        "Scheduler=Main Partition=prod AllocNode:Sid=bbpv1:13062 ReqNodeList=(null) "
        "ExcNodeList=(null) NodeList=(null) SchedNodeList=r2i0n25 NumNodes=1 NumCPUs=2 NumTasks=1 "
        "CPUs/Task=2 ReqB:S:C:T=0:0:*:* TRES=cpu=2,mem=1547549M,node=1,billing=2 Socks/Node=* "
        "NtasksPerN:B:S:C=0:0:*:* CoreSpec=* MinCPUsNode=2 MinMemoryNode=0 MinTmpDiskNode=0 "
        "Features=[skl|clx] DelayBoot=00:00:00 OverSubscribe=NO Contiguous=0 Licenses=(null) "
        "Network=(null) Command=(null) WorkDir=/work_dir StdErr=/work_dir StdIn=/dev/null "
        "StdOut=/work_dir Power="
    )
    pending, status = _process(scontrol_output)
    assert pending == "0%10"
    assert status[0]["ArrayTaskId"] == "0"
    assert status[0]["JobId"] == "123"


def test_sim_param_overrides():
    """Test sim param overrides."""
    with set_luigi_config(
        {"SimulationCampaign": {"simulation-type": "MultiscaleNrdmsPySim"}},
        configfile="tmp_luigi.cfg",
    ):
        task = SimCampaign()
        # pylint: disable=protected-access
        sim_campaign_task, sim_task = task._task_instances(True, SimulationCampaign)
        assert sim_campaign_task.ntasks_per_node == 0
        assert sim_task.ntasks_per_node == 0
    with set_luigi_config(
        {
            "SimulationCampaign": {"simulation-type": "MultiscaleNrdmsPySim"},
            "MultiscaleNrdmsPySim": {"ntasks-per-node": 10},
        },
        configfile="tmp_luigi.cfg",
    ):
        task = SimCampaign()
        # pylint: disable=protected-access
        sim_campaign_task, sim_task = task._task_instances(True, SimulationCampaign)
        assert sim_campaign_task.ntasks_per_node == 0
        assert sim_task.ntasks_per_node == 0
    # RunSimCampaign should not override params from sim type task
    with set_luigi_config(
        {"RunSimCampaign": {"simulation-type": "MultiscaleNrdmsPySim"}}, configfile="tmp_luigi.cfg"
    ):
        task = RunSimCampaign()
        # pylint: disable=protected-access
        sim_campaign_task, sim_task = task._task_instances(True, RunSimCampaign)
        assert not hasattr(sim_campaign_task, "ntasks_per_node")
        assert sim_task.ntasks_per_node == 32
    with set_luigi_config(
        {
            "RunSimCampaign": {"simulation-type": "MultiscaleNrdmsPySim"},
            "MultiscaleNrdmsPySim": {"ntasks-per-node": 10},
        },
        configfile="tmp_luigi.cfg",
    ):
        task = RunSimCampaign()
        # pylint: disable=protected-access
        sim_campaign_task, sim_task = task._task_instances(True, RunSimCampaign)
        assert not hasattr(sim_campaign_task, "ntasks_per_node")
        assert sim_task.ntasks_per_node == 10
