"""Tests"""

from bbp_workflow.slurm import _process_alloc_params
from bbp_workflow.task import SlurmCfg


def test_default():
    """Test exclusive."""
    worker_count = None
    slurm_cfg = SlurmCfg()
    slurm_cfg = _process_alloc_params(worker_count, slurm_cfg)
    assert not slurm_cfg.exclusive
    assert slurm_cfg.ntasks == 1
    assert slurm_cfg.nodes == 1
    assert slurm_cfg.cpus_per_task == 0


def test_default_workers():
    """Test exclusive."""
    worker_count = 5
    slurm_cfg = SlurmCfg()
    slurm_cfg = _process_alloc_params(worker_count, slurm_cfg)
    assert not slurm_cfg.exclusive
    assert slurm_cfg.ntasks == 1
    assert slurm_cfg.ntasks_per_node == 0
    assert slurm_cfg.nodes == 1
    assert slurm_cfg.cpus_per_task == 5


def test_multi_node_workers():
    """Test exclusive multi-node."""
    worker_count = 5
    slurm_cfg = SlurmCfg(nodes=10)
    slurm_cfg = _process_alloc_params(worker_count, slurm_cfg)
    assert not slurm_cfg.exclusive
    assert slurm_cfg.ntasks == 0
    assert slurm_cfg.ntasks_per_node == 1
    assert slurm_cfg.nodes == 10
    assert slurm_cfg.cpus_per_task == 5, "Cpus based on worker_count"


def test_multi_node_workers_custom_cpus():
    """Test exclusive multi-node."""
    worker_count = 5
    slurm_cfg = SlurmCfg(nodes=10, cpus_per_task=3)
    slurm_cfg = _process_alloc_params(worker_count, slurm_cfg)
    assert not slurm_cfg.exclusive
    assert slurm_cfg.ntasks == 0
    assert slurm_cfg.ntasks_per_node == 1
    assert slurm_cfg.nodes == 10
    assert slurm_cfg.cpus_per_task == 3, "Custom cpus is preserved"


def test_exclusive_node():
    """Test exclusive."""
    worker_count = None
    slurm_cfg = SlurmCfg(exclusive=True)
    slurm_cfg = _process_alloc_params(worker_count, slurm_cfg)
    assert slurm_cfg.ntasks == 1
    assert slurm_cfg.nodes == 1


def test_exclusive_multi_node():
    """Test exclusive multi-node."""
    worker_count = None
    slurm_cfg = SlurmCfg(nodes=10, exclusive=True)
    slurm_cfg = _process_alloc_params(worker_count, slurm_cfg)
    assert slurm_cfg.ntasks == 0
    assert slurm_cfg.ntasks_per_node == 1
    assert slurm_cfg.nodes == 10
    assert slurm_cfg.cpus_per_task == 0


def test_exclusive_multi_node_workers():
    """Test exclusive multi-node."""
    worker_count = 5
    slurm_cfg = SlurmCfg(nodes=10, exclusive=True)
    slurm_cfg = _process_alloc_params(worker_count, slurm_cfg)
    assert slurm_cfg.exclusive
    assert slurm_cfg.ntasks == 0
    assert slurm_cfg.ntasks_per_node == 1
    assert slurm_cfg.nodes == 10
    assert slurm_cfg.cpus_per_task == 0
