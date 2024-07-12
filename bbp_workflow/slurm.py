# SPDX-License-Identifier: Apache-2.0

"""SLURM utils."""

import os
import re
import subprocess
import sys
from contextlib import contextmanager
from copy import copy
from pathlib import Path

from bbp_workflow.luigi import RemoteContext
from bbp_workflow.settings import DEFAULT_HOST, ENVIRONMENT, L
from bbp_workflow.util import to_env_commands, to_sbatch_params, to_srun_params


class _PPCMD:
    """Lazy print command object."""

    def __init__(self, cmd):
        self.cmd = cmd

    def __str__(self):
        """Format cmd."""
        # sanitize sensitive info from showing up in the logs
        return "\n".join(
            re.sub(r"export (NEXUS_TOKEN|KC_SCR)=.+$", r"# export \1=<MASKED>", s)
            for s in self.cmd.split("\n")
        )


def _check_return_code(process, stdout=None, stderr=None, cmd=None):
    if process.returncode:
        msg = f"Process failed with exit code {process.returncode}"
        if stdout:
            msg += f"\nSTDOUT:{stdout.decode().strip()}"
        if process.stdout and not process.stdout.closed and process.stdout.readable():
            msg += f"\nSTDOUT:{process.stdout.read().decode().strip()}"
        if stderr:
            msg += f"\nSTDERR:{stderr.decode().strip()}"
        if process.stderr and not process.stderr.closed and process.stderr.readable():
            msg += f"\nSTDERR:{process.stderr.read().decode().strip()}"
        if cmd:
            msg += f"\nCOMMAND:\n{cmd}"
        raise RuntimeError(msg)


def _scancel(host, job_id):
    cmd = f"scancel {job_id}"
    L.debug("host: %s command:\n%s", host, _PPCMD(cmd))
    remote = RemoteContext(host)
    process = remote.Popen(["bash", "-l"], stdin=subprocess.PIPE)
    stdout, stderr = process.communicate(cmd.encode())
    _check_return_code(process, stdout, stderr)


def _srun(slurm_cfg, env_cfg, cmd, pre_cmd=None):
    """Assemble srun command."""
    return "\n".join(
        ["set -e"]
        + to_env_commands(env_cfg)
        + ([pre_cmd] if pre_cmd else [])
        + [f"stdbuf -oL -eL srun {to_srun_params(slurm_cfg)} {cmd}"]
    )


def _run_srun(host, srun_cmd, return_result=False):
    """Run a srun command.remote_script`` and ``args``.

    Args:
        host (luigi.Parameter): Head node of the cluster.
        srun_cmd (str): Command to run.
        return_result (bool): If True, output of the ``cmd`` will be captured and returned.

    Returns:
        Result of the command stripped if ``return_result`` was true.
    """
    L.debug("host: %s command:\n%s", host, _PPCMD(srun_cmd))
    remote = RemoteContext(host)
    if return_result:
        process = remote.Popen(
            ["bash", "-l"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate(srun_cmd.encode())
        _check_return_code(process, stdout, stderr)
        return stdout.decode().strip()
    else:
        process = remote.Popen(["bash", "-l"], stdin=subprocess.PIPE)
        stdout, stderr = process.communicate(srun_cmd.encode())
        _check_return_code(process, stdout, stderr)
        return None


def _sbatch(cfgs, env_cfg, pre_cmd=None):
    """Assemble sbatch command from slurm configs.

    If multiple cfgs are given then hetjob sbatch script will be assembled.

    Args:
        cfgs (typing.List[typing.Tuple(SlurmCfg, str)]): slurm configis and commands to launch as
            list of tuples: elements of hetjob.
        env_cfg (EnvCfg): environment configuration for the sbatch.
        pre_cmd (str): command to be executed before srun.
    """
    cmd_str = "\n".join(
        ["set -e"]
        + to_env_commands(env_cfg)
        + ["stdbuf -oL -eL sbatch << 'EOF_SBATCH'", "#!/bin/bash -l"]
    )
    slurm_cfgs = []
    slurm_cfgs += [to_sbatch_params(slurm_cfg) for slurm_cfg, _ in cfgs]
    cmd_str += "\n" + "\n#SBATCH hetjob\n".join(slurm_cfgs)
    if len(cfgs) > 1:
        srun_cmd = " : ".join(f"--het-group={i} {cmd}".strip() for i, (_, cmd) in enumerate(cfgs))
    else:
        srun_cmd = cfgs[0][1]  # take the second component of tuple(which is the command)
    cmd_str = "\n".join(
        [cmd_str]
        + ([pre_cmd] if pre_cmd else [])
        + ([f"srun {srun_cmd}"] if srun_cmd else [])
        + ["EOF_SBATCH"]
    )
    return cmd_str


@contextmanager
def _run_sbatch(host, sbatch_cmd, job_script_path=None, should_cancel=True):
    """Run sbatch command from the arguments.

    Args:
        host (str): remote host.
        sbatch_cmd (str): sbatch script.
        job_script_path (str): path to the job script output file.
        should_cancel (bool): if sbatch is non-blocking set this to False, so the job is not
            canceled immediately.
    """
    L.debug("host: %s command:\n%s", host, _PPCMD(sbatch_cmd))
    process = RemoteContext(host).Popen(
        ["bash", "-l"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    process.stdin.write(sbatch_cmd.encode())
    process.stdin.close()

    job_id = None
    while True:
        line = process.stdout.readline().decode()
        sys.stdout.write(line)
        sys.stdout.flush()
        match = re.match(r"^Submitted batch job (\d+)$", line)
        if match:
            job_id = match.group(1)
            L.debug("Submitted batch job:%s", job_id)
            break
        if process.poll() is not None:
            break
    if job_id is None and process.poll() is not None:
        _check_return_code(process, cmd=str(_PPCMD(sbatch_cmd)))
    assert job_id, "Unable to submit batch job!"
    try:
        if job_script_path is not None:
            (Path(job_script_path) / f".{job_id}.script").write_text(str(_PPCMD(sbatch_cmd)))
        yield job_id, process
        process.wait()
        _check_return_code(process)
    finally:
        if should_cancel:
            _scancel(host, job_id)

    if ENVIRONMENT != "aws":
        was_cancelled = _run_cmd(
            host,
            f'sacct -u {os.environ["USER"]} -s CANCELLED -j {job_id} --noheader --format jobid',
            capture_output=True,
        )
        if was_cancelled:
            raise RuntimeError("Process run was canceled")


def _run_cmd(host, cmd, capture_output=False, env=None):
    """Run cmd on the host and by default return the result.

    Returns:
        Result of the command stripped.
    """
    L.debug("host: %s command:\n%s", host, _PPCMD(cmd))
    remote = RemoteContext(host)
    bash_cmd = ["bash", "-l"]
    if env:

        def _format(k, v):
            v = str(v).replace("\\", "\\\\").replace('"', '\\"')
            return f'{k}="{v}"'

        bash_cmd = ["env", *(_format(*item) for item in env.items()), *bash_cmd]
    if capture_output:
        process = remote.Popen(
            bash_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = process.communicate(cmd.encode())
        _check_return_code(process, stdout, stderr)
        return stdout.decode().strip()
    else:
        process = remote.Popen(bash_cmd, stdin=subprocess.PIPE)
        stdout, stderr = process.communicate(cmd.encode())
        _check_return_code(process, stdout, stderr)
        return None


def _process_alloc_params(worker_count, slurm_cfg):
    """Fix/fill missing slurm params."""
    cfg = copy(slurm_cfg)
    if not slurm_cfg.nodes or int(slurm_cfg.nodes) < 2:
        cfg.nodes = 1  # make sure we use single node allocation
        cfg.ntasks = 1  # run single luigi main process(it will scale with multiprocessing)
    else:
        cfg.ntasks_per_node = 1

    if worker_count and not slurm_cfg.cpus_per_task and not slurm_cfg.exclusive:
        # number of workers explicitly provided but number of cores was not specified
        # and the node is not allocated exclusively
        cfg.cpus_per_task = worker_count  # core per worker

    if not slurm_cfg.job_name:
        cfg.job_name = "wrkflw"

    return cfg


def _allocate_resources(worker_count, slurm_cfg, host_cfg):
    slurm_cfg = _process_alloc_params(worker_count, slurm_cfg)
    cfg = copy(slurm_cfg)  # don't use clone as it won't pick up values from [DEFAULT]
    cfg.mpi = ""  # reset mpi value as salloc doesn't need it
    cfg.job_output = None
    cmd = f"stdbuf -oL -eL salloc {to_srun_params(cfg)} --no-shell"
    L.debug("host: %s command:\n%s", host_cfg.host, _PPCMD(cmd))
    process = RemoteContext(host_cfg.host).Popen(
        ["bash", "-l"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    process.stdin.write(cmd.encode())
    process.stdin.close()
    lines = ""
    while True:
        line = process.stderr.readline().decode().strip()
        match = re.match(r"^salloc: (Pending|Granted) job allocation (\d+)$", line)
        if match:
            job_id = match.group(2)
            L.debug("BB5 workers got allocated job_id: %s", job_id)
            return job_id, slurm_cfg
        lines += f"{line}\n"
        returncode = process.poll()
        if returncode is not None:
            L.error(lines)
            assert returncode == 0, "Error running salloc!"
            return None, slurm_cfg


def _salloc(slurm_cfg):
    cmd = f"stdbuf -oL -eL salloc {to_srun_params(slurm_cfg)} --no-shell"
    L.debug("host: %s command:\n%s", DEFAULT_HOST, _PPCMD(cmd))
    process = RemoteContext(DEFAULT_HOST).Popen(
        ["bash", "-l"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    process.stdin.write(cmd.encode())
    process.stdin.close()
    lines = ""
    while True:
        line = process.stderr.readline().decode().strip()
        match = re.match(r"^salloc: (Pending|Granted) job allocation (\d+)$", line)
        if match:
            job_id = match.group(2)
            L.debug("Salloc job_id: %s", job_id)
            return job_id
        lines += f"{line}\n"
        returncode = process.poll()
        if returncode is not None:
            L.error(lines)
            assert returncode == 0, "Error running salloc!"
            return None
