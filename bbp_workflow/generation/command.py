# SPDX-License-Identifier: Apache-2.0

"""Command module."""

import os
import re
import subprocess
from pathlib import Path

from bbp_workflow.generation import environment
from bbp_workflow.luigi import RemoteContext
from bbp_workflow.settings import HTTPS_PROXY, L
from bbp_workflow.slurm import _check_return_code
from bbp_workflow.task import SlurmCfg
from bbp_workflow.util import map_slurm_params

DEFAULT_ENV_VARS = (
    "NEXUS_TOKEN",
    "NEXUS_WORKFLOW",
    "NEXUS_BASE",
    "NEXUS_USERINFO",
    "NEXUS_ORG",
    "NEXUS_PROJ",
    "NEXUS_NO_PROV",
    "NEXUS_DRY_RUN",
    "KC_SCR",
    "DEBUG",
)

MASKED_ENV_VARS = (
    "NEXUS_TOKEN",
    "KC_SCR",
)


def _mask_token(string, name):
    return re.sub(rf"{name}=(\S+)", f"{name}=${name}", string)


def _mask(string):
    for masked_var in MASKED_ENV_VARS:
        string = _mask_token(string, masked_var)
    return string


def build_command(
    command: dict,
    inputs: dict,
    kg_config: dict,
    env_config: dict,
    slurm_config: dict,
    runtime_arguments: dict = None,
    global_env_vars=DEFAULT_ENV_VARS,
):
    """Construct the final command.

    Args:
        inputs: A dictionary with the input names and values.
        runtime_arguments: Extra arguments to add during runtime.

    Returns: The command as a string.
    """
    str_exports = _build_exports(kg_config, slurm_config, global_env_vars)

    str_command = _build_core_command(
        command=command["base_command"],
        inputs=inputs,
        named_arguments=command["named_arguments"],
        positional_arguments=command["positional_arguments"],
        runtime_arguments=runtime_arguments,
    )

    if slurm_config:
        str_command = _build_salloc_command(slurm_config, str_command)

    if str_exports:
        str_command = f"{str_exports} && {str_command}"

    if env_config:
        str_command = environment.build_environment_command(str_command, env_config)

    return str_command


def _build_exports(kg_config=None, slurm_config=None, global_env_vars=None):
    """Build export string."""
    env_vars = build_runtime_env_vars(kg_config, slurm_config, global_env_vars)

    if not env_vars:
        return ""

    str_export = "export " + " ".join(f"{key}={value}" for key, value in env_vars.items())

    return str_export


def build_runtime_env_vars(kg_config=None, slurm_config=None, global_env_vars=None):
    """Create runtime env vars."""
    env_vars = {}

    if global_env_vars:
        env_vars.update(_build_global_env_vars(global_env_vars))

    # override any nexus globals with kg_config if available
    if kg_config:
        env_vars.update(_build_kg_env_vars(kg_config))

    # always enable internet
    env_vars["https_proxy"] = HTTPS_PROXY

    if slurm_config and "env_vars" in slurm_config:
        env_vars.update(slurm_config["env_vars"])

    return env_vars


def _build_global_env_vars(global_env_vars):
    return {env_var: os.environ[env_var] for env_var in global_env_vars if env_var in os.environ}


def _build_kg_env_vars(kg_config):
    return {
        "NEXUS_BASE": kg_config["kg_base"],
        "NEXUS_ORG": kg_config["kg_org"],
        "NEXUS_PROJ": kg_config["kg_proj"],
    }


def _build_core_command(command, inputs, named_arguments, positional_arguments, runtime_arguments):
    command = list(command)

    for prefix, argument in named_arguments.items():
        command.extend((prefix, inputs[argument]))

    for _, argument in sorted(positional_arguments):
        command.append(inputs[argument])

    if runtime_arguments:
        command.extend(runtime_arguments)

    str_command = " ".join(map(str, command))

    return str_command


def _build_salloc_command(slurm_config: dict, cmd: str):

    params = map_slurm_params(SlurmCfg(**slurm_config))
    str_params = " ".join(params)

    # escape single quotes
    cmd = _escape_single_quotes(cmd.replace("'", "'\\''"))

    return f"stdbuf -oL -eL salloc {str_params} srun --mpi=none sh -c '{cmd}'"


def _escape_single_quotes(value):
    """Return the given string after escaping the single quote character."""
    return value.replace("'", "'\\''")


def run_command(command: str, host_config=None, output_file=None):
    """Run command."""
    if host_config:
        return _run_remote_salloc_command(command, host_config["host"], output_file=output_file)
    else:
        raise NotImplementedError


def _run_remote_salloc_command(command, host, output_file: Path | None = None):

    command = f"set -e && {command}"

    process = RemoteContext(host).Popen(
        ["bash -l"],
        stdin=subprocess.PIPE,
    )

    if output_file:
        L.info("\n\nOutput log at %s\n", output_file)
        command = _redirect_to_file(command, output_file)
        _write_command_to_file(_mask(command), output_file)

    masked_command = _mask(command)
    L.debug("command:\n%s", masked_command)
    stdout, stderr = process.communicate(command.encode())

    _check_return_code(process, stdout, stderr, cmd=masked_command)

    return process


def _write_command_to_file(cmd, filepath):
    text = f"COMMAND:\n{cmd}"
    Path(filepath).write_text(text, encoding="utf-8")


def _redirect_to_file(cmd, filepath):
    return f"( {cmd} ) >> {filepath} 2>&1"
