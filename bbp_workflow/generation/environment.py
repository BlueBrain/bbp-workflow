# SPDX-License-Identifier: Apache-2.0

"""Environment related utilities."""

from pathlib import Path
from typing import Any, Dict

SPACK_MODULEPATH = "/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta"
MODULES_ENABLE_PATH = "/etc/profile.d/modules.sh"

APPTAINER_MODULEPATH = SPACK_MODULEPATH
APPTAINER_MODULES = ["unstable", "singularityce"]
APPTAINER_EXECUTABLE = "singularity"
APPTAINER_OPTIONS = "--cleanenv --containall --bind $TMPDIR:/tmp,/gpfs/bbp.cscs.ch/project"
APPTAINER_IMAGEPATH = "/gpfs/bbp.cscs.ch/ssd/containers"


def _build_module_cmd(cmd: str, config: Dict[str, Any]) -> str:
    """Wrap the command with modules."""
    modulepath = config.get("modulepath", SPACK_MODULEPATH)
    modules = config["modules"]

    return " && ".join(
        [
            f". {MODULES_ENABLE_PATH}",
            "module purge",
            f"export MODULEPATH={modulepath}",
            f"module load {' '.join(modules)}",
            f"echo MODULEPATH={modulepath}",
            "module list",
            cmd,
        ]
    )


def _build_apptainer_cmd(cmd: str, config: Dict[str, Any]) -> str:
    """Wrap the command with apptainer/singularity."""
    modulepath = config.get("modulepath", APPTAINER_MODULEPATH)
    modules = config.get("modules", APPTAINER_MODULES)
    options = config.get("options", APPTAINER_OPTIONS)
    executable = config.get("executable", APPTAINER_EXECUTABLE)
    image = Path(APPTAINER_IMAGEPATH, config["image"])
    # the current working directory is used also inside the container
    cmd = f'{executable} exec {options} {image} bash <<EOF\ncd "$(pwd)" && {cmd}\nEOF\n'

    cmd = " && ".join(
        [
            f". {MODULES_ENABLE_PATH}",
            "module purge",
            f"module use {modulepath}",
            f"module load {' '.join(modules)}",
            "singularity --version",
            cmd,
        ]
    )
    return cmd


def _build_venv_cmd(cmd: str, config: Dict[str, Any]):
    """Wrap the command with an existing virtual environment."""
    path = config["path"]
    return f". {path}/bin/activate && {cmd}"


ENV_MAPPING: Dict[str, Any] = {
    "MODULE": _build_module_cmd,
    "APPTAINER": _build_apptainer_cmd,
    "VENV": _build_venv_cmd,
}


def build_environment_command(cmd: str, config: dict) -> str:
    """Get shell command combining the chosen environment and the current cmd."""
    build_function = ENV_MAPPING[config["env_type"]]
    return build_function(cmd=cmd, config=config)
