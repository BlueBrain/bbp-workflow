from unittest.mock import Mock, patch

import pytest
from blue_cwl.core import environment as constants

from bbp_workflow.generation import environment as test_module


@pytest.fixture
def config_module():
    return {"modulepath": "/modulepath", "modules": ["archive/2022-03", "circuit-build/2.0.0"]}


MOCK_ALLOCATION = Mock(shell_command=lambda cmd: f"salloc -J X srun sh -c '{cmd}'")


MODULES_PREAMBLE = (
    f". {constants.MODULES_ENABLE_PATH} && "
    "module purge && "
    "export MODULEPATH=/modulepath && "
    "module load archive/2022-03 circuit-build/2.0.0 && "
    "echo MODULEPATH=/modulepath && "
    "module list"
)


def test_build_module_cmd(config_module):

    cmd = "echo X"
    res = test_module._build_module_cmd(cmd=cmd, config=config_module)
    assert res == f"{MODULES_PREAMBLE} && echo X"


def test_build_apptainer_cmd():

    cmd = "echo X"
    config = {"image": "path"}
    res = test_module._build_apptainer_cmd(cmd=cmd, config=config)

    assert res == (
        f". {constants.MODULES_ENABLE_PATH} && "
        "module purge && "
        f"module use {constants.APPTAINER_MODULEPATH} && "
        "module load unstable singularityce && "
        "singularity --version && "
        f"singularity exec {constants.APPTAINER_OPTIONS} {constants.APPTAINER_IMAGEPATH}/path "
        'bash <<EOF\ncd "$(pwd)" && echo X\nEOF\n'
    )


def test_build_environment_command__venv():
    config = {
        "env_type": "VENV",
        "path": "venv-path",
    }
    res = test_module.build_environment_command(cmd="echo X", config=config)
    assert res == ". venv-path/bin/activate && echo X"


def test_build_environment_command__apptainer():
    config = {
        "env_type": "APPTAINER",
        "image": "singularity-image-path",
    }
    res = test_module.build_environment_command(cmd="echo X", config=config)
    assert "singularity-image-path" in res


def test_build_environment_command__module():
    config = {
        "env_type": "MODULE",
        "modules": ["unstable", "foo"],
    }
    res = test_module.build_environment_command(cmd="echo X", config=config)
    assert "unstable foo" in res
