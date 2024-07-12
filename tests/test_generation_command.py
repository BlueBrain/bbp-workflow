import os
from pathlib import Path
from unittest.mock import patch

import pytest

from bbp_workflow.generation import command as test_module
from bbp_workflow.generation.command import DEFAULT_ENV_VARS


def mockenv(**envvars):
    return patch.dict(os.environ, envvars, clear=True)


def test_mask_token__single():
    string = "TOKENA=aasd.asdf-gdf.gd-f"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENA=$TOKENA"


def test_mask_token__start():
    string = "TOKENA=aasd.asdf-gdf.gd-f TOKENB=sdsade"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENA=$TOKENA TOKENB=sdsade"


def test_mask_token__end():
    string = "TOKENB=sdsade TOKENA=aasd.asdf-gdf.gd-f"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENB=sdsade TOKENA=$TOKENA"


def test_mask_token__inside():
    string = "TOKENB=sdsade TOKENA=aasd.asdf-gdf.gd-f TOKENC=sdsade"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENB=sdsade TOKENA=$TOKENA TOKENC=sdsade"


def test_mask_token__multiple():
    string = "TOKENB=sdsade TOKENA=aasd.asdf-gdf.gd-f TOKENC=sdsade TOKENA=aasd.asdf-gdf.gd-f"
    res = test_module._mask_token(string, "TOKENA")
    assert res == "TOKENB=sdsade TOKENA=$TOKENA TOKENC=sdsade TOKENA=$TOKENA"


def test_mask():
    string = "A=12324 NEXUS_TOKEN=aasd.asdf-gdf.gd-f B=3434 KC_SCR=ssda.3243-dfgr C=43534"
    res = test_module._mask(string)
    assert res == "A=12324 NEXUS_TOKEN=$NEXUS_TOKEN B=3434 KC_SCR=$KC_SCR C=43534"


@mockenv(NEXUS_TOKEN="my-token")
def test_build_command():

    command = {
        "base_command": ["foo", "bar"],
        "named_arguments": {"--a": "a", "--b": "b"},
        "positional_arguments": [(1, "d"), (0, "c")],
    }
    inputs = {"a": 1, "b": 2, "c": 3, "d": 4}
    runtime_arguments = ["--e", 5, "--f", 6]

    kg_config = {
        "kg_base": "my-base",
        "kg_org": "my-org",
        "kg_proj": "my-proj",
    }
    env_config = {
        "env_type": "MODULE",
        "modules": ["archive/2022-01", "bbp-workflow/0.1"],
    }
    slurm_config = {
        "ntasks": 1,
        "wait": False,
    }

    global_env_vars = DEFAULT_ENV_VARS

    result = test_module.build_command(
        command, inputs, kg_config, env_config, slurm_config, runtime_arguments, global_env_vars
    )

    assert result == (
        ". /etc/profile.d/modules.sh && module purge && "
        "export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load archive/2022-01 bbp-workflow/0.1 && "
        "echo MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module list && "
        "export NEXUS_TOKEN=my-token NEXUS_BASE=my-base NEXUS_ORG=my-org NEXUS_PROJ=my-proj "
        "https_proxy=http://bbpproxy.epfl.ch:80/ && "
        "stdbuf -oL -eL salloc "
        "--partition=prod "
        "--ntasks=1 "
        "--constraint=cpu "
        "srun --mpi=none sh -c 'foo bar --a 1 --b 2 3 4 --e 5 --f 6'"
    )


@mockenv(NEXUS_BASE="base", NEXUS_ORG="org", NEXUS_PROJ="proj", NOT_INCLUDED="John")
def test_build_global_env_vars():
    global_env_vars = ["NEXUS_BASE", "NEXUS_ORG", "NEXUS_PROJ"]

    result = test_module._build_global_env_vars(global_env_vars)
    assert result == {
        "NEXUS_BASE": "base",
        "NEXUS_ORG": "org",
        "NEXUS_PROJ": "proj",
    }


def test_build_kg_env_vars():
    kg_config = {"kg_base": "base", "kg_org": "org", "kg_proj": "proj"}
    result = test_module._build_kg_env_vars(kg_config)
    assert result == {
        "NEXUS_BASE": "base",
        "NEXUS_ORG": "org",
        "NEXUS_PROJ": "proj",
    }


def test_build_exports__empty():
    kg_config = slurm_config = global_env_vars = None
    result = test_module._build_exports(kg_config, slurm_config, global_env_vars)
    assert result == "export https_proxy=http://bbpproxy.epfl.ch:80/"


@mockenv(NEXUS_BASE="base", NEXUS_ORG="org", NEXUS_PROJ="proj")
def test_build_exports__kg_configs():

    slurm_config = None
    kg_config = {"kg_base": "my-base", "kg_org": "my-org", "kg_proj": "my-proj"}
    global_vars = {"NEXUS_BASE": "old-base", "NEXUS_ORG": "old-org", "NEXUS_PROJ": "old-proj"}

    result = test_module._build_exports(kg_config, slurm_config, global_vars)
    assert (
        result
        == "export NEXUS_BASE=my-base NEXUS_ORG=my-org NEXUS_PROJ=my-proj https_proxy=http://bbpproxy.epfl.ch:80/"
    )


@mockenv(NEXUS_BASE="base", NEXUS_ORG="org", NEXUS_PROJ="proj")
def test_build_exports__kg_overrides():

    slurm_config = None
    kg_config = {"kg_base": "my-base", "kg_org": "my-org", "kg_proj": "my-proj"}
    global_vars = ["NEXUS_BASE", "NEXUS_ORG", "NEXUS_PROJ"]

    result = test_module._build_exports(kg_config, slurm_config, global_vars)
    assert (
        result
        == "export NEXUS_BASE=my-base NEXUS_ORG=my-org NEXUS_PROJ=my-proj https_proxy=http://bbpproxy.epfl.ch:80/"
    )


def test_build_exports__slurm():

    kg_config = global_vars = None

    slurm_config = {"env_vars": {"var1": "val1", "var2": "val2"}}

    result = test_module._build_exports(kg_config, slurm_config, global_vars)

    assert result == "export https_proxy=http://bbpproxy.epfl.ch:80/ var1=val1 var2=val2"


def test_escape_single_quotes():
    """Test escaping single quotes in salloc command."""
    result = test_module._escape_single_quotes("foo's bar")
    assert result == r"foo'\''s bar"


def test_build_salloc_command():

    config = {
        "ntasks": 1,
        "job_name": "my-job",
        "time": "1:00:00",
        "wait": False,
    }
    cmd = "echo X"

    result = test_module._build_salloc_command(config, cmd)

    assert result == (
        "stdbuf -oL -eL salloc "
        "--partition=prod "
        "--ntasks=1 "
        "--constraint=cpu "
        "--time=1:00:00 "
        "--job-name=my-job "
        "srun --mpi=none sh -c 'echo X'"
    )


def test_build_core_command():

    command = ["foo", "bar"]
    inputs = {"a": 1, "b": 2, "c": 3, "d": 4}
    named_arguments = {"--a": "a", "--b": "b"}
    positional_arguments = [(1, "d"), (0, "c")]
    runtime_arguments = ["--e", 5, "--f", 6]

    result = test_module._build_core_command(
        command, inputs, named_arguments, positional_arguments, runtime_arguments
    )

    assert result == "foo bar --a 1 --b 2 3 4 --e 5 --f 6"


def test_run_command():

    with patch("bbp_workflow.generation.command._run_remote_salloc_command", return_value="salloc"):
        result = test_module.run_command("", host_config={"host": "bbpv1.epfl.ch"})
    assert result == "salloc"
