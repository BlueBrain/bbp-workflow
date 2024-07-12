"""Tests"""

import textwrap
import zipfile

import pytest
from luigi_tools.util import set_luigi_config

import bbp_workflow.util
from bbp_workflow.settings import MODULE_ARCHIVE
from bbp_workflow.task import EnvCfg, SlurmCfg
from bbp_workflow.util import (
    _fix_bbp_link,
    _get_content_type,
    _make_zip,
    _render_template,
    amend_cmd,
    cmd_join,
    env_exports,
    job_log_url,
    make_web_link,
    metadata_path,
    to_env_commands,
    to_sbatch_params,
    to_srun_params,
)


def test_sbatch_params():
    """test slurm params"""
    conf = SlurmCfg(
        account="abc", partition="partition", exclusive=True, wait=False, job_name="name"
    )
    assert (
        to_sbatch_params(conf) == "#SBATCH --account=abc\n"
        "#SBATCH --partition=partition\n"
        "#SBATCH --constraint=cpu\n"
        "#SBATCH --exclusive\n"
        "#SBATCH --job-name=name"
    )


def test_srun_params():
    """test slurm params"""
    conf = SlurmCfg(account="abc", partition="partition", exclusive=True, job_name="name")
    assert (
        to_srun_params(conf)
        == "--account=abc --partition=partition --constraint=cpu --exclusive --job-name=name"
    )


def test_env_exports():
    """test env exports"""
    env = ""
    assert env_exports(env) == []

    env = "A=B,C=D"
    assert env_exports(env) == ["export A=B", "export C=D"]

    env = "   A=B   ,  C=D"
    assert env_exports(env) == ["export A=B", "export C=D"]

    env = " \n   A=B   \n  C=D  "
    assert env_exports(env) == ["export A=B", "export C=D"]


def test_to_env_commands_default_moudel_archive():
    """Test environment parameters."""
    env = EnvCfg(
        module_path="/module/path", modules="module1 module2", enable_internet=True, env="A=B,C=D"
    )
    assert [
        "export MODULEPATH=/module/path:$MODULEPATH",
        f"module load {MODULE_ARCHIVE}",
        "module load module1 module2",
        "export https_proxy=http://bbpproxy.epfl.ch:80/",
        "export A=B",
        "export C=D",
    ] == to_env_commands(env)


def test_to_env_commands_no_module_archive():
    """Test environment parameters."""
    env = EnvCfg(
        module_path="/module/path",
        module_archive="",
        modules="module1 module2",
        enable_internet=True,
        env="A=B,C=D",
    )
    assert [
        "export MODULEPATH=/module/path:$MODULEPATH",
        "module load unstable",
        "module load module1 module2",
        "export https_proxy=http://bbpproxy.epfl.ch:80/",
        "export A=B",
        "export C=D",
    ] == to_env_commands(env)


def test_to_env_commands_explicit_module_archive():
    """Test environment parameters."""
    env = EnvCfg(
        module_path="/module/path",
        module_archive="archive/dummy",
        modules="module1 module2",
        enable_internet=True,
        env="A=B,C=D",
    )
    assert [
        "export MODULEPATH=/module/path:$MODULEPATH",
        "module load archive/dummy",
        "module load module1 module2",
        "export https_proxy=http://bbpproxy.epfl.ch:80/",
        "export A=B",
        "export C=D",
    ] == to_env_commands(env)


def test_to_env_commands_no_modules():
    """Test default None for modules ignores module-path and module-archive."""
    env = EnvCfg(
        module_path="/module/path",
        module_archive="archive/dummy",
        virtual_env="/virtual/env",
        enable_internet=True,
        env="A=B,C=D",
    )
    assert [
        "export https_proxy=http://bbpproxy.epfl.ch:80/",
        "source /virtual/env/bin/activate",
        "export A=B",
        "export C=D",
    ] == to_env_commands(env)


def test_to_env_commands_with_cfg():
    """Test with cfg file."""
    with set_luigi_config(configfile="tmp_luigi.cfg"):
        env_cfg = EnvCfg()
        assert not to_env_commands(env_cfg)
    with set_luigi_config({"EnvCfg": {"modules": ""}}, configfile="tmp_luigi.cfg"):
        env_cfg = EnvCfg()
        assert not to_env_commands(env_cfg)
    with set_luigi_config({"EnvCfg": {"modules": "module1 module2"}}, configfile="tmp_luigi.cfg"):
        env_cfg = EnvCfg()
        assert [f"module load {MODULE_ARCHIVE}", "module load module1 module2"] == to_env_commands(
            env_cfg
        )
    with set_luigi_config(
        {"EnvCfg": {"module_archive": "archive/dummy", "modules": "module1 module2"}},
        configfile="tmp_luigi.cfg",
    ):
        env_cfg = EnvCfg()
        assert ["module load archive/dummy", "module load module1 module2"] == to_env_commands(
            env_cfg
        )


def test_cmd_join():
    """test cmd join"""
    cmd = ""
    assert cmd_join(cmd) == ""

    cmd = "--arg1=A --arg2=B"
    assert cmd_join(cmd) == "--arg1=A --arg2=B"

    cmd = "   --arg1=A   \t  --arg2=B   "
    assert cmd_join(cmd) == "--arg1=A --arg2=B"

    cmd = " \n   --arg1=A   \n  \t --arg2=B  \n \n \n  "
    assert cmd_join(cmd) == "--arg1=A --arg2=B"


def test_make_web_link():
    """Test making web link from nexus resource_id."""
    assert (
        make_web_link(None, "nse", "test", "d539f4c8-fc00-4d95-83dc-fb1e62ef36db")
        == "https://bbp.epfl.ch/nexus/web/nse/test/resources/d539f4c8-fc00-4d95-83dc-fb1e62ef36db"
    )


def test_fix_bbp_link():
    """Test making web link from nexus resource_id."""
    assert _fix_bbp_link("https://1.1.1.1:8888/?t=1") == "https://1.1.1.1:8888/?t=1"
    assert _fix_bbp_link("https://node1:8888/?t=1") == "https://node1.bbp.epfl.ch:8888/?t=1"
    assert _fix_bbp_link("https://node1?t=1") == "https://node1.bbp.epfl.ch?t=1"


def test_match_attach_to():
    """Test ssh prompt."""
    in_str = """
    1) [909516] prod   (null)       0:04    1:00:00      59:56
    2) [909514] prod   (null)       7:46    1:00:00      52:14
    3) [909511] prod   (null)      15:19    1:00:00      44:41
    Select job to attach to: Attaching to job 909516
    Time left: 59:56
    echo HI
    Hostname: r1i4n4
    User: foo
    [foo@r1i4n4]~% echo HI"""
    in_str = textwrap.dedent(in_str)
    # FIXME


def test_ood_link():
    """Test ood link."""
    assert (
        job_log_url(None, "/gpfs/bbp.cscs.ch/ssd/home/user/folder", 123)
        == "https://ood.bbp.epfl.ch/pun/sys/files/api/v1/fs"
        "/gpfs/bbp.cscs.ch/ssd/home/user/folder/slurm-123.out"
    )
    assert (
        job_log_url(None, "/gpfs/bbp.cscs.ch/home/user/folder", 123)
        == "https://ood.bbp.epfl.ch/pun/sys/files/api/v1/fs"
        "/gpfs/bbp.cscs.ch/ssd/home/user/folder/slurm-123.out"
    )
    assert (
        job_log_url(None, "/gpfs/bbp.cscs.ch/project/proj123/folder", 123)
        == "https://ood.bbp.epfl.ch/pun/sys/files/api/v1/fs"
        "/gpfs/bbp.cscs.ch/data/project/proj123/folder/slurm-123.out"
    )
    assert (
        job_log_url(None, "/gpfs/bbp.cscs.ch/project/proj123/scratch/folder", 123)
        == "https://ood.bbp.epfl.ch/pun/sys/files/api/v1/fs"
        "/gpfs/bbp.cscs.ch/data/scratch/proj123/folder/slurm-123.out"
    )


def test_metadata_path():
    """Test metadata path."""
    assert metadata_path(
        "/gpfs/bbp.cscs.ch/ssd/home/user/folder", "/gpfs/bbp.cscs.ch/ssd/home/user/folder"
    ) == ("/gpfs/bbp.cscs.ch/ssd/home/user", "folder")
    assert metadata_path(
        "/gpfs/bbp.cscs.ch/ssd/home/user/folder", "/gpfs/bbp.cscs.ch/ssd/home/user"
    ) == ("/gpfs/bbp.cscs.ch/ssd/home/user", "folder")
    assert metadata_path(
        "/gpfs/bbp.cscs.ch/ssd/home/user/folder/_SUCCESS", "/gpfs/bbp.cscs.ch/ssd/home/user"
    ) == ("/gpfs/bbp.cscs.ch/ssd/home/user", "folder--_SUCCESS")
    assert metadata_path(
        "/gpfs/bbp.cscs.ch/ssd/home/user/a/b/c/_SUCCESS", "/gpfs/bbp.cscs.ch/ssd/home/user"
    ) == ("/gpfs/bbp.cscs.ch/ssd/home/user", "a--b--c--_SUCCESS")


def test_make_zip(tmp_path):
    subdir = tmp_path / "subdir"
    subdir.mkdir()
    testfile = subdir / "testfile.txt"
    testfile.write_text("My content", encoding="utf-8")
    with _make_zip(tmp_path) as buf:
        with zipfile.ZipFile(buf, mode="r") as z:
            assert z.testzip() is None
            assert z.namelist() == ["subdir/testfile.txt"]


@pytest.mark.parametrize(
    "path, expected",
    [
        ("path/to/file.txt", "text/plain"),
        ("path/to/file.png", "image/png"),
        ("path/to/file.bin", "application/octet-stream"),
    ],
)
def test_get_content_type(path, expected):
    result = _get_content_type(path)
    assert result == expected


@pytest.mark.parametrize(
    "template, kwargs, expected",
    [
        ("Hello world", {}, "Hello world"),
        ("Hello $key", {"key": "world"}, "Hello world"),
        ("Hello $$key", {"key": "world"}, "Hello $world"),
        ("Hello \\$key", {"key": "world"}, "Hello $key"),
        ("Hello \\$$key", {"key": "world"}, "Hello $world"),
        ("Hello $key", {"key": "world", "other_ignored_key": "ignored"}, "Hello world"),
    ],
)
def test_render_template(template, kwargs, expected):
    result = _render_template(template, **kwargs)
    assert result == expected


def test_render_template_raises():
    template = "Hello $missing_key"
    with pytest.raises(KeyError, match="missing_key"):
        _render_template(template)


@pytest.fixture
def hpc_environment(request, monkeypatch):
    """Set ENVIRONMENT variable, this fixture must be used with `parametrize`"""
    monkeypatch.setattr(bbp_workflow.util, "ENVIRONMENT", request.param)
    yield request.param


@pytest.mark.parametrize(
    "hpc_environment, container, container_cmd, default_cmd, expected",
    [
        # no container -> use container cmd as default
        (None, "my.sif", "echo A", None, "echo A"),
        # no container and default cmd -> use default
        (None, "my.sif", "echo A", "echo B", "echo B"),
        # bbp container
        (
            "bbp",
            "my.sif",
            "echo A",
            "echo B",
            "singularity run --no-eval "
            "-B /gpfs/bbp.cscs.ch/project:/gpfs/bbp.cscs.ch/project:ro "
            "-B $(pwd) --pwd $(pwd) "
            "/gpfs/bbp.cscs.ch/ssd/containers/hpc/spackah/my.sif "
            "eval 'echo A'",
        ),
        # aws container
        (
            "aws",
            "my.sif",
            "echo A",
            "echo B",
            "singularity run --no-eval "
            "-B /gpfs/bbp.cscs.ch/project:/gpfs/bbp.cscs.ch/project:ro "
            "-B $(pwd) --pwd $(pwd) "
            "/gpfs/bbp.cscs.ch/ssd/containers/hpc/spackah/my.sif "
            "eval 'echo A'",
        ),
    ],
    indirect=["hpc_environment"],
)
def test_amend_cmd(hpc_environment, container, container_cmd, default_cmd, expected):
    assert amend_cmd(container, container_cmd, default_cmd) == expected
