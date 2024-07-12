"""Slurm tests."""

from textwrap import dedent

from bbp_workflow.slurm import _sbatch, _srun
from bbp_workflow.task import EnvCfg, SlurmCfg, _remote_script


class T:
    """."""

    def remote_script(self):  # pylint: disable=no-self-use
        """."""
        print("foo")


def test_srun():
    """Test heterogeneous srun job."""
    output = """\
    set -e
    module load archive/2021-01
    module load module1
    export REMOTE_SCRIPT="$(mktemp -p $HOME)"
    trap 'rm -f -- "$REMOTE_SCRIPT"' INT TERM HUP EXIT
    cat << 'EOF_CODE' > "$REMOTE_SCRIPT"
    print("foo")

    EOF_CODE
    stdbuf -oL -eL srun --account=acc1 --partition=prod --constraint=cpu cmd "$REMOTE_SCRIPT" arg"""
    assert dedent(output) == _srun(
        SlurmCfg(account="acc1"),
        EnvCfg(module_archive="archive/2021-01", modules="module1"),
        'cmd "$REMOTE_SCRIPT" arg',
        pre_cmd=_remote_script(T()),
    )


def test_sbatch_hetjob():
    """Test heterogeneous sbatch job."""

    output = """\
    set -e
    stdbuf -oL -eL sbatch << 'EOF_SBATCH'
    #!/bin/bash -l
    #SBATCH --account=acc1
    #SBATCH --partition=prod
    #SBATCH --constraint=cpu
    #SBATCH --wait
    #SBATCH hetjob
    #SBATCH --account=acc2
    #SBATCH --partition=prod
    #SBATCH --constraint=cpu
    #SBATCH --wait
    #SBATCH hetjob
    #SBATCH --account=acc3
    #SBATCH --partition=prod
    #SBATCH --constraint=cpu
    #SBATCH --wait
    export REMOTE_SCRIPT="$(mktemp -p $HOME)"
    trap 'rm -f -- "$REMOTE_SCRIPT"' INT TERM HUP EXIT
    cat << 'EOF_CODE' > "$REMOTE_SCRIPT"
    print("foo")

    EOF_CODE
    srun --het-group=0 cmd1 : --het-group=1 cmd2 arg1 : --het-group=2 cmd3 "$REMOTE_SCRIPT" arg1
    EOF_SBATCH"""
    cfgs = [
        (SlurmCfg(account="acc1"), "cmd1"),
        (SlurmCfg(account="acc2"), "cmd2 arg1"),
        (SlurmCfg(account="acc3"), 'cmd3 "$REMOTE_SCRIPT" arg1'),
    ]
    assert dedent(output) == _sbatch(cfgs, EnvCfg(), pre_cmd=_remote_script(T()))
