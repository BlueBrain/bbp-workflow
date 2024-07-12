# SPDX-License-Identifier: Apache-2.0

"""Collection of base tasks for the Workflow engine."""

import inspect
import os
import re
import warnings
from pathlib import Path
from textwrap import dedent

from entity_management.core import DataDownload
from luigi import (
    BoolParameter,
    ChoiceParameter,
    Config,
    ExternalTask,
    IntParameter,
    OptionalParameter,
    Parameter,
    Task,
)
from luigi.parameter import UnconsumedParameterWarning
from luigi.target import FileSystemTarget
from luigi.task import flatten

from bbp_workflow.luigi import RemoteTarget, RunAnywayTarget, inherits
from bbp_workflow.settings import DEBUG, DEFAULT_HOST, MODULE_ARCHIVE, WORKFLOW_MODULE
from bbp_workflow.slurm import _run_cmd, _run_sbatch, _run_srun, _sbatch, _srun
from bbp_workflow.util import job_log_url, make_web_link, metadata_path

warnings.filterwarnings("ignore", category=UnconsumedParameterWarning)


def _get_source(remote_script):
    lines = inspect.getsource(remote_script)
    # remove 'def abc(): # efg\n', optional docstring and indentation leaving only function body
    return dedent(
        re.sub(r'[^:]*:[^\n]*\n(\s*[\'"]{3}[\s\S]*?[\'"]{3}\s*?\n?)?', "", lines, count=1)
    )


def _remote_script(obj):
    if obj is not None and hasattr(obj, "remote_script"):
        remote_script = _get_source(obj.remote_script)
        return "\n".join(
            [
                'export REMOTE_SCRIPT="$(mktemp -p $HOME)"',
                "trap 'rm -f -- \"$REMOTE_SCRIPT\"' INT TERM HUP EXIT",
                "cat << 'EOF_CODE' > \"$REMOTE_SCRIPT\"",
                remote_script,
                "EOF_CODE",
            ]
        )
    else:
        return ""


def _is_matching(regexp, name):
    """Check if task name is matching the regexp(which can be empty string)."""
    if len(regexp) > 0 and not re.match(regexp, name):
        return False
    else:
        return True


class RemoteHostCfg(Config):
    """Add ``host`` parameter to your task using this mixin."""

    host = Parameter(
        default=DEFAULT_HOST,
        significant=False,
        description="Remote host which is accessed by the task.",
    )  #:


@inherits(RemoteHostCfg)
class MakeRemoteFolder(Task):
    """Creates a folder on the host file system."""

    path = Parameter(description="Folder path.")  #:

    def run(self):
        """"""
        self.output()._fs.mkdir(self.path)  # pylint: disable=protected-access

    def output(self):
        """:class:`RemoteTarget<luigi.contrib.ssh.RemoteTarget>` (``path=self.path``)."""
        return RemoteTarget(host=self.host, path=self.path)


class OutputCfg(Config):
    """Common configuration for tasks producing output at the specified file system location."""

    path_prefix = Parameter(
        config_path={"section": "DEFAULT", "name": "path-prefix"},
        description="Default value can be provided in the `DEFAULT` cfg file section.",
    )  #:


class SlurmCfg(Config):
    """Standard set of Slurm configuration parameters.

    Slurm based tasks will extend this class, so they can be parametrized the same way.
    """

    # significant
    chdir = OptionalParameter(
        default=None,
        description="Have the remote processes do a chdir to path before beginning execution.",
    )  #:

    # insignificant
    account = Parameter(
        default="",
        significant=False,
        config_path={"section": "DEFAULT", "name": "account"},
        description="Default value can be provided in the  `DEFAULT` section of the accompanying "
        "cfg file.",
    )  #:
    partition = Parameter(
        default="prod",
        significant=False,
        config_path={"section": "DEFAULT", "name": "partition"},
        description="Default value can be provided in the  `DEFAULT` section of the accompanying "
        "cfg file.",
    )  #:
    nodes = IntParameter(
        default=0, significant=False, description="Number of nodes to be allocated."
    )  #:
    qos = ChoiceParameter(
        default="",
        significant=False,
        choices=["", "normal", "longjob", "bigjob", "jenkins"],
        var_type=str,
        description="Quality of service.",
    )  #:
    ntasks = IntParameter(
        default=0, significant=False, description="Number of tasks to launch."
    )  #:
    ntasks_per_node = IntParameter(
        default=0,
        significant=False,
        description="Number of tasks to launch on each allocated node.",
    )  #:
    cpus_per_task = IntParameter(
        default=0, significant=False, description="Amount of cpus allocated per task."
    )  #:
    mpi = OptionalParameter(
        default=None, significant=False, description="Identify the type of MPI to be used."
    )  #:
    mem = OptionalParameter(
        default=None, significant=False, description="Specify the real memory required per node."
    )  #:
    mem_per_cpu = OptionalParameter(
        default=None, significant=False, description="Minimum memory required per allocated CPU."
    )  #:
    constraint = OptionalParameter(
        default="cpu",
        significant=False,
        description="Specify which type of nodes to use in allocation.",
    )  #:
    exclusive = BoolParameter(
        default=False,
        significant=False,
        description="Allocate nodes exclusively and do not share them with other jobs.",
    )  #:
    time = OptionalParameter(
        default=None,
        significant=False,
        description="Set a limit on the total run time of the job allocation.",
    )  #:
    dependency = OptionalParameter(
        default=None,
        significant=False,
        description="Defer the start of this job until the specified dependencies have been "
        "satisfied completed.",
    )
    job_name = OptionalParameter(
        default=None, significant=False, description="Specify a name for the job."
    )  #:
    job_output = OptionalParameter(default=None, significant=False, description="Job output.")  #:
    job_array = OptionalParameter(
        default=None, significant=False, description="Specify job array params."
    )
    wait = BoolParameter(
        default=True, significant=False, description="Use only to wait for sbatch to fininsh."
    )


class EnvCfg(Config):
    """Standard set of environment configuration parameters."""

    virtual_env = OptionalParameter(
        default="",
        config_path={"section": "DEFAULT", "name": "virtual-env"},
        significant=False,
        description="Full path to python virtual environment containing `bin/activate`. "
        "Default value should be placed in the `[DEFAULT]` section "
        "of the config file.",
    )  #:
    module_path = OptionalParameter(
        default="",
        significant=False,
        description="Custom MODULEPATH to be exported before module load.",
    )  #:
    module_archive = Parameter(
        default=MODULE_ARCHIVE,
        significant=False,
        config_path={"section": "DEFAULT", "name": "module-archive"},
        description="Module archive to use. Default value can be placed in the `[DEFAULT]` "
        "section of the config file or, as usual, on the command line.",
    )  #:
    modules = OptionalParameter(
        default=None, significant=False, description="Environment modules to load."
    )  #:
    enable_internet = BoolParameter(
        default=False,
        significant=False,
        description="If True, enables Internet access by exporting https_proxy.",
    )  #:
    env = OptionalParameter(
        default="",
        significant=False,
        description="Comma separated or multi-line extra environment variables to export.",
    )  #:


class KgCfg(Config):
    """Knowledge Graph configuration parameters."""

    # Worker env will have those vars set, don't use config_path here
    kg_base = OptionalParameter(
        default=os.environ.get("NEXUS_BASE"),
        significant=False,
        description="Full url to KG instance. By default production will be used.",
    )  #:
    kg_org = OptionalParameter(
        default=os.environ.get("NEXUS_ORG"),
        significant=False,
        description="KG organization to use. By default `bbp` organisation will be used.",
    )  #:
    kg_proj = OptionalParameter(
        default=os.environ.get("NEXUS_PROJ"), significant=False, description="KG project to use."
    )  #:


class LookupKgEntity(KgCfg, ExternalTask):
    """Base class for Knowledge Graph lookup tasks."""

    url = Parameter(description="Existing knowledge graph entity URL.")  #:

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        splited_url = self.url.split("/")
        proj = splited_url[-3]
        org = splited_url[-4]
        assert splited_url[-5] == "resources"
        assert splited_url[-6] == "v1"
        base = "/".join(splited_url[:-5])
        if self.kg_base is None:
            self.kg_base = base
        if self.kg_org is None:
            self.kg_org = org
        if self.kg_proj is None:
            self.kg_proj = proj


class KgTask(KgCfg, Task):
    """Base class for Knowledge Graph enabled tasks."""

    name = OptionalParameter(
        default=None,
        significant=False,
        description="Name of the knowledge graph entity that will be created by the task.",
    )  #:
    informed_by = OptionalParameter(default=None, significant=False)

    def publish(self, entity, **kwargs):
        """Delegate to entity ``publish`` method using kg params from the task."""
        return entity.publish(base=self.kg_base, org=self.kg_org, proj=self.kg_proj, **kwargs)

    def from_id(self, entity_cls, resource_id, on_no_result=None):
        """Delegate to entity ``from_id`` method using kg params from the task."""
        return entity_cls.from_id(
            resource_id,
            on_no_result=on_no_result,
            base=self.kg_base,
            org=self.kg_org,
            proj=self.kg_proj,
        )

    def from_file(self, *args, **kwargs):
        """Delegate to DataDownload ``from_file`` method using kg params from the task."""
        return DataDownload.from_file(
            *args, base=self.kg_base, org=self.kg_org, proj=self.kg_proj, **kwargs
        )

    def from_json_str(self, *args, **kwargs):
        """Delegate to DataDownload ``from_json_str`` method using kg params from the task."""
        return DataDownload.from_json_str(
            *args, base=self.kg_base, org=self.kg_org, proj=self.kg_proj, **kwargs
        )

    def make_web_link(self, resource_id):
        """Make link to nexus web from the ``resource_id``."""
        return make_web_link(self.kg_base, self.kg_org, self.kg_proj, resource_id)

    def done(self, msg, entity):
        """Mark output as done and print entity URL."""
        if self.set_tracking_url:
            # pylint: disable=not-callable
            self.set_tracking_url(self.make_web_link(resource_id=entity.get_id()))
        url = entity.get_url()
        print(f'\n{msg} {url}\nfor use in cfg: {url.replace("%", "%%")}\n', flush=True)
        if isinstance(self.output(), RunAnywayTarget):
            self.output().done(entity.get_url())
        else:
            first_target = next(iter(flatten(self.output())), None)
            if first_target and isinstance(first_target, FileSystemTarget):
                chdir = getattr(self, "chdir", None)
                path, name = metadata_path(first_target.path, chdir)
                kg_url_path = f"{path}/.{name}.kg_url"
                # pylint: disable=unspecified-encoding
                Path(kg_url_path).write_text(entity.get_url())

    def output(self):
        """:class:`RunAnywayTarget<bbp_workflow.luigi.RunAnywayTarget>`."""
        return RunAnywayTarget(self)


@inherits(EnvCfg, RemoteHostCfg)
class MakeVirtualEnv(Task):
    """Creates a python virtual environment on the host file system.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow bbp_workflow.task MakeVirtualEnv \\
            virtual-env=/gpfs/bbp.cscs.ch/home/${USER}/tmp/venv \\
            modules=py-bbp-workflow \\
            packages='bmtk'
    """

    virtual_env = Parameter(
        config_path={"section": "DEFAULT", "name": "virtual-env"},
        description="Full path to python virtual environment where `bin/activate` "
        "will be created.",
    )  #:
    module_archive = Parameter(
        default="unstable",
        config_path={"section": "DEFAULT", "name": "module-archive"},
        description="Module archive to use.",
    )  #:
    modules = Parameter(default="python", description="Environment modules to load.")  #:
    packages = Parameter(default="", description="Python packages to install.")  #:
    requirements = OptionalParameter(
        default=None, description="Requirements file containing the python packages to install."
    )  #:
    install_luigi = BoolParameter(
        default=True, description="Install luigi in the python virtual environment."
    )  #:

    def run(self):
        """"""
        cmds = [
            "export PIP_INDEX_URL=https://bbpteam.epfl.ch/repository/devpi/simple",
            f"module load {self.module_archive} {self.modules}",
            f"python -m venv {self.virtual_env}",
            f"source {self.virtual_env}/bin/activate",
            "pip install --upgrade pip setuptools wheel",
            "pip install --ignore-installed --no-deps luigi" if self.install_luigi else "",
            f"pip install {self.packages}" if self.packages else "",
            f"pip install -r {self.requirements}" if self.requirements else "",
        ]
        cmd = " && ".join(c for c in cmds if c)
        _run_cmd(self.host, cmd)

    def output(self):
        """:class:`RemoteTarget<luigi.contrib.ssh.RemoteTarget>`\\(``self.host``,\
        ``self.virtual_env``\\)."""
        return RemoteTarget(host=self.host, path=f"{self.virtual_env}/bin/activate")


def _get_job_name(instance):
    if instance.job_name:
        return instance.job_name
    else:
        return instance.get_task_family()


@inherits(SlurmCfg, EnvCfg, RemoteHostCfg)
class SrunTask(Task):
    """Srun task.

    Inherits :class:`SlurmCfg<bbp_workflow.task.SlurmCfg>`,
    :class:`EnvCfg<bbp_workflow.task.EnvCfg>`,
    :class:`RemoteHostCfg<bbp_workflow.task.RemoteHostCfg>`
    parameters, enabling customization of this task.
    """

    command = Parameter(description="The command `srun` will schedule for execution.")  #:
    args = Parameter(
        default="",
        description="Arguments that will be passed to the command. You can also make your custom "
        "`@property` named args, to dynamically assemble args.",
    )  #:
    return_result = BoolParameter(
        default=False, description="If True, will capture the output of the task."
    )  #:

    def run(self):
        """Will run ``self.command`` on the ``self.host`` using SLURM allocation.

        If the task which extends ``SrunTask`` implements ``self.remote_script``, the source of the
        ``self.remote_script`` method will be stored in a tmp file. This tmp file will be submitted
        as the first argument to the ``self.command`` and ``self.args`` will follow.
        """
        remote_script = _remote_script(self)
        if remote_script:
            cmd = f'{self.command} "$REMOTE_SCRIPT" {self.args}'
        else:
            cmd = f"{self.command} {self.args}"
        srun_cmd = _srun(
            self.clone(SlurmCfg, job_name=_get_job_name(self)),
            self.clone(EnvCfg),
            cmd,
            pre_cmd=remote_script,
        )
        return _run_srun(self.host, srun_cmd, return_result=self.return_result)


class _JobPathMixin:
    """Get job path from task output."""

    def get_job_path(self):
        """In case task has file as an output, use it to make job path."""
        first_target = next(iter(flatten(self.output())), None)
        if first_target and isinstance(first_target, FileSystemTarget):
            path, _ = metadata_path(first_target.path, self.chdir)
            return path
        return None

    def get_job_output(self):
        """Return path to log output file located at job_path."""
        job_path = self.get_job_path()
        if job_path:
            return f"{job_path}/.%j.log"
        return None


@inherits(SlurmCfg, EnvCfg, RemoteHostCfg)
class SbatchTask(_JobPathMixin, Task):
    """Sbatch task.

    Inherits :class:`SlurmCfg<bbp_workflow.task.SlurmCfg>`,
    :class:`EnvCfg<bbp_workflow.task.EnvCfg>`,
    :class:`RemoteHostCfg<bbp_workflow.task.RemoteHostCfg>`
    parameters, enabling customization of this task.
    """

    command = Parameter(description="The command `sbatch` will schedule for execution.")  #:
    args = Parameter(
        default="",
        description="Arguments that will be passed to the command. You can also make your custom "
        "`@property` named args, to dynamically assemble args.",
    )  #:

    def run(self):
        """"""
        remote_script = _remote_script(self)
        if remote_script:
            cmd = f'{self.command} "$REMOTE_SCRIPT" {self.args}'
        else:
            cmd = f"{self.command} {self.args}"
        cfgs = [
            (
                self.clone(
                    SlurmCfg, job_name=_get_job_name(self), job_output=self.get_job_output()
                ),
                cmd,
            )
        ]
        sbatch = _sbatch(cfgs, self, pre_cmd=remote_script)
        job_path = self.get_job_path()
        with _run_sbatch(self.host, sbatch, job_script_path=job_path) as (job_id, _):
            if job_path:
                job_path = f"{job_path}/.{job_id}.log"
            # pylint: disable=not-callable
            self.set_tracking_url(job_log_url(job_path, self.chdir, job_id))


@inherits(SlurmCfg, EnvCfg, RemoteHostCfg)
class IPyParallel(_JobPathMixin, Task):
    """Allocate ipyparallel cluster and run ``remote_script``.

    Inherits :class:`SlurmCfg<bbp_workflow.task.SlurmCfg>`,
    :class:`EnvCfg<bbp_workflow.task.EnvCfg>`
    parameters, enabling customization of this task.
    """

    modules = Parameter(default=WORKFLOW_MODULE, description="Environment modules to load.")  #:

    args = Parameter(
        default="",
        description="Arguments that will be passed to the command. You can also make your custom "
        "`@property` named args, to dynamically assemble args.",
    )  #:
    ntasks = IntParameter(default=0, description="Number of ipyparallel engines to launch.")  #:

    def run(self):
        """"""
        task_id = self.task_id
        job_name = _get_job_name(self)
        debug = "--log-level=DEBUG" if DEBUG else ""

        cfgs = [
            (
                self.clone(
                    SlurmCfg, job_output=self.get_job_output(), job_name=f"{job_name}-ipengine"
                ),
                f"bash -c 'sleep 60 && ipengine {debug} --profile={task_id}'",
            ),
            (
                SlurmCfg(
                    account=self.account,
                    partition=self.partition,
                    ntasks=1,
                    job_name=f"{job_name}-ipcontroller",
                ),
                f"bash -c 'ipcontroller --init {debug} --ip=* --location=$SLURMD_NODENAME "
                f"--profile={task_id}'",
            ),
            (
                SlurmCfg(
                    account=self.account,
                    partition=self.partition,
                    ntasks=1,
                    job_name=f"{job_name}-script",
                ),
                # mind the quotes!
                "bash -c '"  # set trap, sleep, run script
                f"trap '\\''ipython --profile={task_id} --nosep --no-color-info --no-pdb "
                '-c "from ipyparallel import Client;Client().shutdown(hub=True, block=True)"'
                "'\\'' INT TERM HUP EXIT && "  # trap that shutsdown cluster
                "sleep 180 && "
                f"ipython {debug} --profile={task_id} --nosep --no-color-info --no-pdb "
                f'"$REMOTE_SCRIPT" {self.args}'
                "'",
            ),
        ]
        sbatch = _sbatch(cfgs, self, pre_cmd=_remote_script(self))
        with _run_sbatch(self.host, sbatch, job_script_path=self.get_job_path()):
            pass

    def remote_script(self):
        """Will run in the context of ipyparallel cluster.

        Use :class:`Client<ipyparallel.Client>` to talk to the cluster:

        .. code-block:: python

            def remote_script(self):
                from ipyparallel import Client
                client = Client()
                lview = client.load_balanced_view()
                lview.block = True
                def f(_):
                    return 'foo'
                result = lview.map(f, range(2))
                print(result)

        """


class IPyParallelExclusive(IPyParallel):
    """:class:`IPyParallel` which uses exclusive nodes with engine(task) per cpus."""

    exclusive = BoolParameter(
        default=True,
        significant=False,
        description="Allocate nodes exclusively and do not share them with other jobs.",
    )  #:
    mem = Parameter(
        default="0", significant=False, description="Real memory required per node."
    )  #:
    cpus_per_task = IntParameter(
        default=1,
        significant=False,
        description="Amount of cpus allocated per engine. Increase to 2 or 3 if your engines "
        "consume too much memory but you still want to use exclusive nodes.",
    )  #:
