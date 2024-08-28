# SPDX-License-Identifier: Apache-2.0

"""Common analysis SBO tasks."""

import abc
import json
import re
import textwrap
from configparser import ConfigParser
from functools import cached_property
from pathlib import Path

from entity_management.analysis import (
    AnalysisReportGeneration,
    AnalysisReportGenerationActivity,
    AnalysisSoftwareSourceCode,
    CumulativeAnalysisReport,
    MultiCumulativeAnalysisReport,
)
from entity_management.base import Derivation
from luigi import (
    BoolParameter,
    Config,
    DictParameter,
    ListParameter,
    OptionalDictParameter,
    OptionalIntParameter,
    OptionalParameter,
    Parameter,
    PathParameter,
    Task,
    WrapperTask,
)
from luigi.parameter import ParameterVisibility

from bbp_workflow.luigi import CompleteTask, RemoteTarget, inherits
from bbp_workflow.sbo.task import BaseTask, MetaTask
from bbp_workflow.settings import DEFAULT_HOST, L
from bbp_workflow.slurm import _run_cmd, _run_sbatch, _sbatch
from bbp_workflow.task import EnvCfg, MakeVirtualEnv, RemoteHostCfg, SlurmCfg
from bbp_workflow.util import _make_zip, _render_template


def get_analysis_config_schema(name):
    """Return the json schema using the given name."""
    tasks_config_schema = {
        "type": "object",
        "properties": {
            "CloneGitRepo": {"type": "object"},
            name: {"type": "object"},
        },
        "required": ["CloneGitRepo", name],
    }
    analysis_configs_schema = {
        "type": "array",
        "items": {"$ref": "#/$defs/TasksConfig"},
        "$defs": {
            "TasksConfig": tasks_config_schema,
        },
    }
    return analysis_configs_schema


class PassTaskCfg(Config):
    """Config mixin that can be used in tasks needing to pass tasks_config to subtasks."""

    tasks_config = OptionalDictParameter(
        description="Configuration dict for required tasks",
        significant=False,
    )  #:


@inherits(RemoteHostCfg)
class CloneGitRepo(Task):
    """Clone a git repository."""

    git_url = Parameter(description="Git repository url to clone.")  #:
    git_ref = Parameter(description="Git reference to checkout (branch, tag, or commit).")  #:
    subdirectory = Parameter(description="Subdirectory of the repository to be checked out.")  #:
    common_dirs = Parameter(
        description="Additional subdirectories to be checked out, relative to 'subdirectory'.",
        default="../common ../data ../../bin",
    )  #:
    repo_path = PathParameter(description="Absolute repo path, recreated if it exists.")  #:
    # use visibility=ParameterVisibility.HIDDEN instead of ParameterVisibility.PRIVATE,
    # or the parameter is ignored when the object is initialized programmatically and serialized
    git_user = OptionalParameter(
        description="Optional git username (read-only).",
        default=None,
        significant=False,
        visibility=ParameterVisibility.HIDDEN,
    )  #:
    git_password = OptionalParameter(
        description="Optional git password (read-only).",
        default=None,
        significant=False,
        visibility=ParameterVisibility.HIDDEN,
    )  #:

    def run(self):
        """Run the Task."""
        cmd = textwrap.dedent(
            """
            set -eu
            module load unstable git
            git version
            if [[ -e "$REPO_PATH" ]]; then
                if [[ -d "$REPO_PATH/.git" ]]; then
                    echo "Removing existing git repository $REPO_PATH..."
                    rm -Rf "$REPO_PATH"
                else
                    echo "The path $REPO_PATH already exists, but it isn't a git repository"
                    exit 1
                fi;
            fi
            export XDG_CONFIG_HOME="${TMPDIR:-/tmp/$USER}/local_config"
            export GIT_CONFIG="$XDG_CONFIG_HOME/git/config"
            echo "GIT_CONFIG=$GIT_CONFIG"
            mkdir -p $(dirname "$GIT_CONFIG")
            trap 'rm -f -- "$GIT_CONFIG"' EXIT
            cat > "$GIT_CONFIG" << END_SCRIPT
            [safe]
                directory = $REPO_PATH
            [credential]
                helper = "!f() { sleep 1; \
                echo \\"username=\\${GIT_USER}\\"; echo \\"password=\\${GIT_PASSWORD}\\"; }; f"
            [url "https://bbpgitlab.epfl.ch/"]
                insteadOf = https://bbpgitlab.epfl.ch/
                insteadOf = ssh://git@bbpgitlab.epfl.ch/
                insteadOf = git@bbpgitlab.epfl.ch:
            [advice]
                detachedHead = false
            END_SCRIPT
            echo "GIT_CONFIG content:"
            cat "$GIT_CONFIG"
            echo "--"
            git clone -v --no-checkout --filter=blob:none "$GIT_URL" "$REPO_PATH"
            GIT_OPTIONS="-C $REPO_PATH"
            COMMON_DIRS=$(printf "$SUBDIRECTORY/%s " $COMMON_DIRS)
            git $GIT_OPTIONS sparse-checkout set $SUBDIRECTORY $COMMON_DIRS
            git $GIT_OPTIONS checkout "$GIT_REF" --
            if [[ ! -d "$REPO_PATH/$SUBDIRECTORY" ]]; then
                echo "ERROR: subdirectory not found in $REPO_PATH: $SUBDIRECTORY"
                exit 1
            fi
            echo "Git checkout completed."
            touch "$SUCCESS_FILE"
            """
        )
        env = {
            "GIT_URL": self.git_url,
            "GIT_REF": self.git_ref,
            "SUBDIRECTORY": self.subdirectory,
            "COMMON_DIRS": self.common_dirs,
            "REPO_PATH": self.repo_path,
            "GIT_USER": self.git_user or "",
            "GIT_PASSWORD": self.git_password or "",
            "SUCCESS_FILE": self.output().path,
        }
        _run_cmd(self.host, cmd, env=env)

    @property
    def code_path(self):
        """Return the absolute path containing source code and requirements."""
        return self.repo_path / self.subdirectory

    def output(self):
        """Return the output that this Task produces."""
        path = self.code_path / ".SUCCESS"
        return RemoteTarget(host=self.host, path=path)


@inherits(PassTaskCfg)
class MakeWorkspace(Task):
    """Make the workspace composed of the cloned git repo and an optional python virtual env."""

    workspace_path = PathParameter(description="Path to the workspace to be created.")  #:

    # pattern used to validate the module names
    _module_pattern = re.compile("^[a-z0-9-]+(/[a-zA-Z0-9._-]+)?$")
    # modules to be loaded, overridable with modules config
    _default_module_archive = "unstable"
    _default_system_modules = "git python"
    _default_modules = ""

    @property
    def virtual_env(self):
        """Return the path of the python virtual environment."""
        return self.workspace_path / "venv" if self.requirements else None

    @property
    def repo_path(self):
        """Return the path of the cloned git repository."""
        return self.workspace_path / "repo"

    @property
    def code_path(self):
        """Return the absolute path containing source code and requirements."""
        return self._clone_repo_task().code_path

    @property
    def python_path(self):
        """Return the python path."""
        return self.code_path.parent

    @property
    def requirements(self):
        """Return the requirements file containing the python packages to install."""
        path = self.code_path / "requirements.txt"
        return path if path.exists() else None

    @property
    def module_archive(self):
        """Return the module archive."""
        return self._modules_config["module_archive"]

    @property
    def modules(self):
        """Return the modules to load, as a space-separated string."""
        return self._modules_config["modules"]

    def _validate_module(self, module):
        """Validate the name of the given module."""
        if not self._module_pattern.match(module):
            raise ValueError(f"Invalid module: {module!r}")
        return module

    def _load_modules_config(self, path):
        """Load and return the modules config from file."""
        if not path.exists():
            return {}
        parser = ConfigParser()
        content = path.read_text(encoding="utf-8")
        if not content.startswith("[DEFAULT]"):
            content = f"[DEFAULT]\n{content}"
        parser.read_string(content)
        return {k.replace("-", "_"): v for k, v in parser["DEFAULT"].items()}

    @cached_property
    def _modules_config(self):
        """Load and return the modules config as a dict."""
        config = {
            "module_archive": self._default_module_archive,
            "system_modules": self._default_system_modules,
            "modules": self._default_modules,
        }
        config.update(self._load_modules_config(self.code_path / "modules.cfg"))
        # merge together system_modules and modules
        modules = config["system_modules"].split() + config["modules"].split()
        return {
            "module_archive": self._validate_module(config["module_archive"]),
            "modules": " ".join(self._validate_module(m) for m in modules),
        }

    def _clone_repo_task(self):
        return CloneGitRepo(
            repo_path=self.repo_path,
            **self.tasks_config["CloneGitRepo"],
        )

    def _make_venv_task(self):
        # build the virtual environment only if requirements.txt exists
        if not self.requirements:
            return CompleteTask()
        return MakeVirtualEnv(
            virtual_env=str(self.virtual_env or ""),
            module_archive=self.module_archive,
            modules=self.modules,
            requirements=str(self.requirements or ""),
            install_luigi=False,
        )

    def run(self):
        """Run the Task."""
        # Dynamic dependency: _make_venv_task depends on the result of _clone_repo_task
        yield self._clone_repo_task()
        yield self._make_venv_task()

    def complete(self):
        """Return True if the Task is complete, False otherwise."""
        return all(task.complete() for task in self.run())


@inherits(SlurmCfg, EnvCfg, PassTaskCfg)
class AnalyseGeneric(BaseTask, metaclass=abc.ABCMeta):
    """Analyse a resource and produce a report."""

    report_name = Parameter(
        default="Custom analysis name",
        description="Name of the report resource in the knowledge graph.",
    )  #:
    report_description = Parameter(
        default="Custom analysis description",
        description="Description of the report resource in the knowledge graph.",
    )  #:
    categories = ListParameter(
        default=[],
        description="Categories to which the report belong.",
    )  #:
    types = ListParameter(
        default=["Analysis"],
        description="Types of reports to which the report belongs.",
    )  #:
    exclusive = BoolParameter(
        default=True,
        significant=False,
        description="Allocate simulation nodes exclusively.",
    )  #:
    mem = Parameter(
        default="0",
        significant=False,
        description="Real memory required per node.",
    )  #:
    command = Parameter(
        description="""
            Command to be used to run the analysis. The following placeholders can be specified,
            and they will be replaced when the analysis is run:

            $CODE_PATH (str): directory containing the user script checked out from git.
            $CONFIG_FILE (str): path to the file containing the analysis configuration.
            $OUTPUT_FILE (str): path to the file that should be written by the user script.

            Example: python '$CODE_PATH/run.py' '$CONFIG_FILE' '$OUTPUT_FILE'
            """,
    )  #:
    analysis_config = DictParameter(description="Analysis configuration dict.")  #:
    workspace_prefix = PathParameter(description="Prefix to the workspace directory.")  #:
    task_index = OptionalIntParameter(description="Index of the task, when executed in a list.")  #:
    source_code_url = OptionalParameter(description="Optional AnalysisSoftwareSourceCode url.")  #:

    @property
    def workspace_path(self):
        """Return the workspace path where repo, venv, output are created."""
        path = self.workspace_prefix / self.config_id
        return path if self.task_index is None else path / str(self.task_index)

    @property
    def analysis_config_path(self):
        """Return the input file to be read by the user script."""
        return self.output_path / "analysis_config.json"

    @property
    def analysis_output_path(self):
        """Return the output file to be written by the user script."""
        return self.output_path / "analysis_output.json"

    @property
    def output_path(self):
        """Return the path to the analysis output."""
        return self.workspace_path / "output"

    @property
    def scratch_path(self):
        """Return the path to the local scratch directory.

        Any output file produced by the user script should be saved there,
        but only the files referenced by the file in analysis_output_path
        will be sent to Nexus.
        """
        return self.workspace_path / "scratch"

    def requires(self):
        """Return the Tasks that this Task depends on."""
        return {
            "workspace": self._workspace_task(),
        }

    def _workspace_task(self):
        """Return the MakeWorkspace task."""
        return MakeWorkspace(
            workspace_path=self.workspace_path,
            tasks_config=self.tasks_config,
        )

    @abc.abstractmethod
    def _get_analysis_config_content(self):
        """Return the rendered content of the analysis config."""

    def _pre_cmd(self):
        workspace = self._workspace_task()
        analysis_config_content = self._get_analysis_config_content()
        return textwrap.dedent(
            f"""
            set -eu
            mkdir -p "{self.output_path}"
            mkdir -p "{self.scratch_path}"
            cat >"{self.analysis_config_path}" <<EOF_CAT
            {analysis_config_content}
            EOF_CAT
            export PYTHONPATH={workspace.python_path}:${{PYTHONPATH:-}}
            {{
            echo "# $(python --version)"
            echo "PYTHON_EXECUTABLE=$(which python)"
            echo "PYTHONPATH=$PYTHONPATH"
            }} >"{self.output_path}/.log_python.txt"
            {{
            echo "# $(git --version)"
            echo "git_url=$(git -C '{workspace.repo_path}' remote get-url origin)"
            echo "git_ref=$(git -C '{workspace.repo_path}' rev-parse HEAD)"
            }} >"{self.output_path}/.log_git.txt"
            module list -t 2>"{self.output_path}/.log_modules.txt"
            if [[ -n "{workspace.virtual_env or ""}" ]]; then
                python -m pip freeze >"{self.output_path}/.log_requirements.txt"
            fi
            """
        )

    def run(self):
        """Run the Task."""
        workspace = self._workspace_task()
        pre_cmd = self._pre_cmd()
        command = _render_template(
            self.command,
            CODE_PATH=workspace.code_path,
            CONFIG_FILE=self.analysis_config_path,
            OUTPUT_FILE=self.analysis_output_path,
        )
        cfgs = [
            (
                self.clone(
                    SlurmCfg,
                    job_name=self.__class__.__name__,
                    job_output=f"{self.output_path}/.%j.log",
                    chdir=str(workspace.repo_path),
                ),
                command,
            )
        ]
        env_cfg = self.clone(
            EnvCfg,
            virtual_env=str(workspace.virtual_env or ""),
            module_archive=workspace.module_archive,
            modules=workspace.modules,
        )
        self.output_path.mkdir(parents=True, exist_ok=True)
        sbatch = _sbatch(cfgs, env_cfg=env_cfg, pre_cmd=pre_cmd)
        with _run_sbatch(DEFAULT_HOST, sbatch, job_script_path=str(self.output_path)):
            pass

    def output(self):
        """Return the output that this Task produces."""
        return RemoteTarget(host=DEFAULT_HOST, path=f"{self.analysis_output_path}")


class MultiAnalyseGeneric(BaseTask, WrapperTask, metaclass=abc.ABCMeta):
    """Run multiple analysis in the same workflow."""

    workspace_prefix = PathParameter(description="Prefix to the workspace directory.")  #:
    analysis_configs = ListParameter(
        description="List of analysis configuration dictionaries.",
    )  #:

    @property
    @abc.abstractmethod
    def _individual_task_class(self):
        """Return the class of the individual task, that must be a subclass of AnalyseGeneric."""

    def _individual_task_parameters(self, task_index, tasks_config):
        """Return the parameters needed to create an instance of the individual task."""
        return {
            "config_id": self.config_id,
            "task_index": task_index,
            "tasks_config": tasks_config,
            "workspace_prefix": self.workspace_prefix,
            **tasks_config[self._individual_task_class.__name__],
        }

    def requires(self):
        """Return the Tasks that this Task depends on."""
        return [
            self._individual_task_class(
                **self._individual_task_parameters(task_index, tasks_config)
            )
            for task_index, tasks_config in enumerate(self.analysis_configs)
        ]


class MultiAnalyseGenericMeta(MetaTask, metaclass=abc.ABCMeta):
    """MetaTask for MultiAnalyseGeneric."""

    @abc.abstractmethod
    def _get_analysed_entity(self):
        """Return the analysed entity, to be used as derivation of CumulativeAnalysisReport."""

    @abc.abstractmethod
    def _publish_part(self, metadata, task, activity):
        """Publish a File and the enclosing entity to Nexus, returning the entity.

        Args:
            metadata: dict containing metadata for the partial analysis report.
            task: instance AnalyseGeneric.
            activity: used to link: AnalysisReport -> wasGeneratedBy -> Activity
        """

    def _publish_parts(self, analysis_output, task, activity):
        """Publish multiple Files and the enclosing entities to Nexus, returning the entities."""
        return [
            self._publish_part(metadata, task, activity) for metadata in analysis_output["outputs"]
        ]

    def _zip_path(self, path):
        """Zip the given path, upload it to Nexus and return a DataDownload resource."""
        with _make_zip(path) as buf:
            return self.from_file(
                buf,
                name=f"{path.name}.zip",
                content_type="application/zip",
            )

    def pre_publish_resource(self, activity):
        """Publish the initial version of the resource before the base task is executed."""
        multitask = self.get_base_task_instance()
        required_tasks = multitask.requires()

        if generated := activity.generated:
            L.warning(
                "Reusing the same resource %s previously generated by the activity %s",
                generated,
                activity,
            )
            if not isinstance(generated, MultiCumulativeAnalysisReport):
                raise RuntimeError(
                    f"The generated resource must be {MultiCumulativeAnalysisReport.__name__}, "
                    f"found {generated.__class__.__name__}"
                )
            if len(generated.hasPart) != len(required_tasks):
                raise RuntimeError(
                    f"The generated resource should have {len(required_tasks)} parts, "
                    f"found {len(generated.hasPart)}"
                )
            return activity, generated
        parts = []
        analysed_entity = self._get_analysed_entity()
        for index, task in enumerate(required_tasks):
            assert isinstance(task, AnalyseGeneric)
            if task.source_code_url:
                agent = AnalysisSoftwareSourceCode.from_url(task.source_code_url)
                generation_activity = AnalysisReportGenerationActivity(wasAssociatedWith=agent)
                generations = [AnalysisReportGeneration(activity=generation_activity)]
            else:
                L.warning(
                    "The source code url for analysis %s is missing, the relation won't be created",
                    index,
                )
                generations = None
            sub_resource = CumulativeAnalysisReport(
                name=task.report_name,
                description=task.report_description,
                categories=list(task.categories),
                types=list(task.types),
                derivation=Derivation(entity=analysed_entity),
                index=index,
                generation=generations,
            )
            sub_resource = self.publish(sub_resource, activity=activity)
            parts.append(sub_resource)

        name = MultiCumulativeAnalysisReport.__name__
        resource = MultiCumulativeAnalysisReport(name=name, hasPart=parts)
        resource = self.publish(resource, activity=activity)
        activity = activity.evolve(generated=resource, used=analysed_entity)
        activity = self.publish(activity)
        return activity, resource

    def publish_resource(self, activity, resource):
        """Publish the resource."""
        assert isinstance(
            resource, MultiCumulativeAnalysisReport
        ), "The expected resource must be created with pre_publish_resource!"

        # read the index of the resources in hasPart, because they might not be sorted
        sub_resources = {int(r.index): r for r in resource.hasPart}
        if len(sub_resources) != len(resource.hasPart):
            raise RuntimeError("Inconsistent sub-resources, some indices might be duplicated")
        multitask = self.get_base_task_instance()
        for index, task in enumerate(multitask.requires()):
            assert isinstance(task, AnalyseGeneric)
            analysis_output_path = Path(task.output().path)
            analysis_output = json.loads(analysis_output_path.read_text(encoding="utf-8"))
            parts = self._publish_parts(analysis_output, task, activity)
            distribution = self._zip_path(task.output_path)
            sub_resource = sub_resources[index].evolve(distribution=distribution, hasPart=parts)
            assert isinstance(sub_resource, CumulativeAnalysisReport)
            self.publish(sub_resource, activity=activity)

        return activity, resource
