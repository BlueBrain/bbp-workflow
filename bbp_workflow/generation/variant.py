# SPDX-License-Identifier: Apache-2.0

"""Variant definition module."""

from datetime import datetime
from functools import cached_property
from pathlib import Path

import luigi
from blue_cwl.core import cwl
from blue_cwl.utils import create_dir, load_json
from blue_cwl.variant import Variant
from entity_management.core import Entity
from entity_management.util import get_entity
from luigi.contrib.ssh import RemoteTarget
from luigi.parameter import ParameterVisibility

from bbp_workflow.generation.command import (
    DEFAULT_ENV_VARS,
    MASKED_ENV_VARS,
    build_runtime_env_vars,
)
from bbp_workflow.generation.entity import VariantTaskActivity, VariantTaskConfig
from bbp_workflow.generation.exception import WorkflowError
from bbp_workflow.generation.parameter import CWLModelParameter
from bbp_workflow.generation.target import VariantTarget
from bbp_workflow.settings import L

# from bbp_workflow.generation.sbo.target import VariantTarget
from bbp_workflow.task import KgTask


def build_variant_task(
    config: Entity,
    variant: Entity,
    runtime_config: dict,
    isolated: bool = False,
):
    """Build a luigi task out of a cwl tool definition.

    The inputs of the tool will become luigi parameters and the outputs targets.

    Args:
        config: Config entity with inputs corresponding to the variant definition.
        variant: Variant definition corresponding to a tool or workflow.
        runtime_config: Runtime parameters.

    Returns:
        A VariantTask instance.
    """
    cwl_definition = variant.tool_definition

    config_url = config.get_url()
    variant_url = variant.get_url()

    match cwl_definition:

        case cwl.Workflow():
            return VariantWorkflowTask(
                config_url=config_url,
                variant_url=variant_url,
                runtime_config=runtime_config,
                isolated=isolated,
            )
        case cwl.CommandLineTool():
            return VariantCommandLineToolTask(
                config_url=config_url,
                variant_url=variant_url,
                runtime_config=runtime_config,
                isolated=isolated,
            )
        case _:
            raise TypeError(f"Unknown CWL type {type(cwl_definition)}.")


class _VariantTaskBase(KgTask):
    """Variant base task."""

    variant_url = luigi.Parameter(
        significant=True,
        description="CWL Workflow variant definition id.",
    )
    config_url = luigi.Parameter(
        significant=True, description="CWL Workflow input values resource url."
    )
    runtime_config = luigi.DictParameter(
        significant=True,
        description="""
        Runtime configuration.

        Example:
            {
              'account': 'proj134',
              'output_dir': '/foo/bar/out',
              'kg_config': ...,
            }

        """,
    )
    isolated = luigi.BoolParameter(
        significant=False,
        default=False,
        description="Ignore activities from other executions.",
        visibility=ParameterVisibility.HIDDEN,
    )

    @cached_property
    def _config(self):
        """Return config entity with input values form CWL template concretization."""
        return VariantTaskConfig.from_url(self.config_url)

    @cached_property
    def _input_values(self):
        """Return config input values for CWL template concretization."""
        frozen_values = self._config.distribution.as_dict()["inputs"]
        runtime_values = {"output_dir": self.runtime_config["output_dir"]}
        return {**frozen_values, **runtime_values}

    @cached_property
    def _definition(self):
        """Return variant CWL definition. Either a CommandLineTool or a Workflow."""
        return Variant.from_url(self.variant_url).tool_definition

    def output(self):
        """Return the target with an activity that has a config and an output resource."""
        return VariantTarget(self)

    @property
    def output_resource(self):
        """Return generated resource via KG search."""
        return self.output().activity.generated

    def _get_remote_entity(self):
        """Get generated entity after the end of the task's execution.

        The definition of the variant is expected to have one final output NexusResource which is
        registered to nexus by the tool command. The registered resource, with its id included, is
        written to a jsonld file that luigi uses to fetch the resource.
        """
        output_file = self.output_path

        assert Path(output_file).exists, f"Resource output file does not exist at {output_file}"

        L.debug("Target resource file at %s", output_file)

        # the wrapper outputs a registered entity as a jsonld file
        generated_entity = _get_local_entity(path=output_file)

        return generated_entity

    def run(self):
        """Publish a target."""
        config = self._config
        generated_entity = self._get_remote_entity()

        activity = VariantTaskActivity(
            used_config=config,
            used_rev=config.get_rev(),
            generated=generated_entity,
            status="Done",
        )

        activity = self.publish(activity)

        L.debug(
            (
                "\nVariantTaskActivity registered:\n"
                "\turl        : %s\n"
                "\tused_config: %s\n"
                "\tused_rev   : %s\n"
                "\tgenerated  : %s\n"
            ),
            activity.get_url(),
            config.get_url(),
            config.get_rev(),
            generated_entity.get_url(),
        )

        return activity


def _get_local_entity(path):
    """Load locally stored entity from the workflow wrapper.

    The wrapper registers the output resource to KG and stores the jsonld locally.
    This function gets the id of the registered resource from the file and fetches
    the entity for KG using the id.

    Note: Even though the entity's metadata is stored locally, the entity is regardless fetched from
    KG to ensure there were no issues with registration.
    """
    json_resource = load_json(path)

    # backwards compatibility with forge's json format
    if "id" in json_resource:
        resource_id = json_resource["id"]
    # compatibility with entity-management's jsonld format
    else:
        resource_id = json_resource["@id"]

    return get_entity(resource_id=resource_id, cls=Entity)


class VariantCommandLineToolTask(_VariantTaskBase):
    """Variant CommandLineTool Task."""

    def requires(self):
        """Get CommandLineTool requirement."""
        return CommandLineToolTask(
            definition=self._definition,
            input_values=self._input_values,
            runtime_config=self.runtime_config,
        )

    @property
    def output_path(self):
        """Return output path of generated entity."""
        return self.requires().output_path


class VariantWorkflowTask(_VariantTaskBase):
    """Variant Workflow Task."""

    def requires(self):
        """Return the workflow step requirements."""
        return {
            step.id: WorkflowStepTask(
                name=step.id,
                definition=self._definition,
                input_values=self._input_values,
                runtime_config=self.runtime_config,
            )
            for step in self._definition.steps
        }

    @property
    def output_path(self):
        """Return resource output file."""
        outputs = self._definition.outputs

        if len(outputs) != 1:
            raise WorkflowError(
                f"Workflow should have only one Nexus Resource as output. Found {len(outputs)}."
            )

        step = next(iter(outputs.values()))
        source_task_name, source_output_name = step.split_source_output()

        upstream_tasks = self.requires()

        if source_task_name in upstream_tasks:
            source_task = upstream_tasks[source_task_name]
        else:
            raise WorkflowError(
                f"Workflow could not fetch '{source_task_name}' step task.\n"
                f"Upstream tasks: {sorted(upstream_tasks.keys())}"
            )

        source_outputs = source_task.output()

        if source_output_name in source_outputs:
            source_output = source_task.output()[source_output_name]
        else:
            raise WorkflowError(
                f"Workflow failed to fetch output '{source_output_name}' "
                f"from '{source_task_name}' WorkflowStepTask.\n"
                f"'{source_task_name}' output names: {sorted(source_outputs.keys())}"
            )

        return source_output.path


class CommandLineToolTask(luigi.Task):
    """CommandLineTool Task."""

    definition = CWLModelParameter(
        model=cwl.CommandLineTool,
        significant=True,
        description="CWL Tool definition.",
        visibility=ParameterVisibility.HIDDEN,
    )

    input_values = luigi.DictParameter(
        significant=True,
        description="CWL input values",
    )

    runtime_config = luigi.DictParameter(
        significant=False,
        description="""
            Remote host configuration.
            Example: {'host': 'bbpv1.epfl.ch'}
        """,
        visibility=ParameterVisibility.HIDDEN,
    )

    def __repr__(self):
        """
        Build a task representation like `MyTask(param1=1.5, param2='5')`.

        Note: CWL definitions are truncated to Tool(label: mylabel, ...)
        """
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # Build up task id
        repr_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            if param_objs[param_name].significant:
                if param_name == "definition":
                    repr_parts.append(f'definition={{"label": {param_value.label} ...}}')
                else:
                    repr_parts.append(
                        f"{param_name}={param_objs[param_name].serialize(param_value)}"
                    )

        task_str = f"{self.get_task_family()}({', '.join(repr_parts)})"

        return task_str

    @cached_property
    def output_dir(self):
        """Process output directory."""
        return self.input_values["output_dir"]  # pylint: disable=unsubscriptable-object

    @cached_property
    def process(self):
        """Return CWL CommandLineTool concretized process using the input values."""
        return self.definition.make(input_values=self.input_values)

    def output(self):
        """Return the outputs of the cwl process."""
        # pylint: disable=unsubscriptable-object
        host = self.runtime_config["host_config"]["host"]
        return {
            name: RemoteTarget(host=host, path=out.path)
            for name, out in self.process.outputs.items()
        }

    @property
    def output_path(self):
        """Return output resource path."""
        outputs = self.output()
        assert len(outputs) == 1

        output = list(outputs.values())[0]
        return output.path

    def _generate_log_path(self):
        """Generate a timestamped log path."""
        log_dir = create_dir(Path(self.output_dir, "logs"))
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        return log_dir / f"{timestamp}.log"

    @property
    def runtime_env_vars(self):
        """Get runtime env vars."""
        return build_runtime_env_vars(
            kg_config=self.runtime_config.get("kg_config"),
            global_env_vars=DEFAULT_ENV_VARS,
        )

    @property
    def command(self):
        """Build task command."""
        return self.process.build_command(env_vars=self.runtime_env_vars)

    def run(self):
        """Run step process."""
        self.process.run_command(
            command=self.command,
            redirect_to=self._generate_log_path(),
            masked_vars=MASKED_ENV_VARS,
        )


class WorkflowStepTask(CommandLineToolTask):
    """Workflow Step Task."""

    name = luigi.Parameter(
        significant=True,
        description="CWL Workflow step name.",
    )

    definition = CWLModelParameter(
        model=cwl.Workflow,
        significant=True,
        description="CWL Workflow definition.",
        visibility=ParameterVisibility.HIDDEN,
    )

    def _generate_log_path(self):
        """Generate a timestamped log path."""
        log_dir = create_dir(Path(self.output_dir, "logs"))
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        return log_dir / f"{self.name}__{timestamp}.log"

    def requires(self):
        """Get the requirements of the current task from the cwl definition."""
        return {
            name: WorkflowStepTask(
                name=name,
                input_values=self.input_values,
                definition=self.definition,
                runtime_config=self.runtime_config,
            )
            for name in self.definition.get_step_source_names(self.name)
        }

    @property
    def process(self):
        """Build CWL workflow concretized step process linking upstream outputs to step's inputs."""
        sources = {name: task.process for name, task in self.requires().items()}
        return self.definition.make_workflow_step(
            step_name=self.name,
            input_values=self.input_values,
            sources=sources,
        )
