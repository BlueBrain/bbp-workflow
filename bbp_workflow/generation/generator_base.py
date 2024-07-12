# SPDX-License-Identifier: Apache-2.0

"""Generator task base module."""

import json
from pathlib import Path
from typing import Optional

import luigi
import luigi.util
from blue_cwl.utils import create_dir
from blue_cwl.variant import Variant
from entity_management.config import ModelBuildingConfig
from entity_management.workflow import GeneratorTaskActivity
from luigi.parameter import ParameterVisibility
from luigi.task import flatten

from bbp_workflow.generation.entity import (
    VariantTaskConfig,
    VariantTaskParameterization,
    get_model_building_subconfig,
)
from bbp_workflow.generation.exception import EntityNotFoundError, WorkflowError
from bbp_workflow.generation.forge_ensure_once import create_or_find
from bbp_workflow.generation.target import GeneratorTarget
from bbp_workflow.generation.variant import build_variant_task
from bbp_workflow.settings import L
from bbp_workflow.task import KgCfg, KgTask


class GeneratorTask(KgTask):
    """Generator Task class."""

    main_config_url = luigi.Parameter()
    main_output_dir = luigi.PathParameter()

    generator_name = luigi.Parameter(visibility=ParameterVisibility.PRIVATE)
    generator_config_name = luigi.Parameter(visibility=ParameterVisibility.PRIVATE)

    host_config = luigi.DictParameter(
        significant=False,
        description="""
            Remote host configuration.
            Example: {'host': 'bbpv1.epfl.ch'}
        """,
        visibility=ParameterVisibility.HIDDEN,
    )

    account = luigi.Parameter(
        significant=False,
        description="Slurm account.",
        visibility=ParameterVisibility.HIDDEN,
    )

    isolated = luigi.BoolParameter(
        significant=False,
        default=False,
        description="Ignore activities from other executions.",
        visibility=ParameterVisibility.HIDDEN,
    )

    @classmethod
    def from_instance(cls, instance: "GeneratorTask") -> "GeneratorTask":
        """Use instance's parameters to create a new instance of this class.

        Note: All generator tasks have the same parameters.
        """
        return cls(
            main_config_url=instance.main_config_url,
            main_output_dir=instance.main_output_dir,
            host_config=instance.host_config,
            account=instance.account,
            isolated=instance.isolated,
        )

    @property
    def main_config(self):
        """Return the main configuration entity."""
        cfg = ModelBuildingConfig.from_url(self.main_config_url)
        if cfg is None:
            raise WorkflowError(f"Main config was not found: {self.main_config_url}")
        return cfg

    @property
    def generator_config(self):
        """Return the generator's configuration entity."""
        try:
            return get_model_building_subconfig(
                model_building_config=self.main_config,
                name=self.generator_config_name,
            )
        except EntityNotFoundError as e:
            raise WorkflowError(f"Generator {type(self).__name__} failed to fetch config.") from e

    @property
    def config_url(self):
        """Return the generator's config url."""
        return self.generator_config.get_url()

    @property
    def output_dir(self):
        """Output directory for generator task."""
        return str(Path(self.main_output_dir, self.generator_config_name))

    @property
    def kg_config(self) -> dict:
        """Return the KG config as a dictionary."""
        return self.clone(KgCfg).param_kwargs

    @property
    def output_resource(self):
        """Return output resource generated by this task."""
        return self.output().activity.generated

    def output(self):
        """Return the target with an activity that has a config and an output resource."""
        return GeneratorTarget(self)

    def _variant_upstream_inputs(self):
        """Get mapping from upstream outputs to variant task inputs."""
        requirements = self.requires()

        if not requirements:
            return {}

        return {
            input_name: generator_instance.output_resource.get_id()
            for input_name, generator_instance in requirements.items()
        }

    def _variant_inputs(self, variant_id, data: dict, extra_inputs: Optional[dict] = None) -> dict:
        """Build variant's inputs.

        Args:
            generator_name: The name of the generator.
            algorithm_name: The name of the algorithm.
            version: The version of the generator/algorithm variant.
            data: The configuration data to extract the inputs from.
            extra_inputs: Non-default inputs that are specific to the generator.

        Returns:
            A dictionary with all the inputs of the variant.

        Note: The variant definition id is added by default to all variants.
        """
        inputs = {"variant": variant_id}

        # keep only the name and id of the inputs in the data
        if "inputs" in data:
            inputs.update({inp["name"]: inp["id"] for inp in data["inputs"]})

        # upstream inputs allow to override what inputs are needed from the parent task if any
        inputs.update(**self._variant_upstream_inputs())

        if extra_inputs:
            inputs.update(extra_inputs)

        if "configuration" in data:

            # create or fetch a resource the hash of which is determined
            # by the configuration entries
            configuration = create_or_find(
                entity_cls=VariantTaskParameterization,
                properties={},
                serialized_content=json.dumps(data["configuration"]),
                filename="variant_task_parameterization.json",
                content_type="application/json",
            )
            inputs["configuration"] = configuration.get_id()

        return inputs

    def build_variant_task(self, *, algorithm, version, data, output_dir, extra_inputs=None):
        """Construct and return a variant task."""
        variant = Variant.from_search(
            generator_name=self.generator_name,
            variant_name=algorithm,
            version=version,
        )

        # create variant definition input values
        input_values = self._variant_inputs(
            variant_id=variant.get_id(),
            data=data,
            extra_inputs=extra_inputs,
        )

        # create or find config that parametrizes the variant definition
        config = create_or_find(
            entity_cls=VariantTaskConfig,
            properties={},
            serialized_content=json.dumps({"inputs": input_values}),
            filename="variant_task_configuration.json",
            content_type="application/json",
        )

        # one subfolder per region
        runtime_config = {
            "output_dir": str(output_dir),
            "account": self.account,
            "kg_config": self.kg_config,
            "host_config": self.host_config,
            "isolated": self.isolated,
        }

        return build_variant_task(
            config=config,
            variant=variant,
            runtime_config=runtime_config,
            isolated=self.isolated,
        )

    def publish_target(self, generated_entity):
        """Publish a target."""
        config = self.generator_config

        activity = GeneratorTaskActivity(
            used_config=config,
            used_rev=config.get_rev(),
            generated=generated_entity,
            status="Done",
        )

        activity = self.publish(activity)

        L.debug(
            (
                "\nGeneratorTaskActivity registered:\n"
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


class RelayGenerator(GeneratorTask):
    """A relay generator that takes the config and propagates it downstream."""

    def run(self):
        """Generate a new config copy and return it."""
        create_dir(Path(self.output_dir))

        config = self.generator_config

        # Publish a copy of the config
        generated_entity = config.evolve()

        # pylint: disable=protected-access
        generated_entity._force_attr("_id", None)

        self.publish_target(generated_entity=self.publish(generated_entity))


class MultiVariantGenerator(GeneratorTask):
    """Multiple variants."""

    def scatter(self):
        """Dispatch variant tasks."""
        raise NotImplementedError

    def merge(self, tasks):
        """Combine by returning the resource of the first task."""
        return flatten(tasks)[0].output_resource

    def run(self):
        create_dir(Path(self.output_dir))
        tasks = self.scatter()
        yield tasks
        L.debug("Variant tasks completed for generator with id: %s", self.task_id)
        self.publish_target(self.merge(tasks))