# SPDX-License-Identifier: Apache-2.0

"""Module for specifying generators."""

from pathlib import Path

import luigi
from blue_cwl.nexus import get_region_acronym
from blue_cwl.utils import create_dir

from bbp_workflow.generation.generator_base import MultiVariantGenerator, RelayGenerator

_GENERATORS = {}


def register_generator(cls: "GeneratorTask") -> "GeneratorTask":
    """Register generator classes to fetch by configuration name."""
    config_name = cls.generator_config_name

    if isinstance(config_name, luigi.Parameter):
        raise ValueError(f"'generator_config_name' attribute is not set for {cls.__name__}")

    if config_name in _GENERATORS:
        raise ValueError(f"{cls.__name__}'s config name {config_name} is already registered.")

    _GENERATORS[config_name] = cls
    return cls


def get_class_from_config_name(generator_config_name: str) -> "GeneratorTask":
    """Return the generator class corresponding to the input config name."""
    return _GENERATORS[generator_config_name]


@register_generator
class CellCompositionGenerator(MultiVariantGenerator):
    """CellCompositionGenerator class."""

    generator_name = "cell_composition"
    generator_config_name = "cellCompositionConfig"

    def scatter(self):
        """Launch one variant per region in the configuration."""
        config_data = self.generator_config.distribution.as_dict()

        tasks = {}
        for region_id, data in config_data.items():
            task = self.build_variant_task(
                algorithm=data["variantDefinition"]["algorithm"],
                version=data["variantDefinition"]["version"],
                data=data,
                output_dir=create_dir(
                    Path(
                        self.output_dir,
                        get_region_acronym(region_id),
                    )
                ),
                extra_inputs={"region_id": region_id},
            )
            tasks[region_id] = task

        return tasks


@register_generator
class CellPositionGenerator(MultiVariantGenerator):
    """CellPositionGenerator class."""

    generator_name = "cell_position"
    generator_config_name = "cellPositionConfig"

    def requires(self):
        """Return required tasks."""
        return {"cell_composition": CellCompositionGenerator.from_instance(self)}

    def scatter(self):
        """Launch one variant per region in the configuration."""
        config_data = self.generator_config.distribution.as_dict()

        tasks = {}
        for region_id, data in config_data.items():
            task = self.build_variant_task(
                algorithm=data["variantDefinition"]["algorithm"],
                version=data["variantDefinition"]["version"],
                data=data,
                output_dir=create_dir(
                    Path(
                        self.output_dir,
                        get_region_acronym(region_id),
                    )
                ),
                extra_inputs={"region_id": region_id},
            )
            tasks[region_id] = task

        return tasks


@register_generator
class MorphologyAssignmentGenerator(MultiVariantGenerator):
    """MorphologyAssignmentGenerator class."""

    generator_name = "mmodel"
    generator_config_name = "morphologyAssignmentConfig"

    def requires(self):
        """Return required tasks."""
        return {"circuit": CellPositionGenerator.from_instance(self)}

    def scatter(self):
        """Launch one variant for mmodel."""
        config_data = self.generator_config.distribution.as_dict()

        # hardcoded for now
        algorithm = "neurons_mmodel"
        version = config_data["variantDefinition"]["topological_synthesis"]["version"]
        assert version == config_data["variantDefinition"]["placeholder_assignment"]["version"]

        task = self.build_variant_task(
            algorithm=algorithm,
            version=version,
            data={"configuration": config_data},
            output_dir=create_dir(Path(self.output_dir)),
        )
        return [task]


@register_generator
class MEModelGenerator(MultiVariantGenerator):
    """MEModelGenerator class."""

    generator_name = "memodel"
    generator_config_name = "meModelConfig"

    def requires(self):
        """Return required tasks."""
        return {"circuit": MorphologyAssignmentGenerator.from_instance(self)}

    def scatter(self):
        """Launch one variant for me-model."""
        config_data = self.generator_config.distribution.as_dict()

        # hardcoded for now
        algorithm = "neurons_memodel"
        version = config_data["variantDefinition"]["neurons_me_model"]["version"]

        task = self.build_variant_task(
            algorithm=algorithm,
            version=version,
            data={"configuration": config_data},
            output_dir=create_dir(Path(self.output_dir)),
        )
        return [task]


@register_generator
class MacroConnectomeGenerator(RelayGenerator):
    """MacroConnectomeGenetor relays the config generated by the UI downstream."""

    generator_name = "macro"
    generator_config_name = "macroConnectomeConfig"


@register_generator
class MicroConnectomeGenerator(MultiVariantGenerator):
    """Connectome generator task."""

    generator_name = "connectome"
    generator_config_name = "microConnectomeConfig"

    def requires(self):
        """Return required tasks."""
        return {
            "circuit": MEModelGenerator.from_instance(self),
            "macro_connectome_config": MacroConnectomeGenerator.from_instance(self),
        }

    @staticmethod
    def _groupby_variant(data):
        """Return hierarchy data per variant.

        For variants that share the same algorithm their configs are grouped together in order to be
        sent to the same tool. This is the case with the Erdos Renyi and Distance
        Dependent algorithms for the placeholder connectome generation.
        """

        def template():
            return {
                "variants": {},
                "initial": {},
                "overrides": {},
            }

        configs = {}
        for variant_name, variant_data in data["variants"].items():

            key = (variant_data["algorithm"], variant_data["version"])

            # group together the variants that use the same tool (e.g. placeholder)
            cfg = configs.get(key, template())

            cfg["variants"][variant_name] = {"params": variant_data["params"]}
            cfg["initial"][variant_name] = data["initial"][variant_name]
            cfg["overrides"][variant_name] = data["overrides"].get(variant_name, {})

            # copy over variants

            cfg["initial"]["variants"] = data["initial"]["variants"]
            cfg["overrides"]["variants"] = data["overrides"].get("variants", {})

            if key not in configs:
                configs[key] = cfg

        return configs

    def scatter(self):
        """Launch one variant per algorithm."""
        variant_groups = self._groupby_variant(self.generator_config.distribution.as_dict())

        tasks = []
        for (algorithm, version), data in variant_groups.items():

            task = self.build_variant_task(
                algorithm=algorithm,
                version=version,
                data={"configuration": data},
                output_dir=create_dir(Path(self.output_dir, f"{algorithm}__{version}")),
            )
            tasks.append(task)

        return tasks


@register_generator
class ConnectomeFilteringGenerator(MultiVariantGenerator):
    """Connectome filtering task."""

    generator_name = "connectome_filtering"
    generator_config_name = "synapseConfig"

    def requires(self):
        """Return required tasks."""
        return {"circuit": MicroConnectomeGenerator.from_instance(self)}

    def scatter(self):
        """Launch one variant for functionalizer."""
        config_data = self.generator_config.distribution.as_dict()

        if "variantDefinition" in config_data:
            algorithm = config_data["variantDefinition"]["algorithm"]
            version = config_data["variantDefinition"]["version"]
        else:
            # for backwards compatibility with older configs
            algorithm = "synapses"
            version = "v1"

        task = self.build_variant_task(
            algorithm=algorithm,
            version=version,
            data={"configuration": config_data},
            output_dir=create_dir(Path(self.output_dir)),
        )

        return [task]
