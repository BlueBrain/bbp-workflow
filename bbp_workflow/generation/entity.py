# SPDX-License-Identifier: Apache-2.0

"""Entity definitions module."""

from entity_management.simulation import AttrOf, DataDownload, attributes
from entity_management.workflow import BbpWorkflowActivity, BbpWorkflowConfig

from bbp_workflow.generation.exception import EntityNotFoundError


def get_model_building_subconfig(model_building_config, name):
    """Get model building config."""
    try:
        config = getattr(model_building_config.configs, name)
    except AttributeError as e:
        raise ValueError(
            f"Config name '{name}' is not an attribute of ModelBuildingConfig "
            f"{model_building_config.get_url()}"
        ) from e
    if config is None:
        raise EntityNotFoundError(
            f"Sub-config '{name}' was not found.\n"
            f"ModelBuildingConfig: {model_building_config.get_url()}"
        )
    return config


@attributes(
    {
        "generatorName": AttrOf(str, default=None),
        "distribution": AttrOf(DataDownload),
    }
)
class GeneratorTaskConfig(BbpWorkflowConfig):
    """Generator Configuration."""


@attributes(
    {
        "distribution": AttrOf(DataDownload),
        "generatorName": AttrOf(str, default=None),
    }
)
class VariantTaskConfig(BbpWorkflowConfig):
    """VariantConfig class."""


@attributes(
    {
        "distribution": AttrOf(DataDownload),
    }
)
class VariantTaskParameterization(BbpWorkflowConfig):
    """VariantTaskParameterization class."""


class VariantTaskActivity(BbpWorkflowActivity):
    """Activity of a Variant task."""
