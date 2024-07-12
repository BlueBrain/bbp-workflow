import json
from pathlib import Path
from unittest.mock import patch

import pytest
from entity_management.config import ModelBuildingConfig

from bbp_workflow.generation import entity as test_module
from bbp_workflow.generation.exception import EntityNotFoundError

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def model_building_config_resp():
    return json.loads(Path(DATA_DIR / "model_building_config_resp.json").read_bytes())


@pytest.fixture
def model_building_config(model_building_config_resp):
    with patch("entity_management.nexus.load_by_url", return_value=model_building_config_resp):
        return ModelBuildingConfig.from_url(None)


@pytest.fixture
def cell_position_config_resp():
    return json.loads(Path(DATA_DIR / "cell_position_config_resp.json").read_bytes())


def test_get_model_building_subconfig(model_building_config, cell_position_config_resp):
    with patch("entity_management.nexus.load_by_id", return_value=cell_position_config_resp):
        res = test_module.get_model_building_subconfig(model_building_config, "cellPositionConfig")
        assert res.generatorName == "cell_position"


def test_get_model_building_subconfig__error(model_building_config):
    with pytest.raises(
        ValueError, match="Config name 'foo' is not an attribute of ModelBuildingConfig."
    ):
        res = test_module.get_model_building_subconfig(model_building_config, "foo")


def test_get_model_building_subconfig__error__not_found(model_building_config):
    with pytest.raises(EntityNotFoundError):
        res = test_module.get_model_building_subconfig(model_building_config, "meModelConfig")
