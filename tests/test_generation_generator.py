from pathlib import Path
from unittest.mock import patch

import luigi
import pytest

from bbp_workflow.generation import generator, generator_base
from bbp_workflow.generation.exception import EntityNotFoundError, WorkflowError


def test_register_generator():
    with patch.dict(generator._GENERATORS, clear=True):

        @generator.register_generator
        class A:
            generator_config_name = "a"

        @generator.register_generator
        class B:
            generator_config_name = "b"

        assert generator._GENERATORS == {"a": A, "b": B}


def test_register_generator__missing_config_name():
    with patch.dict(generator._GENERATORS, clear=True):
        with pytest.raises(
            AttributeError, match="type object 'A' has no attribute 'generator_config_name'"
        ):

            @generator.register_generator
            class A:
                """Test."""


def test_register_generator__luigi_parameter_raises():
    with patch.dict(generator._GENERATORS, clear=True):
        with pytest.raises(ValueError, match="'generator_config_name' attribute is not set for A"):

            @generator.register_generator
            class A:
                """Test."""

                generator_config_name = luigi.Parameter()


def test_register_generator__existing_config_name():
    with patch.dict(generator._GENERATORS, clear=True):
        with pytest.raises(ValueError, match="B's config name a is already registered"):

            @generator.register_generator
            class A:
                generator_config_name = "a"

            @generator.register_generator
            class B:
                generator_config_name = "a"


def _generator_args():
    return {
        "main_config_url": "my-config-url",
        "main_output_dir": "my-output-dir",
        "host_config": {"host": "bbpv1.epfl.ch"},
        "account": "proj134",
        "isolated": False,
    }


def _generators():
    return {
        config_name: cls(**_generator_args()) for config_name, cls in generator._GENERATORS.items()
    }


@pytest.fixture
def generator_task():
    return generator_base.GeneratorTask(
        generator_config_name="some-generator",
        generator_name="foo",
        **_generator_args(),
    )


@pytest.mark.parametrize("generator", _generators().values())
def test_generator__output_dir(generator):
    assert generator.output_dir == str(generator.main_output_dir / generator.generator_config_name)


def test_get_class_from_config_name():
    with patch.dict(generator._GENERATORS, {"a": 1, "b": 2}):
        assert generator.get_class_from_config_name("a") == 1
        assert generator.get_class_from_config_name("b") == 2


def test_generator__parameters(generator_task):
    res = generator_task
    assert res.main_config_url == "my-config-url"
    assert res.main_output_dir == Path("my-output-dir")
    assert res.generator_config_name == "some-generator"
    assert res.host_config == {"host": "bbpv1.epfl.ch"}
    assert res.account == "proj134"
    assert res.isolated is False


@pytest.mark.parametrize("cls", generator._GENERATORS.values())
def test_generator_from_instance(generator_task, cls):

    res = cls.from_instance(generator_task)

    assert res.main_config_url == generator_task.main_config_url
    assert res.main_output_dir == generator_task.main_output_dir
    assert res.generator_config_name != generator_task.generator_config_name
    assert res.host_config == generator_task.host_config
    assert res.account == generator_task.account
    assert res.isolated == generator_task.isolated


def test_generator__generator_config__raises():

    side_effect = EntityNotFoundError("Resource not found")

    with patch(
        "bbp_workflow.generation.generator_base.ModelBuildingConfig.from_url",
        side_effect=side_effect,
    ):
        with pytest.raises(WorkflowError):
            generator_base.GeneratorTask(
                generator_config_name="some-generator",
                generator_name="foo",
                **_generator_args(),
            ).generator_config()


def test_MicroConnectomeGenerator__groupby_variant():

    config = {
        "variants": {
            "placeholder__erdos_renyi": {
                "algorithm": "placeholder",
                "version": "v1",
                "params": {
                    "weight": {
                        "type": "float32",
                        "unitCode": "#synapses/connection",
                        "default": 0.0,
                    },
                },
            },
            "placeholder__distance_dependent": {
                "algorithm": "placeholder",
                "version": "v1",
                "params": {
                    "exponent": {"type": "float32", "unitCode": "1/um", "default": 0.008},
                },
            },
            "touchdetector": {
                "algorithm": "touchdetector",
                "version": "v1",
                "params": {
                    "tparam": {"type": "float32", "unitCode": "1/um", "default": 0.008},
                },
            },
        },
        "initial": {
            "variants": {"id": "variants-id"},
            "placeholder__erdos_renyi": {"id": "er-id"},
            "placeholder__distance_dependent": {"id": "dd-id"},
            "touchdetector": {"id": "td-id"},
        },
        "overrides": {
            "variants": {"id": "variants-overrides-id"},
            "placeholder__erdos_renyi": {"id": "er-overrides-id"},
            "placeholder__distance_dependent": {"id": "dd-overrides-id"},
            "touchdetector": {"id": "td-overrides-id"},
        },
    }

    res = generator.MicroConnectomeGenerator._groupby_variant(config)

    assert res.keys() == {("placeholder", "v1"), ("touchdetector", "v1")}

    assert res == {
        ("placeholder", "v1"): {
            "variants": {
                "placeholder__erdos_renyi": {
                    "params": {
                        "weight": {
                            "type": "float32",
                            "unitCode": "#synapses/connection",
                            "default": 0.0,
                        },
                    },
                },
                "placeholder__distance_dependent": {
                    "params": {
                        "exponent": {"type": "float32", "unitCode": "1/um", "default": 0.008}
                    }
                },
            },
            "initial": {
                "variants": {"id": "variants-id"},
                "placeholder__erdos_renyi": {"id": "er-id"},
                "placeholder__distance_dependent": {"id": "dd-id"},
            },
            "overrides": {
                "variants": {"id": "variants-overrides-id"},
                "placeholder__erdos_renyi": {"id": "er-overrides-id"},
                "placeholder__distance_dependent": {"id": "dd-overrides-id"},
            },
        },
        ("touchdetector", "v1"): {
            "variants": {
                "touchdetector": {
                    "params": {"tparam": {"type": "float32", "unitCode": "1/um", "default": 0.008}}
                }
            },
            "initial": {"variants": {"id": "variants-id"}, "touchdetector": {"id": "td-id"}},
            "overrides": {
                "variants": {"id": "variants-overrides-id"},
                "touchdetector": {"id": "td-overrides-id"},
            },
        },
    }
