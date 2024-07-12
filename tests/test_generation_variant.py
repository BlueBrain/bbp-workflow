import functools
import json
import os
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from blue_cwl.core import cwl, process
from blue_cwl.utils import load_json
from blue_cwl.variant import Variant
from entity_management import nexus
from entity_management.core import Entity
from luigi.contrib.ssh import RemoteTarget

from bbp_workflow.generation import variant as test_module
from bbp_workflow.generation.entity import VariantTaskConfig
from bbp_workflow.generation.exception import WorkflowError
from bbp_workflow.generation.target import VariantTarget

DATA_DIR = Path(__file__).parent / "data"

CONFIG_TOOL_ID = "https://config-tool-id"
CONFIG_TOOL_URL = "https://config-tool-url"
CONFIG_TOOL_CONTENTURL = "https://config-tool-distribution-url"

CONFIG_WORKFLOW_ID = "https://config-workflow-id"
CONFIG_WORKFLOW_URL = "https://config-workflow-url"
CONFIG_WORKFLOW_CONTENTURL = "https://config-workflow-distribution-url"

VARIANT_TOOL_ID = "https://variant-tool-id"
VARIANT_TOOL_URL = "https://variant-tool-url"
VARIANT_TOOL_CONTENTURL = "https://variant-tool-distribution-url"

VARIANT_WORKFLOW_ID = "https://variant-workflow-id"
VARIANT_WORKFLOW_URL = "https://variant-workflow-url"
VARIANT_WORKFLOW_CONTENTURL = "https://variant-workflow-distribution-url"

CIRCUIT_ID = "htpps://circuit-id"
CIRCUIT_URL = "htpps://circuit-url"

MOCK_ENV_VARS = {
    "NEXUS_BASE": "my-base",
    "NEXUS_ORG": "my-org",
    "NEXUS_TOKEN": "my-token",
    "NEXUS_WORKFLOW": "my-workflow",
}


def patchenv(**envvars):
    """Patch function environment."""
    return patch.dict(os.environ, envvars, clear=True)


def _resource(properties, distr_dict=None):

    if distr_dict:
        distribution = {
            "@type": "DataDownload",
            "contentSize": {"unitCode": "bytes", "value": 928},
            "contentUrl": distr_dict["url"],
            "digest": {
                "algorithm": "SHA-256",
                "value": "ab7c288f30487c111b9435f1affefa4bcca5beb795f691b60eb9bc6c0f8e19d2",
            },
            "encodingFormat": distr_dict["format"],
            "name": distr_dict["name"],
        }
    else:
        distribution = None

    default = {
        "@context": [
            "https://bluebrain.github.io/nexus/contexts/metadata.json",
            "https://bbp.neuroshapes.org",
        ],
        "_constrainedBy": "https://bluebrain.github.io/nexus/schemas/unconstrained.json",
        "_createdAt": "2024-02-19T14:45:13.685305Z",
        "_createdBy": "https://bbp.epfl.ch/nexus/v1/realms/bbp/users/zisis",
        "_deprecated": False,
        "_incoming": "https://foo-id",
        "_outgoing": "https://bar-id",
        "_project": "https://bbp.epfl.ch/nexus/v1/projects/bbp/mmb-point-neuron-framework-model",
        "_rev": 1,
        "_schemaProject": "https://bbp.epfl.ch/nexus/v1/projects/bbp/mmb-point-neuron-framework-model",
        "_updatedAt": "2024-02-19T14:45:13.685305Z",
        "_updatedBy": "https://bbp.epfl.ch/nexus/v1/realms/bbp/users/zisis",
    }

    if distribution:
        default["distribution"] = distribution

    return default | properties


CIRCUIT_METADATA = _resource(
    {
        "@id": CIRCUIT_ID,
        "_self": CIRCUIT_URL,
        "@type": "DetailedCircuit",
        "circuitConfigPath": {"@type": "DataDownload", "url": "file:///circuit_config.json"},
    }
)


class MockTool:

    variant_metadata = _resource(
        properties={
            "@id": VARIANT_TOOL_ID,
            "_self": VARIANT_TOOL_URL,
            "@type": "Variant",
            "name": "foo|bar|v1",
            "generator_name": "foo",
            "variant_name": "bar",
            "version": "v1",
        },
        distr_dict={
            "url": VARIANT_TOOL_CONTENTURL,
            "name": "definition.cwl",
            "format": "application/cwl",
        },
    )

    variant_distribution_file = str(DATA_DIR / "variant_tool_distribution.cwl")

    input_values = {
        "variant": VARIANT_TOOL_ID,
        "circuit": CIRCUIT_ID,
        "configuration": CONFIG_TOOL_ID,
    }

    config_metadata = _resource(
        properties={
            "@id": CONFIG_TOOL_ID,
            "_self": CONFIG_TOOL_URL,
            "@type": "VariantTaskConfig",
            "name": "foo",
        },
        distr_dict={
            "url": CONFIG_TOOL_CONTENTURL,
            "format": "application/json",
            "name": "variant_task_configuration.json",
        },
    )

    config_distribution = {
        "inputs": {
            "variant": VARIANT_TOOL_ID,
            "circuit": CIRCUIT_ID,
            "configuration": CONFIG_TOOL_ID,
        }
    }


class MockWorkflow:

    variant_metadata = _resource(
        properties={
            "@id": VARIANT_WORKFLOW_ID,
            "_self": VARIANT_WORKFLOW_URL,
            "@type": "Variant",
            "name": "foo|bar|v1",
            "generator_name": "foo",
            "variant_name": "bar",
            "version": "v1",
        },
        distr_dict={
            "url": VARIANT_WORKFLOW_CONTENTURL,
            "name": "definition.cwl",
            "format": "application/cwl",
        },
    )

    variant_distribution_file = str(DATA_DIR / "variant_workflow_distribution.cwl")

    input_values = {
        "variant": VARIANT_WORKFLOW_ID,
        "circuit": CIRCUIT_ID,
        "configuration": CONFIG_WORKFLOW_ID,
    }

    config_metadata = _resource(
        properties={
            "@id": CONFIG_WORKFLOW_ID,
            "_self": CONFIG_WORKFLOW_URL,
            "@type": "VariantTaskConfig",
            "name": "foo",
        },
        distr_dict={
            "url": CONFIG_WORKFLOW_CONTENTURL,
            "format": "application/json",
            "name": "variant_task_configuration.json",
        },
    )

    config_distribution = {
        "inputs": {
            "variant": VARIANT_WORKFLOW_ID,
            "circuit": CIRCUIT_ID,
            "configuration": CONFIG_WORKFLOW_ID,
        }
    }


import shutil


def patch_nexus(function):
    """Patch nexus requests to return local ones."""

    def load_by_id(resource_id, *args, **kwargs):
        return {CIRCUIT_ID: CIRCUIT_METADATA}[resource_id]

    def load_by_url(url, *args, **kwargs):
        return {
            CONFIG_TOOL_URL: MockTool.config_metadata,
            CONFIG_WORKFLOW_URL: MockWorkflow.config_metadata,
            VARIANT_TOOL_URL: MockTool.variant_metadata,
            VARIANT_WORKFLOW_URL: MockWorkflow.variant_metadata,
        }[url]

    def file_as_dict(url, *args, **kwargs):
        return {
            CONFIG_TOOL_CONTENTURL: MockTool.config_distribution,
            CONFIG_WORKFLOW_CONTENTURL: MockWorkflow.config_distribution,
        }[url]

    def download_file(url, path, *args, **kwargs):

        source = {
            VARIANT_TOOL_CONTENTURL: MockTool.variant_distribution_file,
            VARIANT_WORKFLOW_CONTENTURL: MockWorkflow.variant_distribution_file,
        }[url]

        target = Path(path, Path(source).name)

        shutil.copyfile(source, target)

        return str(target)

    @functools.wraps(function)
    def wrapper(*args, **kwargs):

        with (
            patch("entity_management.nexus.load_by_id", side_effect=load_by_id),
            patch("entity_management.nexus.load_by_url", side_effect=load_by_url),
            patch("entity_management.nexus.file_as_dict", side_effect=file_as_dict),
            patch("entity_management.nexus.download_file", side_effect=download_file),
        ):
            function(*args, **kwargs)

    return wrapper


@pytest.fixture
def cmdtool_variant():
    """Return CommandLineTool cwl definition filepath."""

    variant = Variant.from_file(
        DATA_DIR / "variant_tool_distribution.cwl",
        generator_name="connectome_filtering",
        variant_name="synapses",
        version="v1",
    )

    variant._force_attr("_self", "https://variant-url")

    return variant


@pytest.fixture
def workflow_variant():
    """Return Workflow cwl definition filepath."""
    return Variant.from_file(
        DATA_DIR / "variant_workflow_distribution.cwl",
        generator_name="connectome_filtering",
        variant_name="synapses",
        version="v1",
    )


def test_build_variant_task(cmdtool_variant, workflow_variant):
    """Test that the builder dispatches the correct tasks depending on the definition type."""

    runtime_config = {}

    config = Mock(get_url=Mock(return_value="https://config-url"))

    res = test_module.build_variant_task(
        config=config, variant=cmdtool_variant, runtime_config=runtime_config
    )
    assert isinstance(res, test_module.VariantCommandLineToolTask)

    res = test_module.build_variant_task(
        config=config, variant=workflow_variant, runtime_config=runtime_config
    )
    assert isinstance(res, test_module.VariantWorkflowTask)

    mock_variant = Mock()
    mock_variant.tool_definition = {"foo": "bar"}
    mock_variant.get_url.return_vale = "https://config-url"

    with pytest.raises(TypeError, match="Unknown CWL type"):
        test_module.build_variant_task(
            config=config, variant=mock_variant, runtime_config=runtime_config
        )


@patch_nexus
@patch.dict(
    os.environ,
    MOCK_ENV_VARS,
    clear=True,
)
def test_variant_command_line_tool_task(tmpdir):

    runtime_config = {
        "output_dir": str(tmpdir),
        "host_config": {"host": "my-host"},
    }

    task = test_module.VariantCommandLineToolTask(
        variant_url=VARIANT_TOOL_URL,
        config_url=CONFIG_TOOL_URL,
        runtime_config=runtime_config,
    )

    # check variant configuration & input values
    config = task._config
    assert config.get_url() == CONFIG_TOOL_URL

    # Variant level tasks should have variant targets that query for the activity existence
    assert isinstance(task.output(), VariantTarget)

    # expected input values are the input values to the task plus the output directory from the
    # runtime config that is added to the variant input values
    expected_input_values = MockTool.input_values | {"output_dir": runtime_config["output_dir"]}
    assert task._input_values == expected_input_values

    # check cwl variant definition
    definition = task._definition
    assert isinstance(definition, cwl.CommandLineTool)

    # check output path associated to this task.
    expected_output_path = str(tmpdir / "tool_resource.json")
    assert task.output_path == expected_output_path

    # write a mock target resource and try loading it
    Path(expected_output_path).write_text(json.dumps({"@id": CIRCUIT_ID}))
    remote_entity = task._get_remote_entity()
    assert remote_entity.get_id() == CIRCUIT_ID
    assert remote_entity._type == "DetailedCircuit"

    # check that the activity created has the correct config and generated entity linked
    with patch.object(task, "publish", lambda e: e):
        activity = task.run()
        used_config = activity.used_config
        assert used_config.get_url() == CONFIG_TOOL_URL
        generated = activity.generated
        assert generated.get_url() == CIRCUIT_URL

    # The variant tool will have one dependency, the CommandLineTool task that does not depend on nexus
    # resources, but rather takes the materialized inputs
    tool_task = task.requires()
    assert isinstance(tool_task, test_module.CommandLineToolTask)

    # ensure that the input values passed to the CommandLineToolTask are the expected ones, with the
    # output directory included.
    tool_task.input_values == expected_input_values

    assert tool_task.runtime_config == task.runtime_config

    # concretize cwl variant definition with input values passed to the task
    assert isinstance(tool_task.process, process.CommandLineToolProcess)

    # the CommandLineTool task has no requirements
    assert not tool_task.requires()

    # output dir is injected in input_values from the runtime config
    assert tool_task.output_dir == runtime_config["output_dir"]

    # one NexusResource output
    task_outputs = tool_task.output()
    assert len(task_outputs) == 1
    assert (
        task_outputs["partial_circuit"].path == runtime_config["output_dir"] + "/tool_resource.json"
    )
    # check runtime env vars
    env_vars = tool_task.runtime_env_vars
    assert env_vars == MOCK_ENV_VARS | {"https_proxy": "http://bbpproxy.epfl.ch:80/"}

    # check executable tool command
    command = tool_task.command

    assert command == (
        ". /etc/profile.d/modules.sh && module purge && "
        "export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable spykfunc parquet-converters py-cwl-registry && "
        "export NEXUS_TOKEN=my-token NEXUS_WORKFLOW=my-workflow NEXUS_BASE=my-base NEXUS_ORG=my-org "
        "https_proxy=http://bbpproxy.epfl.ch:80/ && "
        "cwl-registry execute connectome-filtering-synapses mono-execution "
        "--configuration-id https://config-tool-id "
        "--circuit-id htpps://circuit-id "
        "--variant-id https://variant-tool-id "
        f'--output-dir {runtime_config["output_dir"]}'
    )


@patch_nexus
@patch.dict(
    os.environ,
    MOCK_ENV_VARS,
    clear=True,
)
def test_variant_workflow_task(tmpdir):

    output_dir = str(tmpdir)

    runtime_config = {
        "output_dir": output_dir,
        "host_config": {"host": "my-host"},
    }

    task = test_module.VariantWorkflowTask(
        variant_url=VARIANT_WORKFLOW_URL,
        config_url=CONFIG_WORKFLOW_URL,
        runtime_config=runtime_config,
    )

    # check variant configuration & input values
    config = task._config
    assert config.get_url() == CONFIG_WORKFLOW_URL

    # Variant level tasks should have variant targets that query for the activity existence
    assert isinstance(task.output(), VariantTarget)

    # expected input values are the input values to the task plus the output directory from the
    # runtime config that is added to the variant input values
    expected_input_values = MockWorkflow.input_values | {"output_dir": output_dir}
    assert task._input_values == expected_input_values

    # check cwl variant definition
    definition = task._definition
    assert isinstance(definition, cwl.Workflow)

    expected_env_vars = MOCK_ENV_VARS | {"https_proxy": "http://bbpproxy.epfl.ch:80/"}

    # check workflow step tasks
    step_tasks = task.requires()
    assert step_tasks.keys() == {
        "dirs",
        "stage",
        "transform",
        "connectome_filtering",
        "parquet_to_sonata",
        "register",
    }

    task_stage = step_tasks["stage"]

    assert task_stage.requires().keys() == {"dirs"}
    assert task_stage.runtime_env_vars == expected_env_vars

    assert task_stage.output().keys() == {
        "configuration_file",
        "circuit_file",
        "variant_file",
        "atlas_file",
        "edges_file",
    }

    assert task_stage.command == (
        ". /etc/profile.d/modules.sh && module purge && "
        "export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable py-cwl-registry && "
        "export NEXUS_TOKEN=my-token NEXUS_WORKFLOW=my-workflow NEXUS_BASE=my-base NEXUS_ORG=my-org "
        "https_proxy=http://bbpproxy.epfl.ch:80/ && "
        "cwl-registry execute connectome-filtering-synapses stage "
        f"--configuration-id {CONFIG_WORKFLOW_ID} "
        f"--circuit-id {CIRCUIT_ID} "
        f"--variant-id {VARIANT_WORKFLOW_ID} "
        f"--staging-dir {output_dir}/stage "
        f"--output-configuration-file {output_dir}/stage/staged_configuration_file.json "
        f"--output-circuit-file {output_dir}/stage/circuit_config.json "
        f"--output-variant-file {output_dir}/stage/variant.cwl "
        f"--output-atlas-file {output_dir}/stage/atlas.json "
        f"--output-edges-file {output_dir}/stage/edges.h5"
    )

    task_register = step_tasks["register"]

    assert task_register.requires().keys() == {"parquet_to_sonata", "dirs"}
    assert task_register.output().keys() == {"resource_file"}
    assert task_register.runtime_env_vars == expected_env_vars

    assert task_register.command == (
        ". /etc/profile.d/modules.sh && module purge && "
        "export MODULEPATH=/gpfs/bbp.cscs.ch/ssd/apps/bsd/modules/_meta && "
        "module load unstable py-cwl-registry && "
        "export NEXUS_TOKEN=my-token NEXUS_WORKFLOW=my-workflow NEXUS_BASE=my-base NEXUS_ORG=my-org "
        "https_proxy=http://bbpproxy.epfl.ch:80/ && "
        "cwl-registry execute connectome-filtering-synapses register "
        f"--circuit-id {CIRCUIT_ID} "
        f"--edges-file {output_dir}/build/edges.h5 "
        f"--output-dir {output_dir}/build "
        f"--output-resource-file {output_dir}/workflow_resource.json"
    )

    # check output path associated to this task.
    expected_output_path = str(tmpdir / "workflow_resource.json")
    assert task.output_path == expected_output_path

    # raise if not exactly one output resource
    with patch.object(task, "_definition", Mock(outputs={})):
        with pytest.raises(
            WorkflowError, match="Workflow should have only one Nexus Resource as output."
        ):
            task.output_path

    # raise if source name not in upstream requirements
    with patch.object(task, "requires", lambda: {}):
        with pytest.raises(WorkflowError, match="Workflow could not fetch"):
            task.output_path

    # raise if required upstream output not in source's outputs
    mock_requires = {name: Mock(output=lambda: {}) for name in task.requires()}
    with patch.object(task, "requires", lambda: mock_requires):
        with pytest.raises(WorkflowError, match="Workflow failed to fetch output"):
            task.output_path

    # write a mock target resource and try loading it
    Path(expected_output_path).write_text(json.dumps({"@id": CIRCUIT_ID}))
    remote_entity = task._get_remote_entity()
    assert remote_entity.get_id() == CIRCUIT_ID
    assert remote_entity._type == "DetailedCircuit"

    # check that the activity created has the correct config and generated entity linked
    with patch.object(task, "publish", lambda e: e):
        activity = task.run()
        used_config = activity.used_config
        assert used_config.get_url() == CONFIG_WORKFLOW_URL
        generated = activity.generated
        assert generated.get_url() == CIRCUIT_URL


def test_local_entity__backwards_compatibility(tmp_path):
    """Test backwards compatibility with forge json output file."""
    path1 = tmp_path / "resource_forge.json"
    Path(path1).write_text(json.dumps({"id": "foo", "label": "foo-label"}))

    with patch("bbp_workflow.generation.variant.get_entity") as patched:
        res = test_module._get_local_entity(path1)
        patched.assert_called_once_with(
            resource_id="foo",
            cls=Entity,
        )


def test_local_entity__jsonld(tmp_path):
    """Test compatibility with jsonld local file."""
    path2 = tmp_path / "resource_entm.json"

    Path(path2).write_text(json.dumps({"@id": "bar", "label": "bar-label"}))

    with patch("bbp_workflow.generation.variant.get_entity") as patched:
        res = test_module._get_local_entity(path2)
        patched.assert_called_once_with(
            resource_id="bar",
            cls=Entity,
        )


def test_workflow_step_task(tmpdir, workflow_variant):

    outdir = str(tmpdir)

    runtime_config = {
        "output_dir": outdir,
        "host_config": {"host": "my-host"},
    }

    input_values = MockWorkflow.input_values | {"output_dir": outdir}

    workflow_variant = workflow_variant.tool_definition

    res = test_module.WorkflowStepTask(
        name="stage",
        definition=workflow_variant,
        input_values=input_values,
        runtime_config=runtime_config,
    )

    assert res.output_dir == runtime_config["output_dir"]

    step_process = res.process
    assert isinstance(step_process, process.CommandLineToolProcess)

    outputs = res.output()

    host = runtime_config["host_config"]["host"]

    expected_outputs = {
        "configuration_file": RemoteTarget(
            host=host, path=str(Path(outdir, "stage/staged_configuration_file.json"))
        ),
        "circuit_file": RemoteTarget(
            host=host, path=str(Path(outdir, "stage/circuit_config.json"))
        ),
        "variant_file": RemoteTarget(host=host, path=str(Path(outdir, "stage/variant.cwl"))),
        "atlas_file": RemoteTarget(host=host, path=str(Path(outdir, "stage/atlas.json"))),
        "edges_file": RemoteTarget(host=host, path=str(Path(outdir, "stage/edges.h5"))),
    }

    assert outputs.keys() == expected_outputs.keys()

    for name, out in outputs.items():
        assert out.path == expected_outputs[name].path

    requirements = res.requires()
    assert requirements.keys() == {"dirs"}

    log_path = res._generate_log_path()
    assert str(log_path).startswith(outdir)
