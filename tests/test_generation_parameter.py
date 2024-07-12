from pathlib import Path

import luigi
import pytest
from blue_cwl.core import cwl, parse_cwl_file
from blue_cwl.utils import write_yaml
from luigi_tools.util import set_luigi_config

from bbp_workflow.generation import parameter as test_module

DATA_DIR = Path(__file__).parent / "data"


@pytest.fixture
def cwl_data():
    return {
        "cwlVersion": "v1.2",
        "class": "CommandLineTool",
        "baseCommand": ["cp"],
        "inputs": {
            "source_file": {
                "type": "File",
                "inputBinding": {"position": 1},
            },
            "target_file": {
                "type": "string",
                "inputBinding": {"position": 2},
            },
        },
        "outputs": {
            "out_file": {
                "type": "File",
                "outputBinding": {"glob": "$(inputs.target_file)"},
            },
        },
    }


@pytest.fixture
def tool(tmp_path, cwl_data):
    path = Path(tmp_path / "copy.cwl")
    write_yaml(filepath=path, data=cwl_data)
    return parse_cwl_file(path)


def test_cwl_model_parameter(tool):

    param = test_module.CWLModelParameter(model=cwl.CommandLineTool)

    serialized = param.serialize(tool)

    normalized = param.normalize(tool)
    assert normalized == tool

    normalized = param.normalize(serialized)
    assert normalized == tool


def test_cwl_model_parameter__build(tool):

    class A(luigi.Task):

        a = test_module.CWLModelParameter(model=cwl.CommandLineTool)

        def run(self):
            assert self.a.inputs.keys() == {"source_file", "target_file"}
            assert self.a.outputs.keys() == {"out_file"}

    with set_luigi_config({"A": {"a": tool.to_string()}}):
        assert luigi.build([A()], local_scheduler=True)

    assert luigi.build([A(a=tool)], local_scheduler=True)
