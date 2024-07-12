"""Task tests."""

from luigi import Task
from luigi_tools.util import set_luigi_config
from pint import get_application_registry

from bbp_workflow.luigi import QuantityParameter
from bbp_workflow.task import LookupKgEntity

URL = "https://staging.nexus.ocp.bbp.epfl.ch/v1/resources/org/proj/_/UU-I-D"
UREG = get_application_registry()


def test_url_parsing():
    """Test entity lookup fills in kg details from the URL."""
    entity = LookupKgEntity(url=URL)
    assert entity.kg_base == "https://staging.nexus.ocp.bbp.epfl.ch/v1"
    assert entity.kg_org == "org"
    assert entity.kg_proj == "proj"


def test_url_parsing_no_override():
    """Test entity lookup fills in kg details from the URL but doesn't override."""
    entity = LookupKgEntity(url=URL, kg_proj="abc")
    assert entity.kg_base == "https://staging.nexus.ocp.bbp.epfl.ch/v1"
    assert entity.kg_org == "org"
    assert entity.kg_proj == "abc"


def test_url_parsing_with_cfg():
    """Test entity lookup fills in kg details from the URL passed in cfg file."""
    with set_luigi_config({"LookupKgEntity": {"url": URL}}, configfile="tmp_luigi.cfg"):
        entity = LookupKgEntity()
        assert entity.kg_base == "https://staging.nexus.ocp.bbp.epfl.ch/v1"
        assert entity.kg_org == "org"
        assert entity.kg_proj == "proj"


def test_quantity_param_with_cfg():
    """Test quantity parameter in cfg."""

    class _TestTask(Task):
        quantity = QuantityParameter()

    with set_luigi_config(
        {"DEFAULT": {"experimental-units": False}, "_TestTask": {"quantity": "10m"}},
        configfile="tmp_luigi.cfg",
    ):
        task = _TestTask()
        assert task.quantity == 10
        assert isinstance(task.quantity, int)
    with set_luigi_config(
        {"DEFAULT": {"experimental-units": False}, "_TestTask": {"quantity": "10.1m"}},
        configfile="tmp_luigi.cfg",
    ):
        task = _TestTask()
        assert task.quantity == 10.1
        assert isinstance(task.quantity, float)
    with set_luigi_config(
        {"DEFAULT": {"experimental-units": True}, "_TestTask": {"quantity": "10m"}},
        configfile="tmp_luigi.cfg",
    ):
        task = _TestTask()
        assert task.quantity == 10 * UREG.m
        assert isinstance(task.quantity.magnitude, int)
        assert task.quantity.units == UREG.meter
    with set_luigi_config(
        {"DEFAULT": {"experimental-units": True}, "_TestTask": {"quantity": "10.1m"}},
        configfile="tmp_luigi.cfg",
    ):
        task = _TestTask()
        assert task.quantity == 10.1 * UREG.m
        assert isinstance(task.quantity.magnitude, float)
        assert task.quantity.units == UREG.meter
