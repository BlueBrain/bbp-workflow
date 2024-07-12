# SPDX-License-Identifier: Apache-2.0

"""Generic entities for SBO tasks."""

from entity_management.base import Derivation, attributes
from entity_management.core import Agent, Contribution, Entity
from entity_management.util import AttrOf
from entity_management.workflow import BbpWorkflowActivity


class EnsuredList(list):
    """List to be used when the argument passed to the constructor might not be an iterable."""

    def __init__(self, iterable=()):
        """Init the object, always passing an iterable to the superclass.

        This may be needed when an attribute is defined as a list, but there is only one element,
        and it's returned from Nexus as a single item instead of a list.
        """
        if not isinstance(iterable, (list, tuple)):
            iterable = (iterable,)
        super().__init__(iterable)


@attributes({})
class MultiCumulativeSimulationCampaignAnalysis(BbpWorkflowActivity):
    """Activity generating multiple analyses of a Simulation Campaign."""


@attributes({})
class MultiEModelAnalysis(BbpWorkflowActivity):
    """Activity generating multiple analyses of an EModel."""


@attributes(
    {
        "name": AttrOf(str, default=None),
        "description": AttrOf(str, default=None),
        "codeRepository": AttrOf(str, default=None),
        "subdirectory": AttrOf(str, default=None),
        "command": AttrOf(str, default=None),
    }
)
class AnalysisSoftwareSourceCode(Agent):
    """AnalysisSoftwareSourceCode."""


@attributes(
    {
        "derivation": AttrOf(Derivation),
        "categories": AttrOf(EnsuredList[str], default=None),
        "types": AttrOf(EnsuredList[str], default=None),
    }
)
class AnalysisReport(Entity):
    """AnalysisReport."""


@attributes(
    {
        "derivation": AttrOf(Derivation),
        "categories": AttrOf(EnsuredList[str], default=None),
        "types": AttrOf(EnsuredList[str], default=None),
        "hasPart": AttrOf(EnsuredList[AnalysisReport], default=None),
        "index": AttrOf(int, default=None),
        "contribution": AttrOf(EnsuredList[Contribution], default=None),
    }
)
class CumulativeAnalysisReport(Entity):
    """CumulativeAnalysisReport."""


@attributes(
    {
        "hasPart": AttrOf(EnsuredList[CumulativeAnalysisReport], default=None),
    }
)
class MultiCumulativeAnalysisReport(Entity):
    """MultiCumulativeAnalysisReport."""
