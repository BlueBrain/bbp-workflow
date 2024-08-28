# SPDX-License-Identifier: Apache-2.0

"""Specific simulation analysis SBO tasks."""

import json
from functools import cached_property
from pathlib import Path

from entity_management.analysis import (
    AnalysisReport,
    Derivation,
    MultiCumulativeSimulationCampaignAnalysis,
)
from entity_management.simulation import Simulation
from luigi import DictParameter, ListParameter, Parameter

from bbp_workflow.sbo.analysis.common.task import (
    AnalyseGeneric,
    MultiAnalyseGeneric,
    MultiAnalyseGenericMeta,
    get_analysis_config_schema,
)
from bbp_workflow.sbo.sim.task import RunSimCampaign, RunSimCampaignMeta
from bbp_workflow.settings import L
from bbp_workflow.simulation.util import read_config
from bbp_workflow.util import _get_content_type, _render_template, unfreeze


class AnalyseSimCampaign(AnalyseGeneric):
    """Analyse a Simulation Campaign."""

    analysis_config = DictParameter(
        description="""
            Analysis configuration dict. The following placeholders can be specified,
            and they will be replaced when the analysis is run:

            $SIMULATION_CAMPAIGN_FILE (str): path to the configuration file of the sim campaign.
            $SCRATCH_PATH (str): directory to be used to store any artefact produced by the script.
            """,
    )  #:

    def requires(self):
        """Return the Tasks that this Task depends on."""
        return {
            "campaign": RunSimCampaign(),
            **super().requires(),
        }

    def _get_analysis_config_content(self):
        """Return the rendered content of the analysis config."""
        simulation_campaign_file = self.requires()["campaign"].input().path
        analysis_config_content = json.dumps(unfreeze(self.analysis_config))
        return _render_template(
            analysis_config_content,
            SIMULATION_CAMPAIGN_FILE=simulation_campaign_file,
            SCRATCH_PATH=self.scratch_path,
        )


class MultiAnalyseSimCampaign(MultiAnalyseGeneric):
    """Run multiple analysis on a Simulation Campaign."""

    _individual_task_class = AnalyseSimCampaign
    analysis_configs = ListParameter(
        description="List of analysis configuration dictionaries.",
        schema=get_analysis_config_schema(_individual_task_class.__name__),
    )  #:


class MultiAnalyseSimCampaignMeta(MultiAnalyseGenericMeta):
    """MetaTask for MultiAnalyseSimCampaign."""

    activity_class = Parameter(
        default=(
            f"{MultiCumulativeSimulationCampaignAnalysis.__module__}."
            f"{MultiCumulativeSimulationCampaignAnalysis.__name__}"
        )
    )

    def requires(self):
        """Return the Tasks that this Task depends on."""
        return RunSimCampaignMeta()

    def _get_analysed_entity(self):
        """Return the analysed entity, to be used as derivation of CumulativeAnalysisReport."""
        return self.requires().output().activity.generated

    @cached_property
    def _index_to_url(self):
        """Return the dict that maps simulation_id -> nexus url."""
        sim_campaign = self.requires().output().activity.generated
        sims = read_config(sim_campaign.simulations.as_dict())
        return dict(enumerate(sims.data.ravel()))

    def _publish_part(self, metadata, task, activity):
        """Publish a File and the enclosing entity to Nexus, returning the entity.

        Args:
            metadata: dict containing metadata for the partial analysis report.
            task: instance of AnalyseSimCampaign.
            activity: used to link: AnalysisReport -> wasGeneratedBy -> Activity
        """
        path = Path(metadata["path"])
        ids = metadata.get("simulation_ids", [])
        if len(ids) == 1:
            simulation_id = ids[0]
            name = f"{task.report_name} (Simulation {simulation_id})"
            derivation = Derivation(entity=Simulation.from_url(self._index_to_url[simulation_id]))
        else:
            L.warning("The relation report -> simulation won't be created because ids=%s", ids)
            name = task.report_name
            derivation = None
        content_type = metadata.get("content_type") or _get_content_type(path)
        distribution = self.from_file(path, name=path.name, content_type=content_type)
        resource = AnalysisReport(
            name=name,
            description=task.report_description,
            categories=list(task.categories),
            types=list(task.types),
            distribution=distribution,
            derivation=derivation,
        )
        return self.publish(resource, activity=activity)
