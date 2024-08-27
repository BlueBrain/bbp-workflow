# SPDX-License-Identifier: Apache-2.0

"""Specific EModel analysis SBO tasks."""

import json
import textwrap
from pathlib import Path

from blue_cwl.me_model.staging import stage_emodel
from entity_management.analysis import AnalysisReport, MultiEModelAnalysis
from entity_management.base import Derivation
from entity_management.emodel import EModel
from luigi import DictParameter, ListParameter, Parameter, PathParameter

from bbp_workflow.luigi import RemoteTarget, inherits
from bbp_workflow.sbo.analysis.common.task import (
    AnalyseGeneric,
    MultiAnalyseGeneric,
    MultiAnalyseGenericMeta,
    get_analysis_config_schema,
)
from bbp_workflow.sbo.task import BaseTask
from bbp_workflow.settings import DEFAULT_HOST
from bbp_workflow.task import KgCfg
from bbp_workflow.util import _get_content_type, _render_template, unfreeze


def _save_emodel_recipes(emodel, emodel_paths, recipes_path):
    """Save recipes.json in BluePyEModel format.

    Args:
        emodel (EModel): EModel instance.
        emodel_paths (dict): dict of paths used by the EModel.
        recipes_path (Path): path to the recipes file to be written.
    """

    def _relative(path):
        return str(Path(path).resolve().relative_to(recipes_path.parent.resolve()))

    morphology_path = Path(emodel_paths["morphology"])
    data = {
        "morph_path": _relative(morphology_path.parent),
        "morphology": morphology_path.name,
        "params": _relative(emodel_paths["params"]["bounds"]),
        "final": _relative(emodel_paths["params"]["values"]),
    }
    for key in "features", "pipeline_settings":
        if value := emodel_paths.get(key):
            data[key] = _relative(value)
    data = {emodel.eModel: data}
    recipes_path.write_text(json.dumps(data, indent=2), encoding="utf-8")


@inherits(KgCfg)
class StageEModel(BaseTask):
    """StageEModel task."""

    emodel_url = Parameter(
        description="Nexus url of the EModel to be analysed.",
    )  #:
    emodel_path = PathParameter(
        description="Path to the directory that will contain the EModel.",
    )  #:
    mechanisms_dir = Parameter(
        description="Subdirectory that will contain the mod files, relative to emodel_path.",
    )  #:

    def run(self):
        self.emodel_path.mkdir(parents=True, exist_ok=True)
        emodel = EModel.from_url(self.emodel_url)
        emodel_paths = stage_emodel(
            emodel,
            staging_dir=self.emodel_path,
        )
        _save_emodel_recipes(
            emodel,
            emodel_paths=emodel_paths,
            recipes_path=self.emodel_path / "recipes.json",
        )
        Path(self.output().path).touch()

    def output(self):
        """Return the output that this Task produces."""
        return RemoteTarget(host=DEFAULT_HOST, path=self.emodel_path / ".SUCCESS")


class AnalyseEModel(AnalyseGeneric):
    """Analyse an EModel."""

    emodel_url = Parameter(description="Nexus url of the EModel to be analysed.")  #:
    mechanisms_dir = Parameter(
        description="Subdirectory that will contain the mod files, relative to emodel_path.",
        default="mechanisms",
    )  #:
    analysis_config = DictParameter(
        description="""
            Analysis configuration dict. The following placeholders can be specified,
            and they will be replaced when the analysis is run:

            $EMODEL_ID (str): Nexus id of the EModel.
            $EMODEL_URL (str): Nexus url of the EModel.
            $EMODEL_PATH (str): directory where the EModel data have been staged.
            $EMODEL_EMODEL (str): emodel.
            $EMODEL_ETYPE (str): etype.
            $EMODEL_ITERATION (str): iteration tag.
            $EMODEL_SEED (int): seed.
            $SCRATCH_PATH (str): directory to be used to store any artefact produced by the script.
            """,
    )  #:

    def _get_analysis_config_content(self):
        """Return the rendered content of the analysis config."""
        analysis_config_content = json.dumps(unfreeze(self.analysis_config))
        emodel = EModel.from_url(self.emodel_url)
        return _render_template(
            analysis_config_content,
            EMODEL_ID=emodel.get_id(),
            EMODEL_URL=emodel.get_url(),
            EMODEL_PATH=self.emodel_path,
            EMODEL_EMODEL=emodel.eModel,
            EMODEL_ETYPE=emodel.eType,
            EMODEL_ITERATION=emodel.iteration,
            EMODEL_SEED=emodel.seed,
            SCRATCH_PATH=self.scratch_path,
        )

    @property
    def env(self):
        """Comma separated or multi-line extra environment variables to export."""
        return ",".join(
            [
                "NEURON_MODULE_OPTIONS=-nogui",
                f"BLUECELLULAB_MOD_LIBRARY_PATH={self.emodel_path}",
                f"EMODEL_GENERALISATION_MOD_LIBRARY_PATH={self.emodel_path}",
            ],
        )

    @property
    def emodel_path(self):
        """Return the path to the EModel."""
        return self.workspace_path / "emodel"

    def requires(self):
        """Return the Tasks that this Task depends on."""
        return {
            "emodel": self.clone(
                StageEModel,
                emodel_path=self.emodel_path,
            ),
            **super().requires(),
        }

    def _pre_cmd(self):
        pre_cmd_1 = super()._pre_cmd()
        pre_cmd_2 = textwrap.dedent(
            f"""
            pushd {self.emodel_path}
            nrnivmodl {self.mechanisms_dir}
            popd
            """
        )
        return f"{pre_cmd_1}\n{pre_cmd_2}"


class MultiAnalyseEModel(MultiAnalyseGeneric):
    """Run multiple analysis on an EModel."""

    _individual_task_class = AnalyseEModel
    emodel_url = Parameter(description="Nexus url of the EModel to be analysed.")
    analysis_configs = ListParameter(
        description="List of analysis configuration dictionaries.",
        schema=get_analysis_config_schema(_individual_task_class.__name__),
    )  #:

    def _individual_task_parameters(self, task_index, tasks_config):
        """Return the parameters needed to create an instance of the individual task."""
        return {
            **super()._individual_task_parameters(task_index, tasks_config),
            "emodel_url": self.emodel_url,
        }


class MultiAnalyseEModelMeta(MultiAnalyseGenericMeta):
    """MetaTask for MultiAnalyseEModel."""

    activity_class = Parameter(
        default=f"{MultiEModelAnalysis.__module__}.{MultiEModelAnalysis.__name__}"
    )

    def _get_analysed_entity(self):
        """Return the analysed entity, to be used as derivation of CumulativeAnalysisReport."""
        multitask = self.get_base_task_instance()
        return EModel.from_url(multitask.emodel_url)

    def _publish_part(self, metadata, task, activity):
        """Publish a File and the enclosing entity to Nexus, returning the entity.

        Args:
            metadata: dict containing metadata for the partial analysis report.
            task: instance of AnalyseEModel.
            activity: used to link: AnalysisReport -> wasGeneratedBy -> Activity
        """
        path = Path(metadata["path"])
        name = task.report_name
        content_type = metadata.get("content_type") or _get_content_type(path)
        distribution = self.from_file(path, name=path.name, content_type=content_type)
        # same derivation as in CumulativeAnalysisReport
        derivation = Derivation(entity=self._get_analysed_entity())
        resource = AnalysisReport(
            name=name,
            description=task.report_description,
            categories=list(task.categories),
            types=list(task.types),
            distribution=distribution,
            derivation=derivation,
        )
        return self.publish(resource, activity=activity)
