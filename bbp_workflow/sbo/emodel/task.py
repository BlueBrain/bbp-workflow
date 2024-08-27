# SPDX-License-Identifier: Apache-2.0

"""Specific simulation campaign SBO tasks."""

import logging
import tempfile
from configparser import BasicInterpolation, ConfigParser

import bluepyemodel.efeatures_extraction.auto_targets
import bluepyemodel.tasks.emodel_creation.optimisation
from bluepyemodel.efeatures_extraction.targets_configurator import TargetsConfigurator
from bluepyemodel.emodel_pipeline.emodel_pipeline import EModel_pipeline
from bluepyemodel.emodel_pipeline.emodel_settings import EModelPipelineSettings
from bluepyemodel.model.model_configurator import ModelConfigurator
from entity_management.core import Entity
from entity_management.workflow import BbpWorkflowActivity
from luigi import Parameter

from bbp_workflow.sbo.task import BaseTask, MetaTask

L = logging.getLogger(__name__)


class EModelOptimizationConfig(Entity):
    """The .cfg file for `bluepyemodel.tasks.*` luigi tasks."""


class LaunchEModelOptimisation(
    bluepyemodel.tasks.emodel_creation.optimisation.EModelCreation, BaseTask
):
    """Wrapper around bluepyemodel; allows for storing the `config_nexus`."""

    # nexus ID of a entity with a distribution that is the .json config
    # created by the UI; it is used to configure the nexus entities
    # that are needed by the emodel-optimisation; which are attached to the
    # `iteration_tag`, which is contained in the .cfg, and also set by the UI
    config_nexus = Parameter()


class EModelOptimizationActivity(BbpWorkflowActivity):
    """Activity for emodel-optimisation."""


class LaunchEModelOptimisationMeta(MetaTask):
    """Register entities required for the bluepyemodel optimisation with NEXUS.

    Prior to bluepyemodel running, it needs many entities created in NEXUS,
    all keyed to the `iteration_tag` that keeps track of a particular execution.
    """

    activity_class = Parameter(
        default=f"{EModelOptimizationActivity.__module__}.{EModelOptimizationActivity.__name__}"
    )

    def publish_resource(self, activity, resource):
        """Publish the resource."""
        return activity, resource

    def pre_publish_resource(self, activity):  # pragma: no cover
        # XXX should we look at activity.status; right now it's just used to
        # keep this task idempotent; ie: don't re-register emodel-optimisation
        # entities twice
        if activity.get_rev() == 1:
            with tempfile.TemporaryDirectory() as t:
                res = activity.used_config.distribution.download(path=t)
                cfg = ConfigParser(interpolation=BasicInterpolation())
                cfg.read([res])

            # the asserts are here to keep the .cfg and the .json configs in sync,
            # much of the information is duplicated, but that's to keep the bluepyemodel
            # separated from bbp-workflow, and not too intrusive
            emodel_api_config = cfg["EmodelAPIConfig"]
            assert emodel_api_config["nexus_project"] == self.kg_proj, "Project mismatch"
            assert emodel_api_config["nexus_organisation"] == self.kg_org, "Organization mismatch"
            assert emodel_api_config["nexus_endpoint"] == self.kg_base, "Endpoint mismatch"

            emodel_opt_config = cfg["LaunchEModelOptimisation"]
            input_data = EModelOptimizationConfig.from_id(
                emodel_opt_config["config_nexus"]
            ).distribution.as_dict()

            assert emodel_opt_config["emodel"] == input_data["name"]
            assert emodel_opt_config["etype"] == input_data["eType"]
            assert emodel_opt_config.get("mtype", None) == input_data.get("mType", None)
            assert emodel_opt_config.get("ttype", None) == input_data.get("tType", None)
            assert emodel_opt_config["species"] == input_data["species"]
            assert emodel_opt_config["brain_region"] == input_data["brainRegionName"]

            pipeline = _get_pipeline(
                iteration_tag=emodel_opt_config["iteration_tag"],
                input_data=input_data,
                nexus_organisation=self.kg_org,
                nexus_project=self.kg_proj,
                nexus_endpoint="prod",
                forge_path=emodel_api_config["forge_path"],
                forge_ontology_path=emodel_api_config["forge_ontology_path"],
            )

            _save_targets_config(
                pipeline,
                traces=input_data["traces"],
                ecodes_metadata=input_data["ecodes_metadata"],
                ljp=input_data["parameters"]["LJP (liquid junction potential)"],
            )

            # EModelPipelineSettings
            max_ngen = input_data["parameters"]["Max optimisation generation"]
            is_placeholder = input_data["morphologies"][0]["isPlaceholder"]
            if is_placeholder:
                morph_modifiers = []
            else:
                morph_modifiers = input_data.get("morph_modifiers", None)

            _save_pipeline_settings(
                pipeline,
                max_ngen=max_ngen,
                morph_modifiers=morph_modifiers,
            )

            morphology_name = input_data["morphologies"][0]["name"]
            _save_model_configuration(pipeline, morphology_name, input_data["mechanisms"]["raw"])
        return activity, None


def _get_pipeline(
    iteration_tag,
    input_data,
    nexus_organisation,
    nexus_project,
    nexus_endpoint,
    forge_path,
    forge_ontology_path,
):  # pragma: no cover
    """Get `pipeline` object used to register NEXUS entities."""
    L.info("EModel_pipeline")
    pipeline = EModel_pipeline(
        iteration_tag=iteration_tag,
        emodel=input_data["name"],
        species=input_data["species"],
        brain_region=input_data["brainRegionName"],
        mtype=input_data.get("mType", None),
        etype=input_data["eType"],
        ttype=input_data.get("tType", None),
        forge_path=forge_path,
        forge_ontology_path=forge_ontology_path,
        nexus_organisation=nexus_organisation,
        nexus_project=nexus_project,
        nexus_endpoint=nexus_endpoint,
        use_ipyparallel=True,
        data_access_point="nexus",
    )

    return pipeline


def _save_targets_config(
    pipeline, traces, ecodes_metadata, ljp, auto_target=None
):  # pragma: no cover
    """Note: e-model optimisation pipeline uses "targets" / "traces"."""
    # XXX Note: ecodes_metadata is currently replicated for all the `traces`
    # and the LJP value is overwritten
    ecodes_metadata = ecodes_metadata.copy()
    for k in ecodes_metadata:
        ecodes_metadata[k]["ljp"] = ljp

    L.info("traces")
    files = []
    for trace in traces:
        files.append(
            {
                "cell_name": trace["cellName"],
                "resource_id": trace["@id"],
                "etype": trace["eType"],
                "species": trace["subjectSpecies"],
                "ecodes": ecodes_metadata,
            }
        )

    configurator = TargetsConfigurator(pipeline.access_point)

    auto_targets = bluepyemodel.efeatures_extraction.auto_targets.get_auto_target_from_presets(
        [
            auto_target,
        ]
        if auto_target is not None
        else ["firing_pattern", "ap_waveform", "iv", "validation"]
    )

    configurator.new_configuration(
        files=files, targets=None, protocols_rheobase=["IDthresh"], auto_targets=auto_targets
    )

    configurator.save_configuration()
    L.info("TargetsConfigurator: %s", configurator.configuration.as_dict())


def _save_pipeline_settings(
    pipeline,
    max_ngen,
    morph_modifiers,
):  # pragma: no cover
    """E-Model parameters related to the actual optimisation."""
    L.info("EModelPipelineSettings")
    pipeline_settings = EModelPipelineSettings(
        max_ngen=max_ngen,
        morph_modifiers=morph_modifiers,
        name_rmp_protocol="IV_0",
        name_Rin_protocol="IV_-40",
        validation_protocols=[
            "IDhyperpol_150",
        ],
        efel_settings=None,
        extract_absolute_amplitudes=False,
        optimiser="SO-CMA",
        optimisation_params={"offspring_size": 20},
        optimisation_timeout=600.0,
        optimisation_batch_size=5,
        n_model=1,
        max_n_batch=1,
        threshold_efeature_std=0.1,
        validation_threshold=50.0,  # TODO: need to be decreased (e.g. 5) for production
        plot_currentscape=True,
        currentscape_config=None,
        neuron_dt=None,
        stochasticity=True,
        interpolate_RMP_extraction=True,
    )

    pipeline.access_point.store_pipeline_settings(pipeline_settings)


def _save_model_configuration(pipeline, morphology_name, parameters):  # pragma: no cover
    configurator = ModelConfigurator(pipeline.access_point)
    configurator.new_configuration()

    configurator.configuration.init_from_legacy_dict(parameters, {"name": morphology_name})
    configurator.save_configuration()
    L.info("ModelConfigurator: %s", configurator.configuration)
