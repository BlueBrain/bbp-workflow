# SPDX-License-Identifier: Apache-2.0

"""Specific simulation campaign SBO tasks."""

from entity_management.base import BrainLocation, OntologyTerm, Subject
from entity_management.core import DataDownload
from entity_management.simulation import DetailedCircuit
from luigi import ExternalTask, LocalTarget, OptionalParameter, Parameter
from luigi.parameter import MissingParameterException
from luigi.task_register import Register, TaskClassNotFoundException

from bbp_workflow.generation.entity import VariantTaskActivity
from bbp_workflow.sbo.task import BaseTask, MetaTask


class RegDetailedCircuit(ExternalTask, BaseTask):
    """Does nothing as circuit config should be already present."""

    circuit_config_path = Parameter(
        description="Full path to the CircuitConfig including file name."
    )  #:

    def output(self):
        return LocalTarget(path=self.circuit_config_path)


class RegDetailedCircuitMeta(MetaTask):
    """Meta task that specifies which task does the job and what nexus resource is registered."""

    activity_class = Parameter(
        default=f"{VariantTaskActivity.__module__}." f"{VariantTaskActivity.__name__}"
    )

    def publish_resource(self, activity, resource):
        task = self.get_base_task_instance()

        if task.brain_region:
            brain_location = BrainLocation(brainRegion=OntologyTerm(**task.brain_region))
        else:
            brain_location = None

        subject = Subject(species=OntologyTerm(**task.species), strain=OntologyTerm(**task.strain))
        resource = DetailedCircuit(
            name=task.name,
            description=task.description,
            circuitType=task.circuit_type,
            circuitConfigPath=DataDownload(url=f"file://{task.output().path}"),
            brainLocation=brain_location,
            subject=subject,
        )
        resource = self.publish(resource)

        activity = activity.evolve(generated=resource)
        activity = self.publish(activity)

        return activity, resource


class _MetaParameter(OptionalParameter):
    def _get_value(self, task_name, param_name):
        value = super()._get_value(task_name, param_name)
        if value is not None:  # default is none as OptionalParameter
            return value
        try:
            base_task = Register.get_task_cls(task_name)
            if issubclass(base_task, BaseTask):
                meta_task = Register.get_task_cls(f"{task_name}Meta")
                if issubclass(meta_task, MetaTask):
                    circuit = meta_task().output().activity.generated
                    value = circuit.circuitConfigPath.url.replace("file://", "")
        except (TaskClassNotFoundException, MissingParameterException):
            pass
        return value


class FindDetailedCircuit(ExternalTask, BaseTask):
    """Does nothing as circuit config should be already present."""

    circuit_config_path = _MetaParameter(
        default=None, description="Full path to the CircuitConfig including file name."
    )  #:

    def output(self):
        return LocalTarget(path=self.circuit_config_path)


class FindDetailedCircuitMeta(MetaTask):
    """Meta task that specifies which task does the job and what nexus resource is registered."""

    activity_class = Parameter(
        default=f"{VariantTaskActivity.__module__}." f"{VariantTaskActivity.__name__}"
    )

    def publish_resource(self, activity, resource):
        assert False, "Should not publish anything just find existing circuit"
