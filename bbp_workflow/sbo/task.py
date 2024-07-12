# SPDX-License-Identifier: Apache-2.0

"""Collection of generic SBO tasks for the Workflow engine."""

import abc
from datetime import datetime

from entity_management.workflow import BbpWorkflowActivity, BbpWorkflowConfig
from luigi import Config, Event, OptionalParameter, Parameter
from luigi.parameter import MissingParameterException
from luigi.task_register import Register, TaskClassNotFoundException

from bbp_workflow.task import KgTask
from bbp_workflow.util import import_func

from .target import BbpWorkflowTarget
from .util import ConfigFileManager, config_id


class RegisterBbpWorkflowConfig(KgTask):
    """Register generic bbp-workflow config.

    Args:
        url (luigi.Parameter): if provided, will override the same config entity.
        config-file-name (luigi.OptionalParameter): config in external file.
    """

    url = OptionalParameter(default=None)
    config_file_name = Parameter()

    def run(self):
        """"""
        if self.url:
            config = BbpWorkflowConfig.from_url(self.url)
            config = config.evolve(
                distribution=self.from_file(
                    self.config_file_name,
                    resource_id=config.distribution.get_id(),
                    content_type="text/plain",
                )
            )
            assert False, "handle file replace"
        else:
            config = BbpWorkflowConfig(distribution=self.from_file(self.config_file_name))
        config = self.publish(config)
        self.done("config-url:", config)
        return config


class _MetaParameter(OptionalParameter):
    """Parameter that can be set in a Task, depending on the MetaTask.

    If the parameter isn't specified in the BbpWorkflowConfig entity passed as config-url
    to the MetaTask, then it's determined by uuid and revision in the config url.
    """

    def _get_value(self, task_name, param_name):
        value = super()._get_value(task_name, param_name)
        if value is not None:  # default is none as OptionalParameter
            return value
        try:
            base_task = Register.get_task_cls(task_name)
            if issubclass(base_task, BaseTask):
                meta_task = Register.get_task_cls(f"{task_name}Meta")
                if issubclass(meta_task, MetaTask):
                    value = config_id(meta_task().config_url)
        except (TaskClassNotFoundException, MissingParameterException):
            pass
        return value


class BaseTask(Config, abc.ABC):
    """"""

    config_id = _MetaParameter(
        default=None,
        description="Configuration identifier which can be used when composing unique paths.",
    )

    resource_url = OptionalParameter(
        default=None,
        description="Optional nexus resource url that task can update to reflect status.",
    )


class MetaTask(KgTask, abc.ABC):
    """Base abstract SBO luigi task."""

    activity_class = Parameter(
        default=f"{BbpWorkflowActivity.__module__}." f"{BbpWorkflowActivity.__name__}"
    )

    config_url = Parameter(description="Configuration nexus URL that parametrizes `self.task`.")

    def run(self):
        """"""
        config = BbpWorkflowConfig.from_url(self.config_url)
        activity = self.output().activity
        resource = None
        do_pre_publish = True
        if activity is None:
            activity = import_func(self.activity_class)(
                used_config=config,
                used_rev=config.get_rev(),
                startedAtTime=datetime.utcnow(),
                endedAtTime=None,
                status="Running",
            )
            activity = self.publish(activity)
        elif activity.endedAtTime is not None:
            # restart failed
            activity = activity.evolve(
                used_config=config,
                used_rev=config.get_rev(),
                startedAtTime=datetime.utcnow(),
                endedAtTime=None,
                status="Running",
            )
            activity = self.publish(activity)
        else:
            do_pre_publish = False
        try:
            if do_pre_publish:
                activity, resource = self.pre_publish_resource(activity)
            else:
                resource = activity.generated
            # pass resource_url in case base task needs to update nexus entity
            yield self.get_base_task_instance(
                resource_url=None if resource is None else resource.get_url()
            )
            activity, resource = self.publish_resource(activity, resource)
            activity = activity.evolve(endedAtTime=datetime.utcnow(), status="Done")
            activity = self.publish(activity, sync_index=True)
            if self.set_tracking_url:
                if resource is not None:
                    resource_id = resource.get_id()
                else:
                    resource_id = activity.get_id()
                # pylint: disable=not-callable
                self.set_tracking_url(self.make_web_link(resource_id=resource_id))
        except Exception:  # pylint: disable=broad-exception-caught
            activity = activity.evolve(endedAtTime=datetime.utcnow(), status="Failed")
            self.publish(activity, sync_index=True)
            raise

    def get_base_task_instance(self, **kwargs):
        """Instantiate corresponding BaseTask."""
        base_task_class = Register.get_task_cls(self.__class__.__name__[:-4])
        assert issubclass(base_task_class, BaseTask)
        return base_task_class(**kwargs)

    def pre_publish_resource(self, activity):
        """Can be used to publish initial version of the resource before base task is yielded."""
        return activity, None

    def get_base_task_params(self):
        """Collect extra params to be passed for the base task."""
        return {}

    @abc.abstractmethod
    def publish_resource(self, activity, resource):
        """Specific task should be responsible for actually publishing the resource."""

    def output(self):
        """"""
        return BbpWorkflowTarget(self)


@MetaTask.event_handler(Event.START)
def _meta_task_start(task):
    config = BbpWorkflowConfig.from_url(task.config_url)
    assert config
    ConfigFileManager.instance().add_config(task, config)
