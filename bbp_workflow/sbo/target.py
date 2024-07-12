# SPDX-License-Identifier: Apache-2.0

"""Luigi target for SBO tasks."""

from entity_management.workflow import BbpWorkflowConfig
from luigi.target import Target

from bbp_workflow.util import import_func

from .util import ConfigFileManager


class BbpWorkflowTarget(Target):
    """BbpWorkflow SBO task luigi target."""

    def __init__(self, task):
        """"""
        self.task = task

    def exists(self):
        """Return ``True`` if :attr:`self.task.config_url` was used by the Done activity."""
        activity = self.activity  # save the activity, to avoid hitting Nexus twice
        return activity is not None and activity.status == "Done"

    @property
    def activity(self):
        """Corresponding activity instance that links config and task resource.

        Returns:
            Knowledge graph ``self.task.activity_class`` instance or ``None`` if not found.
        """
        config = BbpWorkflowConfig.from_url(self.task.config_url)
        assert config
        ConfigFileManager.instance().add_config(self.task, config)
        activities = import_func(self.task.activity_class).used_config(
            config,
            config.get_rev(),
            base=self.task.kg_base,
            org=self.task.kg_org,
            proj=self.task.kg_proj,
        )
        return next(activities, None)
