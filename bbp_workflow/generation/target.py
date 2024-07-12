# SPDX-License-Identifier: Apache-2.0

"""Targeting."""

import logging
import time

import luigi
from entity_management.core import WorkflowExecution
from entity_management.settings import WORKFLOW
from entity_management.util import get_entity
from entity_management.workflow import GeneratorTaskActivity

from .entity import GeneratorTaskConfig, VariantTaskActivity, VariantTaskConfig

L = logging.getLogger(__name__)


class SBOTarget(luigi.Target):
    """BbpWorkflow SBO task luigi target."""

    activity_class = None
    config_class = None

    def __init__(self, task):
        """"""
        self.task = task

    def exists(self):
        """Return ``True`` if :attr:`self.task.config_url` was used by the task."""
        return self.activity is not None

    @property
    def activity(self):
        """Corresponding activity instance that links config and task resource.

        Returns:
            Knowledge graph ``self.task.activity_class`` instance or ``None`` if not found.
        """
        config = self.config_class.from_url(self.task.config_url)

        if self.task.isolated and WORKFLOW:
            was_influenced_by = get_entity(resource_id=WORKFLOW, cls=WorkflowExecution)
        else:
            was_influenced_by = None

        # you have to love nexus
        for _ in range(10):
            activities = self.activity_class.used_config(
                config,
                config.get_rev(),
                was_influenced_by=was_influenced_by,
                base=self.task.kg_base,
                org=self.task.kg_org,
                proj=self.task.kg_proj,
            )
            activity = next(activities, None)
            if activity:
                break
            time.sleep(1)

        if activity is not None:
            L.debug("Activity found: %s", activity.get_id())
        # hopefully it was indexed in 10 sec...
        return activity


class VariantTarget(SBOTarget):
    """Variant target."""

    config_class = VariantTaskConfig
    activity_class = VariantTaskActivity


class GeneratorTarget(SBOTarget):
    """Generator target."""

    config_class = GeneratorTaskConfig
    activity_class = GeneratorTaskActivity
