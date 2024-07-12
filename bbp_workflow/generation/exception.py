# SPDX-License-Identifier: Apache-2.0

"""Workflow exceptions."""


class WorkflowError(Exception):
    """Workflow exception class."""


class EntityNotFoundError(WorkflowError):
    """Error for not finding an entity."""
