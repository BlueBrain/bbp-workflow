# SPDX-License-Identifier: Apache-2.0

"""Luigi targets."""

from luigi.target import Target


class KgUrlTarget(Target):
    """Knowledge graph luigi target specified by url.

    Args:
        cls (classobj): Knowledge graph entity class from ``entity_management`` lib.
        url (str): Url to the knowledge graph entity.
    """

    def __init__(self, cls, task):
        """Init."""
        self.cls = cls
        self.task = task

    def exists(self):
        """Return ``True`` if :attr:`entity` is not ``None`` and ``False`` otherwise."""
        return self.entity is not None

    @property
    def entity(self):
        """Property which looks up ``cls`` by ``url``.

        Returns:
            Knowledge graph entity instance or ``None`` if not found.
        """
        return self.cls.from_url(
            self.task.url, base=self.task.kg_base, org=self.task.kg_org, proj=self.task.kg_proj
        )
