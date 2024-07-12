# SPDX-License-Identifier: Apache-2.0

"""Util."""

from pathlib import Path
from tempfile import TemporaryDirectory

from entity_management.workflow import BbpWorkflowConfig
from luigi import configuration

from bbp_workflow.settings import L


class ConfigFileManager:
    """Extend dynamically luigi config files."""

    _instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        """"""
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
        return cls._instance

    def __init__(self):
        self._tmp_dir = TemporaryDirectory()  # pylint: disable=consider-using-with

    def add_config(self, task, config):
        """Add extra config file to luigi runtime from `data_download`."""
        L.debug("adding cfg: %s", config.get_url())
        cfg_path = Path(self._tmp_dir.name) / f"{_config_id(config)}.cfg"
        if (
            not cfg_path.exists()
            and config.distribution
            and config.distribution.name.endswith(".cfg")
        ):
            config.distribution.download(cfg_path.parent, cfg_path.name)
            configuration.add_config_path(str(cfg_path))
            L.debug("added cfg: %s", cfg_path)
        for t in task.deps():
            L.debug("dependency: %s", t)
            config_url = getattr(t, "config_url", None)
            if config_url:
                config = BbpWorkflowConfig.from_url(config_url)
                self.add_config(t, config)


def _config_id(config):
    """Config identifier comprised of uuid and rev from config entity."""
    uuid = config.get_id().split("/")[-1]
    rev = config.get_rev()
    return f"{uuid}_{rev}"


def config_id(url):
    """Config identifier comprised of uuid and rev from config url."""
    config = BbpWorkflowConfig.from_url(url)
    return _config_id(config)
