# SPDX-License-Identifier: Apache-2.0

"""Workflow module."""

import luigi
import luigi.util

from bbp_workflow.generation.generator import get_class_from_config_name
from bbp_workflow.task import KgCfg, RemoteHostCfg


@luigi.util.inherits(KgCfg, RemoteHostCfg)
class SBOWorkflow(luigi.WrapperTask):
    """Generator Workflow class for building detailed circuits.

    Args:
        output_dir (Path): Output directory to write generated data.
        config_url (str): ModelBuildingConfig NEXUS resource id.
        account (str): Project account.
        isolated (bool):
            Whether the workflow will run in isolated mode. Isolated mode ignores activities from
            previous executions.
        target (str): The target generator execute. The workflow will run all tasks up until the
            target task.
    """

    output_dir = luigi.PathParameter(
        significant=True,
        absolute=True,
        description="Output base directory for the workflow.",
    )
    config_url = luigi.Parameter(
        significant=True,
        description="UI recipe to build the workflow from.",
    )
    account = luigi.Parameter(significant=False, description="Slurm account.")

    isolated = luigi.BoolParameter(
        significant=False,
        default=False,
        description="Ignore activities from other executions.",
    )
    target = luigi.Parameter(
        significant=True,
        default="synapseConfig",
    )

    @property
    def kg_config(self) -> dict:
        """Return the KG config as a dictionary."""
        return self.clone(KgCfg).param_kwargs

    @property
    def host_config(self) -> dict:
        """Return the host config as a dictionary."""
        return self.clone(RemoteHostCfg).param_kwargs

    def requires(self):
        """Return required leaf task in the workflow DAG, determined by the `target` argument."""
        generator_class = get_class_from_config_name(self.target)

        return generator_class(
            main_config_url=self.config_url,
            main_output_dir=self.output_dir,
            host_config=self.host_config,
            account=self.account,
            kg_base=self.kg_base,
            kg_proj=self.kg_proj,
            kg_org=self.kg_org,
            isolated=self.isolated,
        )
