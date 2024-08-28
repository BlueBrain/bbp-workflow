# SPDX-License-Identifier: Apache-2.0

"""Specific simulation campaign SBO tasks."""

import json
from getpass import getuser
from pathlib import Path
from threading import Thread

import numpy as np
from entity_management.analysis import AnalysisReport
from entity_management.base import Derivation
from entity_management.simulation import Simulation as SimEntity
from entity_management.simulation import (
    SimulationCampaign,
    SimulationCampaignAnalysis,
    SimulationCampaignConfiguration,
    SimulationCampaignExecution,
    SimulationCampaignGeneration,
)
from luigi import (
    BoolParameter,
    ChoiceParameter,
    DictParameter,
    IntParameter,
    ListParameter,
    LocalTarget,
    OptionalDictParameter,
    OptionalParameter,
    Parameter,
    TaskParameter,
    WrapperTask,
)

from bbp_workflow.luigi import RemoteTarget, inherits
from bbp_workflow.sbo.circ.task import FindDetailedCircuit, FindDetailedCircuitMeta
from bbp_workflow.sbo.task import BaseTask, MetaTask
from bbp_workflow.settings import (
    BBP_WORKFLOW_SIF,
    BLUE_CFG,
    DEFAULT_HOST,
    ENVIRONMENT,
    SIM_CFG_JSON,
    WORKFLOW_MODULE,
)
from bbp_workflow.simulation.sim_writer import generate_campaign
from bbp_workflow.simulation.task import (
    GenerateSimulationCampaign,
    SimCampaignMixin,
    _init_rnd,
    _monitoring_file,
    _sim_monitoring,
)
from bbp_workflow.simulation.util import (
    _campaign_sim_indices,
    _indices_to_ranges,
    _sim_indices_to_list,
    campaign_sims,
    read_config,
    xr_to_dict,
)
from bbp_workflow.slurm import _run_sbatch, _sbatch
from bbp_workflow.task import EnvCfg, SlurmCfg, _remote_script
from bbp_workflow.util import amend_cmd, to_str


class GenSimCampaign(GenerateSimulationCampaign, BaseTask):
    """Task that does the job."""

    circuit_config_path = OptionalParameter(default=None)

    circuit_url = OptionalParameter(
        default=None,
        description="Should not be used, just marking it as optional from parent task.",
    )

    def requires(self):
        params = {}
        if self.circuit_config_path is not None:
            params |= {"circuit_config_path": self.circuit_config_path}
        return FindDetailedCircuit(**params)

    def run(self):
        """"""
        rnd = _init_rnd(self.meta_seed)

        circuit_config_path = self.input().path
        config = self.declare_campaign(self.config_id, circuit_config_path, rnd)
        config = generate_campaign(
            config,
            self.coords_filter_func,
            self.param_processors,
            self.max_workers,
            self.set_progress_percentage,
            rnd,
            self.validate_output,
        )

        self._handle_post_processors(config)

    def output(self):
        path_prefix = self.attrs["path_prefix"]  # pylint: disable=unsubscriptable-object
        return LocalTarget(path=f"{path_prefix}/{self.config_id}/config.json")


class GenSimCampaignMeta(MetaTask):
    """Meta task that specifies which task does the job and what nexus resource is registered."""

    activity_class = Parameter(
        default=f"{SimulationCampaignGeneration.__module__}."
        f"{SimulationCampaignGeneration.__name__}"
    )

    def requires(self):
        return FindDetailedCircuitMeta()

    def publish_resource(self, activity, resource):
        circuit = self.input().activity.generated

        task = self.get_base_task_instance()

        blue_config_template = task.attrs["blue_config_template"]
        user_target = task.attrs.get("user_target")
        if user_target:
            user_target = self.from_file(user_target)

        cfg_path = Path(task.output().path)
        config = read_config(json.loads(cfg_path.read_text(encoding="utf-8")))

        resource = SimulationCampaignConfiguration(
            name=task.name,
            description=task.description,
            configuration=self.from_json_str(to_str(xr_to_dict(config))),
            template=self.from_file(blue_config_template),
            target=user_target,
        )
        resource = self.publish(resource, activity=activity)

        activity = activity.evolve(generated=resource, used=circuit)
        activity = self.publish(activity)

        return activity, resource


class RunSimCampaign(SimCampaignMixin, BaseTask):
    """"""

    retry_count = 0

    gen_sim_campaign_params = OptionalDictParameter(default=None)

    sim_config_url = OptionalParameter(default=None)

    simulation_type = TaskParameter(
        description="Specific simulation task class. For example: "
        "CortexNrdmsPySim, ThalamusNrdmsPySim, HippocampusNrdmsPySim."
    )  #:

    benchmark = BoolParameter(
        default=False,
        significant=False,
        parsing=BoolParameter.EXPLICIT_PARSING,
        description="If True, runs last simulation from the campaign and allows to verify if "
        "allocated resources are enough for completion.",
    )

    sim_indices = OptionalParameter(
        default=None, description="Indices of the simulations to run. " "For example: 0-10,20"
    )  #:

    def requires(self):
        params = {}
        if self.gen_sim_campaign_params is not None:
            params |= self.gen_sim_campaign_params
        return GenSimCampaign(**params)

    def _get_cfg(self):
        cfg_path = Path(self.requires().output().path)
        if not cfg_path.exists():
            return cfg_path.parent, None, None
        config = read_config(json.loads(cfg_path.read_text("utf-8")))
        _, path = next(campaign_sims(config))
        is_sonata = (Path(path) / SIM_CFG_JSON).is_file()
        return cfg_path.parent, config, is_sonata

    def run(self):
        """"""
        sim_root, config, is_sonata = self._get_cfg()
        if self.benchmark:
            sim_indices = [_campaign_sim_indices(config)[-1]]
        else:
            if self.sim_indices:
                sim_indices = _sim_indices_to_list(self.sim_indices)
            else:
                sim_indices = _campaign_sim_indices(config)

        sbatch = self._prepare_sbatch(is_sonata, sim_root, sim_indices, None, None, type(self))
        monitoring_thread = None
        with _run_sbatch(DEFAULT_HOST, sbatch, job_script_path=sim_root) as (job_id, ssh_proc):
            try:
                monitoring_thread = Thread(
                    target=_sim_monitoring,
                    args=(ssh_proc, job_id, config, sim_root, self.resource_url),
                )
                monitoring_thread.start()
                monitoring_file = _monitoring_file(sim_root, job_id)
                if monitoring_file is not None:
                    # pylint: disable=not-callable
                    self.set_tracking_url(
                        f"https://bbp-workflow-web-{getuser()}.kcp.bbp.epfl.ch"
                        f"/{monitoring_file.parent.name}/{monitoring_file.name}"
                    )
            finally:
                if monitoring_thread:
                    monitoring_thread.join()

    def output(self):
        """[:class:`RemoteTarget<luigi.contrib.ssh.RemoteTarget>`\\(path = ``cfg.SUCCESS``\\)]."""
        sim_root, config, is_sonata = self._get_cfg()
        if config is None:
            return RemoteTarget(host=DEFAULT_HOST, path=Path(sim_root) / "0" / "None.SUCCESS")
        if is_sonata:
            config_file = SIM_CFG_JSON
        else:
            config_file = BLUE_CFG
        return [
            RemoteTarget(host=DEFAULT_HOST, path=Path(path) / f"{config_file}.SUCCESS")
            for _, path in campaign_sims(config)
        ]


class RunSimCampaignMeta(MetaTask):
    """"""

    retry_count = 0

    activity_class = Parameter(
        default=f"{SimulationCampaignExecution.__module__}."
        f"{SimulationCampaignExecution.__name__}"
    )

    def requires(self):
        return GenSimCampaignMeta()

    def pre_publish_resource(self, activity):
        sim_campaign_cfg = self.input().activity.generated
        config = read_config(sim_campaign_cfg.configuration.as_dict())
        sim_campaign = config.copy(data=np.full(config.shape, fill_value="", dtype=object))
        for i, (coords, sim_path) in enumerate(campaign_sims(config, include_empty=True)):
            sim = self.publish(
                SimEntity(
                    name=str(i),
                    parameter={"coords": coords},
                    status="Pending",
                    config_file=f"{sim_path}/{SIM_CFG_JSON}",
                ),
                activity=activity,
            )
            sim_campaign.loc[coords] = sim.get_url()

        resource = SimulationCampaign(
            name=sim_campaign_cfg.name,
            description=sim_campaign_cfg.description,
            simulations=self.from_json_str(to_str(xr_to_dict(sim_campaign))),
            parameter={
                "coords": {
                    k: [i.item() if hasattr(i, "dtype") else i for i in v.data]
                    for k, v in config.coords.items()
                },  # numpy types to python
                "attrs": config.attrs,
            },
        )
        resource = self.publish(resource, activity=activity)

        activity = activity.evolve(generated=resource, used=sim_campaign_cfg)
        activity = self.publish(activity)

        return activity, resource

    def publish_resource(self, activity, resource):
        assert resource, "Expecting resource to be precreated with pre_publish_resource!"
        return activity, resource


@inherits(SlurmCfg, EnvCfg)
class ReportSimCampaign(BaseTask):
    """"""

    modules = Parameter(default=WORKFLOW_MODULE, description="Environment modules to load.")  #:

    container = Parameter(default=BBP_WORKFLOW_SIF, description="Singularity container to use.")  #:

    exclusive = BoolParameter(
        default=True, significant=False, description="Allocate simulation nodes exclusively."
    )  #:

    mem = Parameter(
        default="0", significant=False, description="Real memory required per node."
    )  #:

    cell_step = IntParameter(default=1, description="Take every CELL_STEPth cell.")  #:

    report_type = Parameter(
        default="spikes", description="Type of the report(spikes or frame report name)."
    )  #:

    report_name = ChoiceParameter(
        choices=["raster", "firing_rate_histogram", "isi", "firing_animation", "trace"],
        default="raster",
        description="Name of the report resource in the knowledge graph",
    )  #:

    node_sets = ListParameter(default=[], description="List of the node sets.")  #:

    report_description = Parameter(
        default="Simulation campaign raster plots",
        description="Description of the report resource.",
    )  #:

    categories = ListParameter(
        default=["Synapse"], description="Categories to which the report belong."
    )  #:

    types = ListParameter(
        default=["Analysis"], description="Types of reports to which the report belongs."
    )  #:

    sim_indices = OptionalParameter(
        default=None, description="Indices of the simulations to make plots." "For example: 0-10,20"
    )  #:

    parallel_jobs = IntParameter(
        default=0,
        significant=False,
        config_path={"section": "DEFAULT", "name": "workers"},
        description="Parallelization level of post-processors. Default value will be taken "
        "from the ``workers`` parameter in the [DEFAULT] section of the cfg file.",
    )  #:

    def requires(self):
        """"""
        return RunSimCampaign()

    @property
    def file_name(self):
        """"""
        return f"{self.report_type}_{self.report_name}.png"

    @property
    def args(self):
        """Args to remote_script."""
        return (
            f"{self.report_type} {self.report_name} "
            f"{json.dumps(json.dumps(self.node_sets))} {self.file_name} {self.cell_step}"
        )

    def remote_script(self):
        """Raster plots."""
        # pylint: disable=import-outside-toplevel,reimported
        from json import loads
        from sys import argv

        from bluepysnap.circuit_ids import CircuitNodeId
        from bluepysnap.simulation import Simulation as Sim

        _, report_type, report_name, node_sets, file_name, cell_step = argv
        node_sets = loads(node_sets)
        cell_step = int(cell_step)

        sim = Sim("simulation_config.json")
        ax = None
        for node_set in node_sets:
            ids = sim.circuit.nodes.ids(sim.node_sets[node_set]).index
            if report_type == "spikes":
                report = getattr(sim, report_type)
            else:
                report = getattr(sim, "reports")[report_type]

            report = report.filter(group=[CircuitNodeId(*node) for node in ids[:: int(cell_step)]])
            report = getattr(report, report_name)
            ax = report(ax=ax)

        ax.figure.savefig(file_name)

    def run(self):
        """"""
        cfg_path = Path(self.requires().requires().output().path)
        config = read_config(json.loads(cfg_path.read_text(encoding="utf-8")))
        path_prefix_name = Path(config.attrs["path_prefix"]) / config.name
        if self.sim_indices:
            sim_indices = self.sim_indices
        else:
            sim_indices = _indices_to_ranges(_campaign_sim_indices(config))
        _, first_sim_path = next(campaign_sims(config))
        first_sim_path = Path(first_sim_path)

        remote_script = _remote_script(self)
        cmd = amend_cmd(self.container, f'python "$REMOTE_SCRIPT" {self.args}')
        cfgs = [
            (
                self.clone(
                    SlurmCfg,
                    job_name="raster-plots",
                    job_array=f"{sim_indices}%{self.parallel_jobs}",
                    job_output=f"{path_prefix_name}/%a/.%A_%a.log",
                ),
                cmd,
            )
        ]
        env = self
        if ENVIRONMENT == "aws":
            env = self.clone(EnvCfg, module_archive="null", modules="intelmpi")
        elif ENVIRONMENT == "bbp":
            env = self.clone(EnvCfg, modules="singularityce")

        sbatch = _sbatch(
            cfgs, env, pre_cmd=f"{remote_script}\ncd {path_prefix_name}/$SLURM_ARRAY_TASK_ID"
        )
        with _run_sbatch(DEFAULT_HOST, sbatch, job_script_path=path_prefix_name):
            pass

    def output(self):
        """"""
        cfg_path = Path(self.requires().requires().output().path)
        if not cfg_path.exists():  # let luigi process upstream tasks to create targets
            return RemoteTarget(host=DEFAULT_HOST, path=str(cfg_path))
        config = read_config(json.loads(cfg_path.read_text(encoding="utf-8")))
        return [
            RemoteTarget(host=DEFAULT_HOST, path=f"{path}/{self.file_name}")
            for _, path in campaign_sims(config)
        ]


class ReportsSimCampaign(BaseTask, WrapperTask):
    """"""

    reports = DictParameter(default={}, description="")  #:

    def get_report_tasks(self):
        """Get report task objects."""
        reports = []
        for report_type, report_names in self.reports.items():
            for report_name, params in report_names.items():
                reports.append(
                    ReportSimCampaign(report_type=report_type, report_name=report_name, **params)
                )
        return reports

    def requires(self):
        """"""
        return self.get_report_tasks()


class ReportsSimCampaignMeta(MetaTask):
    """"""

    activity_class = Parameter(
        default=f"{SimulationCampaignAnalysis.__module__}." f"{SimulationCampaignAnalysis.__name__}"
    )

    def requires(self):
        return RunSimCampaignMeta()

    def publish_resource(self, activity, resource):
        sim_campaign_cfg = self.requires().input().activity.generated
        sim_campaign = self.requires().output().activity.generated
        config = read_config(sim_campaign_cfg.configuration.as_dict())
        sims = read_config(sim_campaign.simulations.as_dict())
        index_to_sim = {i: SimEntity.from_url(url) for i, url in enumerate(sims.data.ravel())}
        for task in self.get_base_task_instance().get_report_tasks():
            for i, (coords, sim_path) in enumerate(campaign_sims(config, include_empty=True)):
                path = Path(sim_path) / task.file_name
                name = "__".join(f"{k}={v}" for k, v in coords.items()) + path.suffix
                distribution = self.from_file(
                    str(path), name=name, content_type=f"image/{path.suffix[1:]}"
                )
                resource = AnalysisReport(
                    name=task.report_name,
                    description=task.report_description,
                    categories=list(task.categories),
                    types=list(task.types),
                    distribution=distribution,
                    derivation=Derivation(entity=index_to_sim[i]),
                )
                self.publish(resource, activity=activity)
        return activity, None
