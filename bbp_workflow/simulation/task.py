# SPDX-License-Identifier: Apache-2.0

"""Collection of simulation tasks for the Workflow engine."""

# pylint: disable=too-many-lines
import logging
from datetime import datetime
from os import environ, symlink
from pathlib import Path
from pprint import pprint
from textwrap import dedent
from threading import Thread
from time import sleep

import numpy as np
import pandas as pd
import xarray as xr
from entity_management.core import DataDownload
from entity_management.simulation import DetailedCircuit
from entity_management.simulation import Simulation as SimEntity
from entity_management.simulation import SimulationCampaign as SimCampaignEntity
from entity_management.simulation import (
    SimulationCampaignConfiguration,
    SimulationCampaignGeneration,
)
from luigi import (
    BoolParameter,
    DictParameter,
    IntParameter,
    ListParameter,
    OptionalBoolParameter,
    OptionalIntParameter,
    OptionalParameter,
    Parameter,
    Task,
    TaskParameter,
    WrapperTask,
)

from bbp_workflow.luigi import QuantityDictParameter, RemoteTarget, RunAnywayTarget, inherits
from bbp_workflow.settings import (
    AVAILABLE_NODES,
    BLUE_CFG,
    DEFAULT_HOST,
    ENVIRONMENT,
    NEURODAMUS_HIPPOCAMPUS_MODULE,
    NEURODAMUS_HIPPOCAMPUS_SIF,
    NEURODAMUS_MULTISCALE_MODULE,
    NEURODAMUS_NEOCORTEX_MODULE,
    NEURODAMUS_NEOCORTEX_PLASTICITY_MODULE,
    NEURODAMUS_NEOCORTEX_SIF,
    NEURODAMUS_THALAMUS_MODULE,
    PY_NEURODAMUS_MODULE,
    SIM_CFG_JSON,
    WORKFLOW_MODULE,
    L,
)
from bbp_workflow.slurm import _run_cmd, _run_sbatch, _sbatch
from bbp_workflow.target import KgUrlTarget
from bbp_workflow.task import (
    EnvCfg,
    KgTask,
    LookupKgEntity,
    RemoteHostCfg,
    SbatchTask,
    SlurmCfg,
    _get_job_name,
)
from bbp_workflow.util import amend_cmd, fix_workflows_path, ood_url, to_str, unfreeze

from .sim_writer import generate_campaign
from .util import (
    _campaign_sim_index_to_coords,
    _campaign_sim_indices,
    _indices_to_ranges,
    _sim_indices_to_list,
    campaign_sims,
    read_config,
    xr_from_dict,
    xr_to_dict,
)


def _sim_config_url_param():
    return OptionalParameter(
        default=None,
        description="Optionally provide simulation campaign URL or configure the URL directly "
        "on the LookupSimulationCampaign task.",
    )


def _is_sonata_param():
    return BoolParameter(
        default=False,
        significant=False,
        description='Set to true in order to use "simulation_config.json".',
    )  #:


def _benchmark_param():
    return BoolParameter(
        default=False,
        significant=False,
        parsing=BoolParameter.EXPLICIT_PARSING,
        description="If True, runs last simulation from the campaign and allows to verify if "
        "allocated resources are enough for completion.",
    )


def _enable_shm():
    return OptionalBoolParameter(
        default=None,
        significant=False,
        parsing=BoolParameter.EXPLICIT_PARSING,
        description="Enables the use of /dev/shm for coreneuron_input.",
    )


class LookupDetailedCircuit(LookupKgEntity):
    """Lookup detailed circuit knowledge graph entity."""

    def output(self):
        ":class:`KgUrlTarget<bbp_workflow.target.KgUrlTarget>`\\( \
        :class:`DetailedCircuit<entity_management.simulation.DetailedCircuit>`, \
        ``self.url``\\)."
        return KgUrlTarget(DetailedCircuit, self)


class LookupSimulationCampaign(LookupKgEntity):
    """Lookup simulation campaign configuration Knowledge Graph entity."""

    def output(self):
        ":class:`KgUrlTarget<bbp_workflow.target.KgUrlTarget>`\\( \
        :class:`SimCampaignConf<entity_management.simulation.SimulationCampaignConfiguration>`, \
        ``self.url``\\)."
        return KgUrlTarget(SimulationCampaignConfiguration, self)


def _lookup_sim_campaign(sim_config_url):
    if sim_config_url:
        return LookupSimulationCampaign(url=sim_config_url)
    else:
        return LookupSimulationCampaign()


class LookupSimCampaign(LookupSimulationCampaign):
    """Alias for :class:`LookupSimulationCampaign`."""


class RegisterSimulationCampaign(KgTask):
    """Register an existing simulation campaign.

    Args:
        folder_name (luigi.Parameter): Folder in the path_prefix where the simulation campaign
            files are located.
        paths (luigi.ListParameter): Nested list of paths corresponding to the dimensions in
            ``self.coords``.
        coords (luigi.DictParameter): Coordinates of the scanned values.
        attrs (luigi.DictParameter): Additional static attributes of the campaign.
        name (luigi.OptionalParameter): Name of the simulation campaign.
        description (luigi.OptionalParameter): Description of the simulation campaign.
    """

    folder_name = Parameter()
    paths = ListParameter()
    coords = DictParameter()
    attrs = DictParameter(default=None)
    name = OptionalParameter(default=None)
    description = OptionalParameter(default=None)

    def declare_campaign(self):
        """"""
        return xr.DataArray(
            data=unfreeze(self.paths),
            name=self.folder_name,
            dims=list(self.coords.keys()),
            coords=unfreeze(self.coords),
            attrs=unfreeze(self.attrs),
        )

    def run(self):
        """"""
        config = self.declare_campaign()
        sim_campaign_cfg = SimulationCampaignConfiguration(
            name=self.name,
            description=self.description,
            configuration=DataDownload.from_json_str(to_str(xr_to_dict(config))),
            template=DataDownload.from_json_str(""),
        )
        sim_campaign_cfg = self.publish(sim_campaign_cfg)
        self.done("Simulation campaign:", sim_campaign_cfg)


class RegisterCoupledCoordsSimulationCampaign(RegisterSimulationCampaign):
    """Register an existing simulation campaign with coupled coords."""

    def declare_campaign(self):
        """"""
        dim_lengths = [len(v) for v in self.coords.values()]
        assert dim_lengths.count(dim_lengths[0]) == len(dim_lengths), (
            "Dimension lengths " "must be equal!"
        )
        coupled = pd.MultiIndex.from_arrays(
            list(self.coords.values()), names=tuple(self.coords.keys())
        )
        return xr.DataArray(
            data=unfreeze(self.paths),
            name=self.folder_name,
            dims="coupled",
            coords={"coupled": coupled},
            attrs=unfreeze(self.attrs),
        )


def _amend_coords(coords, attrs, rnd, *, low, high, size):
    """Update coords with seed dimension."""
    assert "seed" not in attrs, (
        'Error adding "seed" to "coords", ' '"seed" already present in "attrs"!'
    )
    assert "seed" not in coords, (
        'Error adding "seed" to "coords", ' '"seed" already present in "coords"!'
    )
    coords.update({"seed": (low + rnd.choice(high - low + 1, size, replace=False)).tolist()})


def _init_rnd(seed):
    """Initialize random number generator."""
    if seed is not None:
        return np.random.default_rng(seed=seed)
    else:
        return np.random.default_rng()


@inherits(SlurmCfg, EnvCfg, RemoteHostCfg)
class GenerateSimulationCampaign(KgTask):
    r"""Generate simulation campaign.

    Produce simulation configuration files in unique folders at ``path_prefix`` gpfs location.
    Simulation configuration files are generated by replacing any ``blue_config_template``
    placeholder with values from ``coords``, ``attrs`` or any additional mapping that
    ``param-processors`` return. The amount of simulation configurations is determined by the
    Cartesian product of ``coords`` values.

    As a result
    :class:`configuration<entity_management.simulation.SimulationCampaignConfiguration>`
    entity will be created in the Knowledge Graph with
    :class:`generation<entity_management.simulation.SimulationCampaignGeneration>` activity
    linking to the detailed circuit.

    **Usage**:

    .. code-block:: cfg
       :caption: simulation.cfg

        [GenerateSimulationCampaign]
        coords: {"ca": [1.3, 1.35],
                 "stim_number_mcs": [40, 60]}
        attrs: {"path_prefix": "/gpfs/bbp.cscs.ch/path/where/to/generate",
                "blue_config_template": "BlueConfig.tmpl",
                "user_target": "user.target",
                "mg": 1.0,
                "depolarization": 100.0,
                "target": "MiniColumn_0",
                "duration": 2500,
                "seed": 123456}
        coords_filter_func: my_module.filter_function
        param-processors: ["bbp_workflow.sci.default_depolarization_treatment",
                           "bbp_workflow.sci.default_ca_treatment"]

    .. code-block:: bash

        bbp-workflow launch --follow --config simulation.cfg \
            bbp_workflow.simulation GenerateSimulationCampaign \
                circuit-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/circuit-id
    """

    modules = Parameter(
        default=WORKFLOW_MODULE,
        significant=False,
        description="Environment modules to load for post-processors.",
    )  #:

    circuit_url = Parameter(description="Detailed circuit url to use.")  #:
    coords = QuantityDictParameter(
        default={},  # allow empty in case seed_as_coord is set
        description="Coordinates of the scanned values. Generated scan will contain Cartesian "
        "product of the coords values across all the dimensions.",
    )  #:
    attrs = QuantityDictParameter(
        description="Additional static attributes of the campaign.  Required to have at least "
        "`path_prefix` and `blue_config_template`. Optionally can contain "
        "`user_target` file path which will be copied to the simulation folders and "
        "`user_target_file_name` placeholder will contain the destination path "
        "which can be used in the template. Optional `rnd_seed_range` list with "
        "two arguments to numpy randint function will generate seed value for the "
        "campaign and make `seed` placeholder available in the template."
    )  #:
    coords_filter_func = OptionalParameter(
        default=None, description="Filter function to select simulations."
    )  #:
    param_processors = ListParameter(
        default=[], description="List of parameter processing functions."
    )  #:
    post_processors = ListParameter(
        default=[], description="List of post-processing functions."
    )  #:
    name = OptionalParameter(default=None, description="Name of the simulation campaign.")  #:
    description = OptionalParameter(
        default=None, description="Description of the simulation campaign."
    )  #:
    meta_seed = OptionalIntParameter(
        default=None, description="Seed for the generator of `seed` placeholder random values."
    )  #:
    seed_as_coord = DictParameter(
        default={},
        description='Provide dictionary(e.g.{"low":0,"high":10,"size":4}) '
        "in order to update `coords` with additional `seed` dimension "
        "of specified `size` and unique random values from `low`~`high` range.",
    )  #:
    max_workers = IntParameter(
        default=0,
        significant=False,
        config_path={"section": "DEFAULT", "name": "workers"},
        description="Parallelization level of campaign generation. Default value will be taken "
        "from the ``workers`` parameter in the [DEFAULT] section of the cfg file.",
    )  #:
    parallel_jobs = IntParameter(
        default=0,
        significant=False,
        config_path={"section": "DEFAULT", "name": "workers"},
        description="Parallelization level of post-processors. Default value will be taken "
        "from the ``workers`` parameter in the [DEFAULT] section of the cfg file.",
    )  #:
    validate_output = OptionalBoolParameter(
        default=True,
        significant=False,
        description="Set to False if you do not want output file to be validate with "
        "bluepy-configfile.",
    )  #:

    def requires(self):
        """:class:`LookupDetailedCircuit<bbp_workflow.circuit.LookupDetailedCircuit>` \
        (``url=self.circuit_url``)."""
        return LookupDetailedCircuit(url=self.circuit_url)

    def declare_campaign(self, folder_name, circuit_config, rnd):
        """"""
        coords = unfreeze(self.coords)
        attrs = unfreeze(self.attrs)
        if self.seed_as_coord:
            _amend_coords(coords, attrs, rnd, **self.seed_as_coord)  # pylint: disable=not-a-mapping
        config = xr.DataArray(
            name=folder_name, dims=list(coords.keys()), coords=coords, attrs=attrs
        )
        config.attrs["circuit_config"] = circuit_config
        config = config.copy(data=np.full(config.shape, fill_value="", dtype=object))
        return config

    def run(self):
        """"""
        # pylint: disable=unsubscriptable-object
        blue_config_template = fix_workflows_path(self.attrs["blue_config_template"])
        user_target = fix_workflows_path(self.attrs.get("user_target"))
        if user_target:
            user_target = self.from_file(user_target)
        rnd = _init_rnd(self.meta_seed)

        sim_campaign_cfg = SimulationCampaignConfiguration(
            configuration=self.from_json_str(""), template=self.from_json_str("")
        )
        sim_campaign_cfg = self.publish(sim_campaign_cfg)
        folder_name = sim_campaign_cfg.get_id().split("/")[-1]

        circuit = self.input().entity
        if circuit.circuitConfigPath:
            circuit_config = Path(circuit.circuitConfigPath.url.replace("file://", ""))
        else:  # fallback to old metadata
            assert circuit.circuitBase, (
                "DetailedCircuit not found! "
                'Please make sure "circuit-url" parameter points to existing DetailedCircuit.'
            )
            circuit_config = Path(circuit.circuitBase.url.replace("file://", "")) / "CircuitConfig"
        config = self.declare_campaign(folder_name, str(circuit_config), rnd)
        config = generate_campaign(
            config,
            self.coords_filter_func,
            self.param_processors,
            self.max_workers,
            self.set_progress_percentage,
            rnd,
            self.validate_output,
        )

        activity = SimulationCampaignGeneration(generated=sim_campaign_cfg, used=circuit)
        activity = self.publish(activity)

        sim_campaign_cfg = self.publish(
            sim_campaign_cfg.evolve(
                name=self.name,
                description=self.description,
                configuration=self.from_json_str(to_str(xr_to_dict(config))),
                template=self.from_file(blue_config_template),
                target=user_target,
            )
        )
        sim_campaign_cfg = self.publish(sim_campaign_cfg, activity=activity)  # FIXME remove?

        self._handle_post_processors(config)

        self.done("Simulation campaign:", sim_campaign_cfg)

        return sim_campaign_cfg

    def _handle_post_processors(self, config):
        if self.post_processors:
            sim_indices = _indices_to_ranges(_campaign_sim_indices(config))
            path_prefix_name = Path(config.attrs["path_prefix"]) / config.name
            cmd = (
                f'python "$POST_PROCESSOR" {path_prefix_name / "config.json"} $SLURM_ARRAY_TASK_ID'
            )
            pre_cmd = dedent(
                f"""\
                cd {path_prefix_name}/$SLURM_ARRAY_TASK_ID
                export POST_PROCESSOR="$(mktemp -p $HOME)"
                trap 'rm -f -- "$POST_PROCESSOR"' INT TERM HUP EXIT
                cat << 'EOF_CODE' > "$POST_PROCESSOR"

                from sys import argv
                from json import loads
                from pathlib import Path
                from bbp_workflow.simulation.util import read_config
                from bbp_workflow.util import import_func

                _, cfg_path, idx = argv
                config = read_config(loads(Path(cfg_path).read_text()))
                series = config.to_series()
                idx = series.index[int(idx)]
                params = dict(zip(series.index.names, idx if isinstance(idx, tuple) else (idx,)))
                params |= config.attrs
                for func in {self.post_processors}:
                    import_func(func)(**params)
                EOF_CODE
                """
            )
            cfgs = [
                (
                    self.clone(
                        SlurmCfg,
                        job_name=f"post-{_get_job_name(self)}",
                        job_array=f"{sim_indices}%{self.parallel_jobs}",
                        job_output=f"{path_prefix_name}/%a/.%A_%a.log",
                    ),
                    cmd,
                )
            ]
            sbatch = _sbatch(cfgs, self, pre_cmd=pre_cmd)
            with _run_sbatch(self.host, sbatch, job_script_path=path_prefix_name):
                pass


class GenerateCoupledCoordsSimulationCampaign(GenerateSimulationCampaign):
    """Generate simulation campaign with coupled coords values."""

    coords = DictParameter(
        default={},  # allow empty in case seed_as_coord is set
        description="Coordinates of the scanned values. Generated scan will contain zip'ed "
        "tuples of the coords values across all the dimensions. Make sure coords "
        "are of equal length across all the dimensions.",
    )  #:

    def declare_campaign(self, folder_name, circuit_config, rnd):
        """"""
        coords = unfreeze(self.coords)
        attrs = unfreeze(self.attrs)
        if self.seed_as_coord:
            _amend_coords(coords, attrs, rnd, **self.seed_as_coord)  # pylint: disable=not-a-mapping
        dim_lengths = [len(v) for v in coords.values()]
        assert dim_lengths.count(dim_lengths[0]) == len(dim_lengths), (
            "Dimension lengths " "must be equal!"
        )
        coupled = pd.MultiIndex.from_arrays(list(coords.values()), names=tuple(coords.keys()))
        config = xr.DataArray(
            [None] * coupled.size,
            name=folder_name,
            dims="coupled",
            coords={"coupled": coupled},
            attrs=attrs,
        )
        config.attrs["circuit_config"] = circuit_config
        config = config.copy(data=np.full(config.shape, fill_value="", dtype=object))
        return config


class AmendSimulationCampaign(KgTask):
    r"""Generate additional simulations in the campaign with extended filter function.

    **Usage**:

    .. code-block:: cfg
       :caption: simulation.cfg

        [DEFAULT]
        module-archive: archive/2022-02

        [GenerateSimulationCampaign]
        coords: {"ca": [1.3, 1.35],
                 "stim_number_mcs": [40, 60]}
        attrs: {"path_prefix": "/gpfs/bbp.cscs.ch/path/where/to/generate",
                "blue_config_template": "BlueConfig.tmpl",
                "user_target": "user.target",
                "mg": 1.0,
                "depolarization": 100.0,
                "target": "MiniColumn_0",
                "duration": 2500,
                "seed": 123456}
        meta-seed: 123
        coords_filter_func: my_module.filter_function
        param-processors: ["bbp_workflow.sci.default_depolarization_treatment",
                           "bbp_workflow.sci.default_ca_treatment"]

        [AmendSimulationCampaign]
        coords_filter_func: my_module.filter_function_ext

    .. code-block:: bash

        bbp-workflow launch --follow --config simulation.cfg \
            bbp_workflow.simulation AmendSimulationCampaign \
                sim-config-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/sim-config
    """

    sim_config_url = _sim_config_url_param()  #:
    coords_filter_func = OptionalParameter(
        default=None, description="Filter function to select simulations."
    )  #:

    def requires(self):
        """:class:`LookupSimulationCampaign`."""
        return _lookup_sim_campaign(self.sim_config_url)

    def run(self):
        """"""
        sim_campaign_cfg = self.input().entity
        config = xr_from_dict(sim_campaign_cfg.configuration.as_dict())
        generate_task = GenerateSimulationCampaign()

        config = generate_campaign(
            config,
            self.coords_filter_func,
            generate_task.param_processors,
            generate_task.max_workers,
            self.set_progress_percentage,
            _init_rnd(generate_task.meta_seed),
            generate_task.validate_output,
        )

        sim_campaign_cfg = sim_campaign_cfg.evolve(
            configuration=self.from_json_str(to_str(xr_to_dict(config)))
        )
        sim_campaign_cfg = self.publish(sim_campaign_cfg, activity=sim_campaign_cfg.wasGeneratedBy)
        self.done("Simulation campaign:", sim_campaign_cfg)


class NrdmsPySim(SbatchTask):
    """Base neurodamus-py simulation task.

    Run simulation defined by the BlueConfig located in the ``self.chdir`` folder.
    For specific circuit simulation please use :class:`ThalamusNrdmsPySim`,
    :class:`CortexNrdmsPySim` or :class:`HippocampusNrdmsPySim`.
    """

    exclusive = BoolParameter(
        default=True,
        significant=False,
        description="Allocate nodes exclusively and do not share them with other jobs.",
    )  #:
    mem = Parameter(
        default="0", significant=False, description="Real memory required per node."
    )  #:
    cpus_per_task = IntParameter(
        default=2, significant=False, description="Amount of cpus allocated per task."
    )  #:
    model_building_steps = IntParameter(
        default=0, description="Model building steps. Default value of 0 means: not used."
    )  #:
    enable_shm = _enable_shm()  #:
    is_sonata = _is_sonata_param()  #:

    container = OptionalParameter(default=None, description="Singularity container to use.")  #:

    command = OptionalParameter(
        default=None, description="The command `sbatch` will schedule for execution."
    )  #:

    def run(self):
        """"""
        super().run()
        if self.chdir:
            # requested by Christoph
            L.info('Setting group write permissions for: "%s"', self.chdir)
            self.output().fs.remote_context.check_output(["chmod", "g+w", self.chdir])

    def output(self):
        """:class:`RemoteTarget<luigi.contrib.ssh.RemoteTarget>`\\(path =\
        ``{self.chdir}/BlueConfig.SUCCESS``\\) or\
        :class:`RemoteTarget<luigi.contrib.ssh.RemoteTarget>`\\(path =\
        ``{self.chdir}/simulation_config.json.SUCCESS``\\)."""
        if self.chdir:
            output_dir = Path(self.chdir)
        else:
            output_dir = Path(".")
        if self.is_sonata:
            output = output_dir / f"{SIM_CFG_JSON}.SUCCESS"
        else:
            output = output_dir / f"{BLUE_CFG}.SUCCESS"
        return RemoteTarget(host=self.host, path=str(output))


class CortexNrdmsPySim(NrdmsPySim):
    r"""Cortex simulation task.

    Run simulation defined by the simulation config located in the ``self.chdir`` folder.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation CortexNrdmsPySim \
                account=proj123 \
                chdir=/gpfs/bbp.cscs.ch/path/to/blue/config/folder
    """

    modules = Parameter(
        default=f"{NEURODAMUS_NEOCORTEX_MODULE} {PY_NEURODAMUS_MODULE}",
        description="Environment modules to load.",
    )  #:

    container = Parameter(
        default=NEURODAMUS_NEOCORTEX_SIF, description="Singularity container to use."
    )  #:


class PlasticityCortexNrdmsPySim(CortexNrdmsPySim):
    r"""Cortex plasticity simulation task.

    Run simulation defined by the simulation config located in the ``self.chdir`` folder.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation PlasticityCortexNrdmsPySim \
                account=proj123 \
                chdir=/gpfs/bbp.cscs.ch/path/to/blue/config/folder
    """

    modules = Parameter(
        default=f"{NEURODAMUS_NEOCORTEX_PLASTICITY_MODULE} {PY_NEURODAMUS_MODULE}",
        description="Environment modules to load.",
    )  #:


class MultiscaleNrdmsPySim(NrdmsPySim):
    r"""Multiscale simulation task.

    Run simulation defined by the simulation config located in the ``self.chdir`` folder.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation MultiscaleNrdmsPySim \
                account=proj123 \
                chdir=/gpfs/bbp.cscs.ch/path/to/blue/config/folder
    """

    modules = Parameter(
        default=NEURODAMUS_MULTISCALE_MODULE, description="Environment modules to load."
    )  #:

    cpus_per_task = IntParameter(
        default=0, significant=False, description="Amount of cpus allocated per task."
    )  #:

    ntasks_per_node = IntParameter(
        default=32,
        significant=False,
        description="Number of tasks to launch on each allocated node.",
    )  #:

    command = Parameter(
        default="multiscale-run compute",
        description="The command `sbatch` will schedule for execution.",
    )  #:


class ThalamusNrdmsPySim(NrdmsPySim):
    r"""Thalamus simulation task.

    Run simulation defined by the simulation config located in the ``self.chdir`` folder.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation ThalamusNrdmsPySim \
                account=proj123 \
                chdir=/gpfs/bbp.cscs.ch/path/to/blue/config/folder
    """

    modules = Parameter(
        default=f"{NEURODAMUS_THALAMUS_MODULE} {PY_NEURODAMUS_MODULE}",
        description="Environment modules to load.",
    )  #:


class HippocampusNrdmsPySim(NrdmsPySim):
    r"""Hippocampus simulation task.

    Run simulation defined by the simulation config located in the ``self.chdir`` folder.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation HippocampusNrdmsPySim \
                account=proj123 \
                chdir=/gpfs/bbp.cscs.ch/path/to/blue/config/folder
    """

    modules = Parameter(
        default=f"{NEURODAMUS_HIPPOCAMPUS_MODULE} {PY_NEURODAMUS_MODULE}",
        description="Environment modules to load.",
    )  #:

    container = Parameter(
        default=NEURODAMUS_HIPPOCAMPUS_SIF, description="Singularity container to use."
    )  #:


def _create_symlinks(base_path, config):
    """Create human-readable symlinks for sim campaign."""
    series = config.to_series()
    idx_names = series.index.names
    for idx, data in series.items():
        if not isinstance(idx, tuple):
            idx = (idx,)  # if index is atomic(just a number) make tuple out of it
        data = Path(data)
        path_suffix = Path()
        for i, name in enumerate(idx_names):
            path_suffix /= f"{name}={idx[i]}"
        path = base_path / path_suffix
        path.parent.mkdir(parents=True, exist_ok=True)
        if data.is_absolute():
            symlink(f"{data}", path)
        else:
            symlink(f'{config.attrs["path_prefix"]}/{data}', path)


class SimCampaignInfo(Task):
    r"""Print simulation campaign configuration.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation SimCampaignInfo \
                sim-config-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/sim-cfg-id
    """

    sim_config_url = _sim_config_url_param()  #:
    generate_symlinks = BoolParameter(
        default=False,
        description="Provide this param in order to generate human-readable folder structure "
        "with symlinks for the simulation campaign.",
    )  #:

    def requires(self):
        """:class:`LookupSimulationCampaign`."""
        return _lookup_sim_campaign(self.sim_config_url)

    def run(self):
        """"""
        sim_campaign_cfg = self.input().entity
        config = xr_from_dict(sim_campaign_cfg.configuration.as_dict())
        print("SIM CAMPAIGN CONFIG>>>>>", flush=True)
        path_prefix = Path(config.attrs["path_prefix"])
        print(f"PATH: {path_prefix / config.name}", flush=True)
        pprint(config)
        print("<<<<<", flush=True)
        if self.generate_symlinks:
            path = path_prefix / sim_campaign_cfg.name
            path.mkdir(exist_ok=True)
            src_path = path_prefix / config.name
            symlink(src_path / "config.json", path / "config.json")
            _create_symlinks(path, config)
        # pylint: disable=not-callable
        self.set_tracking_url(ood_url(path_prefix / config.name / "config.json"))
        self.output().done()

    def output(self):
        """"""
        return RunAnywayTarget(self)


def _is_sonata(sim_campaign_cfg):
    circuit = sim_campaign_cfg.wasGeneratedBy.used
    if circuit:
        circuit_config_path = Path(circuit.circuitConfigPath.url)
        return circuit_config_path.suffix.lower() == ".json"
    return False


@inherits(SlurmCfg)
class SimulationCampaign(WrapperTask):
    r"""Run simulation campaign.

    Inherits params from :class:`SlurmCfg<bbp_workflow.task.SlurmCfg>` which are going to be
    propagated to the individual simulations of the campaign.

    **Usage**:

    .. code-block:: cfg
       :caption: simulation.cfg

        [DEFAULT]
        kg-proj: project
        account: proj123

        [SimulationCampaign]
        nodes: 10
        time: 3:00:00
        model-building-steps: 2
        simulation-type: CortexNrdmsPySim

    .. code-block:: bash

        bbp-workflow launch --follow --config simulation.cfg \
            bbp_workflow.simulation SimulationCampaign \
                sim-config-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/sim-cfg-id
    """

    retry_count = 0

    benchmark = _benchmark_param()  #:
    sim_config_url = _sim_config_url_param()  #:
    model_building_steps = IntParameter(
        default=0,
        description="Optionally set the number of ModelBuildingSteps for the CoreNeuron "
        "simulation.",
    )  #:
    simulation_type = TaskParameter(
        description="Specific simulation task class. For example: "
        "CortexNrdmsPySim, ThalamusNrdmsPySim, HippocampusNrdmsPySim."
    )  #:
    exclusive = BoolParameter(
        default=True, significant=False, description="Allocate simulation nodes exclusively."
    )  #:
    mem = Parameter(
        default="0", significant=False, description="Real memory required per node."
    )  #:
    cpus_per_task = IntParameter(
        default=2, significant=False, description="Amount of cpus allocated per task."
    )  #:
    # FIXME remove once migrated to SimCampaign
    parallel_jobs = IntParameter(
        default=0, significant=False, config_path={"section": "DEFAULT", "name": "workers"}
    )
    enable_shm = _enable_shm()  #:

    def requires(self):
        """List of ``self.simulation_type``."""
        sim_campaign_cfg = _lookup_sim_campaign(self.sim_config_url).output().entity
        is_sonata = _is_sonata(sim_campaign_cfg)
        config = xr_from_dict(sim_campaign_cfg.configuration.as_dict())
        if self.benchmark:
            return self.clone(
                self.simulation_type,
                chdir=[sim_path for _, sim_path in campaign_sims(config)][-1],
                is_sonata=is_sonata,
            )
        else:
            return [
                self.clone(self.simulation_type, chdir=sim_path, is_sonata=is_sonata)
                for _, sim_path in campaign_sims(config)
            ]


def _sim_task_output_path(is_sonata):
    sim_campaign_task = SimulationCampaign()
    sim_task = sim_campaign_task.clone(sim_campaign_task.simulation_type, is_sonata=is_sonata)
    return Path(sim_task.output().path)


def _monitoring_file(path_prefix_name, job_id):
    base = Path("/var/www/html")
    if not base.is_dir() or environ.get("SLURM_JOB_ID"):
        return None  # no status if no folder or running not in the cloud
    uuid = path_prefix_name.name
    monitoring_file = base / uuid / f"{job_id}.html"
    monitoring_file.parent.mkdir(exist_ok=True)
    return monitoring_file


def _process(out):
    pending = None  # for ArrayTaskId=140-398%10
    status = {}
    for line in out.splitlines():
        props = {
            key_value.split("=")[0]: key_value.split("=")[1]
            for key_value in line.split()
            if "=" in key_value
        }
        index = props["ArrayTaskId"]
        if index.isnumeric():
            status[int(index)] = props
        else:
            pending = index
    return pending, status


def _is_final_state(state):
    return state in ["Done", "Failed"]


def _map_slurm_state(state, exit_code):
    match state:
        case "RUNNING":
            return "Running"
        case "PENDING":
            return "Pending"
        case "COMPLETED":
            if exit_code == "0:0":
                return "Done"
            else:
                return "Failed"


def _merge(sim_props, new_sim_props, index_to_sim_entity):
    update_state = True
    for idx, p in new_sim_props.items():
        if idx in sim_props:
            old_state = sim_props[idx]["JobState"]
            if p["JobState"] == old_state:
                update_state = False
        if update_state and index_to_sim_entity:
            sim = index_to_sim_entity[idx]
            sim = sim.evolve(
                status=_map_slurm_state(p["JobState"], p["ExitCode"]),
                log_url=ood_url(Path(p["StdOut"])),
            )
            if sim.status == "Running" and sim.startedAtTime is None:
                sim = sim.evolve(startedAtTime=datetime.utcnow())
            if _is_final_state(sim.status):
                sim = sim.evolve(endedAtTime=datetime.utcnow())
            index_to_sim_entity[idx] = sim.publish()
        sim_props[idx] = p


def _build_page(file, job_id, path, config, pending, sim_props):
    if file is None:
        return
    index_names = config.to_series().index.names
    index_to_coords = _campaign_sim_index_to_coords(config)
    job_script = path / f".{job_id}.script"
    page = f'<p><a href="{ood_url(job_script)}">.{job_id}.script</a><br></p>'
    page += "<p><table><tr><th>Log</th><th>JobState</th><th>ExitCode</th>"
    for coord_name in index_names:
        page += f"<th>{coord_name}</th>"
    page += "</tr>"
    if pending is not None:
        page += f"<tr><td>{pending}</td></tr>"
    for idx, props in sorted(sim_props.items(), reverse=True):
        page += f'<td><a href="{ood_url(Path(props["StdOut"]))}">{idx}</a></td>'
        page += f'<td>{props["JobState"]}</td>'
        page += f'<td>{props["ExitCode"]}</td>'
        for coord_name in index_names:
            page += f"<td>{index_to_coords[int(idx)][coord_name]}</td>"
        page += "</tr>"
    page += "</table></p>"
    file.write_text(page)


def _sim_monitoring(ssh_proc, job_id, config, path_prefix_name, resource_url=None):
    sim_props = {}  # scontrol report shrinks, so keep info about all sim indices
    file = _monitoring_file(path_prefix_name, job_id)
    if file is not None:
        file.parent.mkdir(exist_ok=True)
    elif resource_url is None:
        return  # no monitoring file or resource to update
    if resource_url is not None:
        sim_campaign = SimCampaignEntity.from_url(resource_url)
        sims = xr_from_dict(sim_campaign.simulations.as_dict())
        index_to_sim = {i: SimEntity.from_url(url) for i, url in enumerate(sims.data.ravel())}
    else:
        index_to_sim = None
    while True:
        try:
            out = _run_cmd(DEFAULT_HOST, f"scontrol -o show job {job_id}", capture_output=True)
        except RuntimeError:
            return  # ignore scontrol errors
        pending, new_sim_props = _process(out)
        _merge(sim_props, new_sim_props, index_to_sim)
        _build_page(file, job_id, path_prefix_name, config, pending, sim_props)
        if ssh_proc.poll() is not None:
            return
        sleep(120)


class SimCampaignMixin:
    """Shared helper methods for old and new sim campaign tasks."""

    parallel_jobs = IntParameter(
        default=8, significant=False, description="Amount of simulations running in parallel."
    )  #:

    def _task_instances(self, is_sonata, sim_campaign_task_type):
        sim_campaign_task = sim_campaign_task_type()
        if ENVIRONMENT is None:
            sim_task = sim_campaign_task.clone(
                sim_campaign_task.simulation_type, is_sonata=is_sonata
            )
        elif ENVIRONMENT == "aws":
            sim_task = sim_campaign_task.clone(
                sim_campaign_task.simulation_type,
                is_sonata=is_sonata,
                module_archive="null",
                modules="intelmpi",
            )
        elif ENVIRONMENT == "bbp":
            sim_task = sim_campaign_task.clone(
                sim_campaign_task.simulation_type, is_sonata=is_sonata, modules="singularityce"
            )
        else:
            raise ValueError(f"Unknown environment {ENVIRONMENT}")

        return sim_campaign_task, sim_task

    def _prepare_sbatch(
        self,
        is_sonata,
        path_prefix_name,
        sim_indices,
        enable_shm=None,
        config_file=BLUE_CFG,
        sim_campaign_task_type=SimulationCampaign,
    ):
        if is_sonata:
            config_file = SIM_CFG_JSON

        sim_indices = _indices_to_ranges(sim_indices)

        sim_campaign_task, sim_task = self._task_instances(is_sonata, sim_campaign_task_type)

        parallel_jobs = self.parallel_jobs
        nodes = sim_task.nodes
        assert parallel_jobs * nodes <= AVAILABLE_NODES, (
            "Please decrease `parallel-jobs` parameter, so that "
            f"`{parallel_jobs=}`*`{nodes=}`<=`{AVAILABLE_NODES=}` !"
        )

        if sim_task.command is None:
            cmd = "special -mpi -python $NEURODAMUS_PYTHON/init.py "
            cmd += f"--configFile={config_file} "
            if enable_shm is True or sim_task.enable_shm is True:
                cmd += "--enable-shm=ON"
            elif enable_shm is False or sim_task.enable_shm is False:
                cmd += "--enable-shm=OFF"
            if sim_task.model_building_steps:
                cmd += f"--modelbuilding-steps={sim_task.model_building_steps} "
        else:
            # used for multiscale run
            cmd = sim_task.command

        cmd = amend_cmd(sim_task.container, cmd, f"dplace {cmd}")

        # FIXME For the c7a.* nodes there is no multithreading enabled
        if ENVIRONMENT in {"aws", "bbp"} and sim_task.constraint.startswith("c7a."):
            sim_task = sim_task.clone(sim_campaign_task.simulation_type, cpus_per_task=1)
            cmd = f"--ntasks=$(($(($SLURM_CPUS_ON_NODE*$SLURM_JOB_NUM_NODES))/2)) {cmd}"

        cfgs = [
            (
                sim_task.clone(
                    SlurmCfg,
                    job_name=_get_job_name(sim_task),
                    job_array=f"{sim_indices}%{self.parallel_jobs}",
                    job_output=f"{path_prefix_name}/%a/.%A_%a.log",
                ),
                cmd,
            )
        ]
        cmd = f"cd {path_prefix_name}/$SLURM_ARRAY_TASK_ID\n"
        cmd += f"test -f {config_file}.SUCCESS && "
        cmd += '{ echo "Simulation output found, exiting."; exit; }\n'
        cmd += "FOLDER_JOB_IDS=\"$(find . -maxdepth 1 -type f -name '.*.log' -exec "
        cmd += "bash -c 'echo ${1//[^_0-9]/}' _ {} \\;)\"\n"
        if L.getEffectiveLevel() == logging.DEBUG:
            cmd += 'echo FOLDER_JOB_IDS="$FOLDER_JOB_IDS"\n'
        cmd += "CURRENT_JOB_ID=${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}\n"
        if L.getEffectiveLevel() == logging.DEBUG:
            cmd += "echo CURRENT_JOB_ID=$CURRENT_JOB_ID\n"
        cmd += 'JOB_IDS_TO_TEST=$((echo "$FOLDER_JOB_IDS"; echo $CURRENT_JOB_ID) | sort | uniq -u '
        cmd += "| paste -d, -s)\n"
        if L.getEffectiveLevel() == logging.DEBUG:
            cmd += "echo JOB_IDS_TO_TEST=$JOB_IDS_TO_TEST\n"
        cmd += 'RUNNING_JOB_IDS=$(test -n "$JOB_IDS_TO_TEST" && squeue --jobs=$JOB_IDS_TO_TEST '
        cmd += "--noheader --states=RUNNING,COMPLETING --format=%A | paste -d, -s)\n"
        cmd += 'test -n "$RUNNING_JOB_IDS" && '
        cmd += '{ echo "Running jobs($RUNNING_JOB_IDS) detected, exiting"; exit; }'
        return _sbatch(cfgs, sim_task, pre_cmd=cmd)


class SimCampaign(SimCampaignMixin, Task):
    r"""Run simulation campaign.

    Example below for large amount(100) of small simulations(10 processes per sim).

    **Usage**:

    .. code-block:: cfg
       :caption: simulation.cfg

        [DEFAULT]
        kg-proj: project
        account: proj123

        [SimulationCampaign]
        exclusive: False
        mem:
        cpus-per-task: 0
        ntasks: 10
        time: 30:00
        simulation-type: CortexNrdmsPySim

    .. code-block:: bash

        bbp-workflow launch --follow --config simulation.cfg \
            bbp_workflow.simulation SimCampaign \
                sim-config-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/sim-cfg-id \
                parallel-jobs=100
    """

    sim_config_url = _sim_config_url_param()  #:
    benchmark = _benchmark_param()  #:
    sim_indices = OptionalParameter(
        default=None, description="Indices of the simulations to run. " "For example: 0-10,20"
    )  #:
    enable_shm = _enable_shm()  #:
    config_file = Parameter(
        default="BlueConfig",
        description="Name of the BlueConfig file. If sonata circuit was used, will be "
        'autoreplaced with "simulation_config.json".',
    )

    def requires(self):
        """:class:`LookupSimulationCampaign`."""
        if self.sim_config_url:
            sim_config_url = self.sim_config_url
        else:
            # FIXME remove once migrated to SimCampaign
            sim_config_url = SimulationCampaign().sim_config_url
        return _lookup_sim_campaign(sim_config_url)

    def _sim_campaign_cfg(self):
        return self.input().entity

    def run(self):
        """"""
        sim_campaign_cfg = self._sim_campaign_cfg()
        config = read_config(sim_campaign_cfg.configuration.as_dict())
        if self.benchmark:
            sim_indices = [_campaign_sim_indices(config)[-1]]
        else:
            if self.sim_indices:
                sim_indices = _sim_indices_to_list(self.sim_indices)
            else:
                sim_indices = _campaign_sim_indices(config)
        path_prefix_name = Path(config.attrs["path_prefix"]) / config.name

        is_sonata = _is_sonata(sim_campaign_cfg)
        sbatch = self._prepare_sbatch(
            is_sonata, path_prefix_name, sim_indices, self.enable_shm, self.config_file
        )
        generated_by_url = sim_campaign_cfg.wasGeneratedBy.get_url()
        monitoring_thread = None
        # pylint: disable=not-callable
        try:
            with _run_sbatch(DEFAULT_HOST, sbatch, job_script_path=path_prefix_name) as (
                job_id,
                ssh_proc,
            ):
                generated_by = SimulationCampaignGeneration.from_url(generated_by_url)
                # status_html = DataDownload(
                #     name=f'{job_id} status',
                #     contentUrl=ood_url(path_prefix_name / f'.{job_id}.html'),
                #     encodingFormat='text/html')
                # generated_by.evolve(startedAtTime=datetime.now(),
                #                     status='Running',
                #                     distribution=status_html).publish()
                generated_by.evolve(startedAtTime=datetime.now(), status="Running").publish()
                monitoring_thread = Thread(
                    target=_sim_monitoring, args=(ssh_proc, job_id, config, path_prefix_name)
                )
                monitoring_thread.start()
                monitoring_file = _monitoring_file(path_prefix_name, job_id)
                self.set_tracking_url(
                    f'https://bbp-workflow-web-{environ["USER"]}.kcp.bbp.epfl.ch'
                    f"/{monitoring_file.parent.name}/{monitoring_file.name}"
                )
            generated_by = SimulationCampaignGeneration.from_url(generated_by_url)
            generated_by.evolve(endedAtTime=datetime.now(), status="Done").publish()
        except RuntimeError:
            generated_by = SimulationCampaignGeneration.from_url(generated_by_url)
            generated_by.evolve(endedAtTime=datetime.now(), status="Failed").publish()
            raise
        finally:
            if monitoring_thread:
                monitoring_thread.join()

    def output(self):
        """[:class:`RemoteTarget<luigi.contrib.ssh.RemoteTarget>`\\(path = ``cfg.SUCCESS``\\)]."""
        sim_campaign_cfg = self._sim_campaign_cfg()
        is_sonata = _is_sonata(sim_campaign_cfg)
        if is_sonata:
            config_file = SIM_CFG_JSON
        else:
            config_file = self.config_file
        config = read_config(sim_campaign_cfg.configuration.as_dict())
        return [
            RemoteTarget(host=DEFAULT_HOST, path=Path(path) / f"{config_file}.SUCCESS")
            for _, path in campaign_sims(config)
        ]


@inherits(RemoteHostCfg)
class SimCampaignThrottle(Task):
    r"""Change the amount of simulations running in parallel for the campaign.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow --config simulation.cfg \
            bbp_workflow.simulation SimCampaignThrottle \
                sim-config-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/sim-cfg-id \
                parallel-jobs=20
    """

    sim_config_url = _sim_config_url_param()  #:
    parallel_jobs = IntParameter(
        default=10, significant=False, description="Amount of simulations running in parallel."
    )  #:
    job_id = OptionalIntParameter(
        default=None,
        significant=False,
        description="If multiple jobs are running, "
        "be specific about which job should be affected.",
    )  #:

    def requires(self):
        """:class:`LookupSimulationCampaign`."""
        return _lookup_sim_campaign(self.sim_config_url)

    def run(self):
        """"""
        sim_campaign_cfg = self.input().entity
        config = read_config(sim_campaign_cfg.configuration.as_dict())
        campaign_path = Path(config.attrs["path_prefix"]) / config.name
        cmd = "squeue --noheader --format=%F "
        cmd += f"--jobs=$(find {campaign_path} -maxdepth 1 -type f -name '.*.script' "
        cmd += "-execdir bash -c 'echo ${1//[^0-9]/}' _ {} \\; | paste -d, -s) | sort | uniq"
        out = _run_cmd(self.host, cmd, capture_output=True)
        lines = out.splitlines()
        job_id = None
        if len(lines) == 1:
            job_id = lines[0]
        if self.job_id is not None:
            job_id = self.job_id
        if job_id is not None:
            cmd = f"scontrol update ArrayTaskThrottle={self.parallel_jobs} JobId={job_id}"
            _run_cmd(self.host, cmd)
            self.output().done()
        else:
            raise ValueError(
                f'Multiple or no running JOB_IDs found: "{lines}"! '
                "Please specify exact JOB_ID as the parameter on the throttle task!"
            )

    def output(self):
        """"""
        return RunAnywayTarget(self)


@inherits(RemoteHostCfg)
class SimCampaignHalt(Task):
    r"""Stop simulation campaign.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow --config simulation.cfg \
            bbp_workflow.simulation SimCampaignHalt \
                sim-config-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/sim-cfg-id
    """

    sim_config_url = _sim_config_url_param()  #:
    force = BoolParameter(
        default=False,
        significant=False,
        parsing=BoolParameter.EXPLICIT_PARSING,
        description="By default halts only pending jobs from the array, "
        "already queued/running will finish. "
        "Set to True in order to scancel all.",
    )  #:

    def requires(self):
        """:class:`LookupSimulationCampaign`."""
        return _lookup_sim_campaign(self.sim_config_url)

    def run(self):
        """"""
        sim_campaign_cfg = self.input().entity
        config = read_config(sim_campaign_cfg.configuration.as_dict())
        campaign_path = Path(config.attrs["path_prefix"]) / config.name
        if self.force:
            # cancel all found job ids in folder
            cmd = f"find {campaign_path} -maxdepth 1 -type f -name '.*.script' "
            cmd += "-execdir bash -c 'echo ${1//[^0-9]/}' _ {} \\; "
            cmd += " | xargs --no-run-if-empty scancel"
        else:
            # cancel only currently pending job ids else unnecessary errors shown
            cmd = "squeue --noheader --states=PENDING --format=%A "
            cmd += f"--jobs=$(find {campaign_path} -maxdepth 1 -type f -name '.*.script' "
            cmd += "-execdir bash -c 'echo ${1//[^0-9]/}' _ {} \\; | paste -d, -s)"
            cmd += " | xargs --no-run-if-empty scancel --state=PENDING"
        _run_cmd(self.host, cmd)
        self.output().done()

    def output(self):
        """"""
        return RunAnywayTarget(self)


class GenerateAndRunSimCampaign(SimCampaign):
    """Generate and run simulation campaign."""

    sim_config_url = OptionalParameter(default=None)
    circuit_url = OptionalParameter(
        default=None,
        description="Optionally provide detailed circuit URL or configure the URL directly "
        "on the LookupDetailedCircuit task.",
    )  #:

    def requires(self):
        """:class:`GenerateSimulationCampaign`."""
        if self.circuit_url:
            return GenerateSimulationCampaign(circuit_url=self.circuit_url)
        return GenerateSimulationCampaign()

    def _sim_campaign_cfg(self):
        url = self.input().get_data()
        if url is None:
            return None
        return SimulationCampaignConfiguration.from_url(url)

    def complete(self):
        """"""
        if self._sim_campaign_cfg() is None:
            return False
        return super().complete()


class GenerateCoupledCoordsAndRunSimCampaign(GenerateAndRunSimCampaign):
    """Generate and run coupled coords simulation campaign."""

    def requires(self):
        """:class:`GenerateCoupledCoordsSimulationCampaign`."""
        if self.circuit_url:
            return GenerateCoupledCoordsSimulationCampaign(circuit_url=self.circuit_url)
        return GenerateCoupledCoordsSimulationCampaign()


class CampaignSimProcessorSbatch(SbatchTask):
    """Simulation from the campaign processor base task."""

    coords = DictParameter(description="Simulation conditions.")  #:
    is_sonata = _is_sonata_param()  #:

    def requires(self):
        """Require simulation."""
        sim_campaign_task = SimulationCampaign()
        sim_task = sim_campaign_task.simulation_type
        return sim_campaign_task.clone(sim_task, chdir=self.chdir, is_sonata=self.is_sonata)


class SimulationCampaignProcessor(WrapperTask):
    """Apply processor tasks to individual simulations form the campaign."""

    sim_config_url = _sim_config_url_param()  #:
    processor_task = TaskParameter(description="Custom task applied to each simulation.")  #:
    coords_filter = DictParameter(default={}, description="Selects subset of coords.")  #:

    def requires(self):
        """Require simulation campaign individual processor tasks."""
        sim_campaign_cfg = _lookup_sim_campaign(self.sim_config_url).output().entity
        is_sonata = _is_sonata(sim_campaign_cfg)
        config = read_config(sim_campaign_cfg.configuration.as_dict(), self.coords_filter)
        # pylint: disable=not-callable
        return [
            self.processor_task(coords=coords, chdir=sim_path, is_sonata=is_sonata)
            for coords, sim_path in campaign_sims(config)
        ]


class MoveSimCampaign(Task):
    r"""Move simulation campaign configuration to the new path prefix.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation MoveSimCampaign \
                sim-config-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/sim-cfg-id \
                path-prefix=/gpfs/bbp.cscs.ch/path/where/to/move
    """

    sim_config_url = _sim_config_url_param()  #:
    path_prefix = Parameter(
        description="New GPFS path prefix where to move all simulation campaign files."
    )  #:

    def requires(self):
        """:class:`LookupSimulationCampaign`."""
        return _lookup_sim_campaign(self.sim_config_url)

    def run(self):
        """"""
        kg_cfg = self.requires()
        sim_campaign_cfg = self.input().entity
        config = xr_from_dict(sim_campaign_cfg.configuration.as_dict())
        from_path = Path(config.attrs["path_prefix"]) / config.name
        to_path = Path(self.path_prefix) / config.name
        assert from_path != to_path, f'Same path prefix provided: "{self.path_prefix}"!'
        print(
            f"Moving SIM CAMPAIGN: {sim_campaign_cfg.get_url()}\n"
            f"from PATH: {from_path}\n"
            f"to   PATH: {to_path}",
            flush=True,
        )
        assert from_path.exists(), f'"{from_path}" does not exist!'
        assert to_path.parent.exists(), f'"{to_path.parent}" does not exist!'

        try:
            out = _run_cmd(DEFAULT_HOST, f"mv -nvT {from_path} {to_path}", capture_output=True)
            print(f"SIM CAMPAIGN move command output:\n{out}", flush=True)
        except RuntimeError as re:
            print(f"SIM CAMPAIGN move command failed:\n{re}", flush=True)

        assert to_path.exists(), f'"{to_path}" was not created!'
        config.attrs["path_prefix"] = str(self.path_prefix)
        sim_campaign_cfg = sim_campaign_cfg.evolve(
            configuration=DataDownload.from_json_str(
                to_str(xr_to_dict(config)),
                base=kg_cfg.kg_base,
                org=kg_cfg.kg_org,
                proj=kg_cfg.kg_proj,
            )
        )
        sim_campaign_cfg.publish(base=kg_cfg.kg_base, org=kg_cfg.kg_org, proj=kg_cfg.kg_proj)
        print(f'Updated SIM CAMPAIGN path_prefix to: "{self.path_prefix}"', flush=True)

        self.output().done()

    def output(self):
        """"""
        return RunAnywayTarget(self)


class RegisterSimCampaignFromConfig(KgTask):
    r"""Register simulation campaign configuration from the existing `config.json`.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation RegisterSimCampaignFromConfig \
                name="Sim campaign name" \
                description="Sim campaign description" \
                kg-proj=my-nexus-project \
                config-path=/gpfs/bbp.cscs.ch/path/config.json
    """

    name = OptionalParameter(default=None)  #:
    description = OptionalParameter(default=None)  #:
    config_path = Parameter(description="GPFS path to the `config.json`.")  #:

    def run(self):
        """"""
        sim_campaign_cfg = SimulationCampaignConfiguration(
            name=self.name,
            description=self.description,
            configuration=DataDownload.from_file(
                self.config_path,
                base=self.kg_base,
                content_type="application/json",
                org=self.kg_org,
                proj=self.kg_proj,
            ),
            template=DataDownload.from_json_str(
                "", base=self.kg_base, org=self.kg_org, proj=self.kg_proj
            ),
        )
        sim_campaign_cfg = self.publish(sim_campaign_cfg)
        self.done("Simulation campaign:", sim_campaign_cfg)


class CleanUpSimCampaignFiles(Task):
    r"""Clean up files from the simulation campaign folders.

    **Usage**:

    .. code-block:: bash

        bbp-workflow launch --follow \
            bbp_workflow.simulation CleanUpSimCampaignFiles \
                sim-config-url=https://bbp.epfl.ch/nexus/v1/resources/bbp/project/_/sim-cfg-id \
                files='["AllCompartments.bbp"]'
    """

    sim_config_url = _sim_config_url_param()  #:
    files = ListParameter(
        description="List of files for deletion from each individual simulation folder."
    )  #:

    def requires(self):
        """:class:`LookupSimulationCampaign`."""
        return _lookup_sim_campaign(self.sim_config_url)

    def run(self):
        """"""
        sim_campaign_cfg = self.input().entity
        config = read_config(sim_campaign_cfg.configuration.as_dict())
        for _, sim_path in campaign_sims(config):
            for file in self.files:  # pylint: disable=not-an-iterable
                f = Path(sim_path) / file
                if f.exists():
                    print(f"Removing: {f}", flush=True)
                    f.unlink()
        self.output().done()

    def output(self):
        """"""
        return RunAnywayTarget(self)
