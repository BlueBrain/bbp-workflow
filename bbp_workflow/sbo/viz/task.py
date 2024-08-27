# SPDX-License-Identifier: Apache-2.0

"""Specific simulation campaign visualization SBO tasks."""

from enum import Enum
from pathlib import Path

import brayns
from entity_management.analysis import AnalysisReport
from entity_management.base import Derivation
from entity_management.simulation import Simulation as SimEntity
from entity_management.simulation import SimulationCampaignAnalysis
from luigi import (
    BoolParameter,
    EnumParameter,
    FloatParameter,
    IntParameter,
    ListParameter,
    OptionalIntParameter,
    OptionalListParameter,
    OptionalParameter,
    Parameter,
    WrapperTask,
)

from bbp_workflow.luigi import RemoteTarget, inherits
from bbp_workflow.sbo.analysis.sim.task import MultiAnalyseSimCampaignMeta
from bbp_workflow.sbo.sim.task import ReportsSimCampaignMeta, RunSimCampaign, RunSimCampaignMeta
from bbp_workflow.sbo.task import BaseTask, MetaTask
from bbp_workflow.settings import BBP_WORKFLOW_SIF, BRAYNS_SIF, DEFAULT_HOST, ENVIRONMENT
from bbp_workflow.simulation.util import campaign_sims, read_config
from bbp_workflow.slurm import _run_cmd, _run_sbatch, _run_srun, _sbatch, _srun
from bbp_workflow.task import EnvCfg, SlurmCfg
from bbp_workflow.util import amend_cmd, singularity_run

FRAMES_PATTERN = "%d.png"


class _VIEWS(Enum):
    front = brayns.CameraRotation.front
    back = brayns.CameraRotation.back
    left = brayns.CameraRotation.left
    right = brayns.CameraRotation.right
    top = brayns.CameraRotation.top
    bottom = brayns.CameraRotation.bottom


class _PROJECTIONS(Enum):
    perspective = brayns.PerspectiveProjection
    orthographic = brayns.OrthographicProjection


def _get_nodes(job_id):
    nodes = _run_cmd(
        DEFAULT_HOST,
        f"srun --jobid={job_id} --overlap bash -c 'echo $(hostname):$TMPDIR'",
        capture_output=True,
    )
    nodes = {node.split(":")[0]: node.split(":")[1] for node in nodes.split("\n")}
    assert all(len(tmp) > 1 for tmp in nodes.values()), f"TMPDIR is not set up! {nodes=}"
    node_count = len(nodes)
    return nodes, node_count


def _get_instances(nodes):
    return [
        brayns.Connector(f"{node}:5000", max_attempts=20, attempt_period=5).connect()
        for node in nodes
    ]


def _init_instances(instances, obj, sim_config):
    loader = brayns.SonataLoader(
        [
            brayns.SonataNodePopulation(
                name=population["name"],
                nodes=brayns.SonataNodes.from_density(population["density"]),
                report=brayns.SonataReport(
                    type=brayns.SonataReportType(population["report_type"]),
                    name=population["report_name"],
                ),
                morphology=brayns.Morphology(
                    radius_multiplier=population["radius_multiplier"],
                    load_soma=population["load_soma"],
                    load_dendrites=population["load_dendrites"],
                    load_axon=population["load_axon"],
                ),
            )
            for population in obj.populations
        ]
    )

    tasks = [loader.load_models_task(instance, str(sim_config)) for instance in instances]
    for task in tasks:
        task.wait_for_result()

    for instance in instances:
        brayns.clear_lights(instance)
        brayns.add_light(instance, brayns.AmbientLight(0.5))
        direction = brayns.Vector3(1, -1, -1).normalized
        direction = obj.camera_view.value.apply(direction)
        brayns.add_light(instance, brayns.DirectionalLight(intensity=10, direction=direction))

        if obj.camera_position is None:
            controller = brayns.CameraController(
                target=brayns.get_bounds(instance),
                aspect_ratio=brayns.Resolution(*obj.resolution).aspect_ratio,
                rotation=obj.camera_view.value,
                projection=obj.camera_type.value,
            )
            camera = controller.camera
        else:
            camera = brayns.Camera(
                view=brayns.View(
                    position=brayns.Vector3(*obj.camera_position),
                    target=brayns.Vector3(*obj.camera_target),
                    up=brayns.Vector3(*obj.camera_up),
                ),
                projection=brayns.OrthographicProjection(obj.camera_height),
            )

        brayns.set_camera(instance, camera)

        brayns.set_renderer(
            instance,
            brayns.ProductionRenderer(background_color=brayns.Color4(*obj.background_color)),
        )


def _get_frame_indices(instances, obj):
    sim = brayns.get_simulation(instances[0])
    frames = brayns.MovieFrames(
        fps=obj.fps,
        slowing_factor=obj.slowing_factor,
        start_frame=obj.start_frame,
        end_frame=obj.end_frame,
    )
    return frames.get_indices(sim)


def _make_movie(job_id, nodes, obj, path):
    node = next(iter(nodes))
    # rsync frames to first node, random sleep to avoid ssh flooding
    _run_cmd(
        DEFAULT_HOST,
        (
            f"srun --jobid={job_id} --overlap bash -c '"
            f"sleep $((RANDOM % 100)) && rsync -lptgoDv -e "
            '"ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no" '
            f"--dirs=. $TMPDIR/ {node}:{nodes[node]}'"
        ),
    )

    movie = brayns.Movie(frames_pattern=FRAMES_PATTERN, fps=obj.fps)
    cmd = movie.get_command_line(path)
    cmd = " ".join(cmd)
    cmd = f"ls && {cmd}"

    if ENVIRONMENT in {"aws", "bbp"}:
        cmd = (
            f"srun --jobid={job_id} --nodelist={node} --chdir={nodes[node]} --overlap "
            f"{singularity_run(BBP_WORKFLOW_SIF, cmd, pwd=nodes[node])}"
        )
        if ENVIRONMENT == "bbp":
            cmd = f"module load {obj.module_archive} singularityce && {cmd}"
    else:
        cmd = (
            f"module load {obj.module_archive} ffmpeg && "
            f"srun --jobid={job_id} --nodelist={node} --chdir={nodes[node]} --overlap {cmd}"
        )
    _run_cmd(DEFAULT_HOST, cmd)


@inherits(SlurmCfg, EnvCfg)
class VideoSimCampaign(BaseTask):
    """Task to render a video from nexus simulation and upload it to nexus."""

    modules = Parameter(
        default="brayns py-brayns", significant=False, description="Environment modules to load."
    )  #:

    container = Parameter(default=BRAYNS_SIF, description="Singularity container to use.")  #:

    exclusive = BoolParameter(
        default=True,
        significant=False,
        description="Allocate nodes exclusively and do not share them with other jobs.",
    )  #:

    mem = Parameter(
        default="0", significant=False, description="Real memory required per node."
    )  #:

    ntasks_per_node = IntParameter(
        default=1, significant=False, description="Amount of tasks executed per node."
    )  #:

    job_name = OptionalParameter(
        default="brayns", significant=False, description="Specify a name for the job."
    )  #:

    constraint = OptionalParameter(
        default="nvme",
        significant=False,
        description="Specify which type of nodes to use in allocation.",
    )  #:

    parallel_jobs = IntParameter(
        default=10, significant=False, description="Amount of viz jobs running in parallel."
    )  #:

    sim_indices = OptionalParameter(
        default=None, description="Indices of the simulations to visualize. " "For example: 0-10,20"
    )  #:

    populations = ListParameter(
        description="Node populations to load, each population is a dict with"
        "the following keys: name, report_type, report_name, density=1, "
        "radius_multiplier=10, load_soma=true, load_dendrites=false, "
        "load_axon=false",
        schema={
            "type": "array",
            "items": {"$ref": "#/$defs/population"},
            "$defs": {
                "population": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "report_type": {"type": "string"},
                        "report_name": {"type": "string"},
                        "density": {"type": "number"},
                        "radius_multiplier": {"type": "number"},
                        "load_soma": {"type": "boolean"},
                        "load_dendrites": {"type": "boolean"},
                        "load_axon": {"type": "boolean"},
                    },
                    "required": ["name", "report_type", "report_name"],
                }
            },
        },
    )  #:

    resolution = ListParameter(
        default=[1920, 1080],
        description="Image resolution in pixels (width, height)",
        schema={"type": "array", "minItems": 2, "maxItems": 2, "items": {"type": "integer"}},
    )  #:

    camera_type = EnumParameter(
        default="orthographic", enum=_PROJECTIONS, description="Camera type."
    )  #:

    camera_view = EnumParameter(default="front", enum=_VIEWS, description="Camera view.")  #:

    camera_position = OptionalListParameter(
        default=None,
        description="Custom camera position [x, y, z]",
        schema={"type": "array", "minItems": 3, "maxItems": 3, "items": {"type": "number"}},
    )  #:

    camera_target = OptionalListParameter(
        default=None,
        description="Custom camera target [x, y, z]",
        schema={"type": "array", "minItems": 3, "maxItems": 3, "items": {"type": "number"}},
    )  #:

    camera_up = OptionalListParameter(
        default=None,
        description="Custom camera up [x, y, z]",
        schema={"type": "array", "minItems": 3, "maxItems": 3, "items": {"type": "number"}},
    )  #:

    camera_height = OptionalIntParameter(default=None, description="Height of camera viewport")  #:

    background_color = ListParameter(
        default=[1, 1, 1, 0],
        description="Background color RGBA",
        schema={"type": "array", "minItems": 4, "maxItems": 4, "items": {"type": "number"}},
    )  #:

    fps = FloatParameter(default=25, description="Video FPS.")  #:

    slowing_factor = FloatParameter(
        default=1.0, description="Slowing factor compared to real time (2 = twice slower)"
    )  #:

    start_frame = IntParameter(
        default=0, description="Index of the first simulation frame (python style)"
    )  #:

    end_frame = IntParameter(
        default=-1, description="Index of the last simulation frame (python style)"
    )  #:

    report_name = Parameter(
        default="Videos", description="Name of the report resource in the knowledge graph."
    )  #:

    report_description = Parameter(
        default="Video of the simulation", description="Description of the report resource."
    )  #:

    categories = ListParameter(
        default=["Soma voltage"], description="Categories to which the report belongs."
    )  #:

    types = ListParameter(
        default=["Validation"], description="Types of reports to which the report belongs."
    )  #:

    file_name = Parameter(default="movie.mp4", description="Name of the video file.")  #:

    def requires(self):
        """Require simulation campaign."""
        return RunSimCampaign()

    def _generate_frames_and_make_movie(self, job_id):
        nodes, node_count = _get_nodes(job_id)
        instances = _get_instances(nodes)
        for sim_config_success in self.input():
            _init_instances(instances, self, Path(sim_config_success.path).with_suffix(""))
            frame_indices = _get_frame_indices(instances, self)
            snapshot = brayns.Snapshot(resolution=brayns.Resolution(*self.resolution))
            tasks = []
            for i, frame_i in enumerate(frame_indices):
                snapshot.frame = frame_i
                tasks.append(
                    snapshot.save_remotely_task(instances[i % node_count], FRAMES_PATTERN % i)
                )
            for task in tasks:
                task.wait_for_result()

            _make_movie(
                job_id, nodes, self, str(Path(sim_config_success.path).with_name(self.file_name))
            )

        for instance in instances:
            brayns.stop(instance)

    def run(self):
        """Render video images in a temp folder and assemble the movie in sim folder."""
        env = self.clone(EnvCfg)
        if ENVIRONMENT == "aws":
            env = self.clone(EnvCfg, modules=None)
        elif ENVIRONMENT == "bbp":
            env = self.clone(EnvCfg, modules="singularityce")
        cmd = amend_cmd(
            self.container, "braynsService --uri 0.0.0.0:5000 --plugin braynsCircuitExplorer"
        )
        brayns_svc = _sbatch([(self.clone(SlurmCfg), cmd)], env, pre_cmd="cd $TMPDIR")
        with _run_sbatch(DEFAULT_HOST, brayns_svc) as (job_id, _):
            wait_for_nodes = _srun(
                self.clone(SlurmCfg, ntasks=1, time="1:00", dependency=f"after:{job_id}"),
                EnvCfg(),
                "true",
            )
            _run_srun(DEFAULT_HOST, wait_for_nodes)
            self._generate_frames_and_make_movie(job_id)

    def output(self):
        sims = self.input()
        if isinstance(sims, RemoteTarget):  # no sims results in target to missing config
            return sims  # propagate missing target, so upstream tasks can run
        return [
            RemoteTarget(
                host=DEFAULT_HOST, path=str(Path(sim_config_success.path).with_name(self.file_name))
            )
            for sim_config_success in self.input()
        ]


class VideoSimCampaignMeta(MetaTask):
    """Meta task that specifies which task does the job and what nexus resource is registered."""

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
        base_task = self.get_base_task_instance()
        for i, (coords, sim_path) in enumerate(campaign_sims(config, include_empty=True)):
            path = Path(sim_path) / base_task.file_name
            name = "__".join(f"{k}={v}" for k, v in coords.items()) + path.suffix
            distribution = self.from_file(
                str(path), name=name, content_type=f"video/{path.suffix[1:]}"
            )
            resource = AnalysisReport(
                name=base_task.report_name,
                description=base_task.report_description,
                distribution=distribution,
                categories=list(base_task.categories),
                types=list(base_task.types),
                derivation=Derivation(entity=index_to_sim[i]),
            )
            self.publish(resource, activity=activity)
        return activity, None


class RunAllSimCampaignMeta(WrapperTask):
    """."""

    def requires(self):
        """."""
        return [MultiAnalyseSimCampaignMeta(), ReportsSimCampaignMeta(), VideoSimCampaignMeta()]
