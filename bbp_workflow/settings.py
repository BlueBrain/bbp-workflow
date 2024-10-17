# SPDX-License-Identifier: Apache-2.0

"""Settings."""

import logging
import logging.config
import os
from pathlib import Path

from bbp_workflow import __version__ as VERSION

DEBUG = os.getenv("DEBUG", None)
LOGGING_CFG = Path("logging.cfg")
if LOGGING_CFG.exists():
    logging.config.fileConfig(LOGGING_CFG, disable_existing_loggers=False)
logging.getLogger("entity_management").setLevel(
    logging.DEBUG if os.getenv("DEBUG_KG", None) else logging.INFO
)
L = logging.getLogger("bbp_workflow")
L.setLevel(logging.DEBUG if DEBUG else logging.INFO)

DEFAULT_HOST = os.getenv("HPC_HEAD_NODE", "bbpv1.epfl.ch")

# None: on bb5 with modules, bbp: container on bb5, aws: container on amazon
ENVIRONMENT = os.getenv("HPC_ENVIRONMENT")

AVAILABLE_NODES = 800

HTTPS_PROXY = "http://bbpproxy.epfl.ch:80/"

MODULE_ARCHIVE = "archive/2024-09"

CFG_JSON = "config.json"
SIM_CFG_JSON = "simulation_config.json"
BLUE_CFG = "BlueConfig"

WORKFLOW_MODULE = "py-bbp-workflow"
ENTITY_MANAGEMENT_MODULE = "py-entity-management"
BLUEPY_MODULE = "py-bluepy"
BRION_MODULE = "brion"
BRAIN_BUILDER_MODULE = "brainbuilder"
PLACEMENT_ALGORITHM_MODULE = "placement-algorithm"
TOUCH_DETECTOR_MODULE = "touchdetector"
PARQUET_CONVERTERS_MODULE = "parquet-converters"
SPYK_FUNC_MODULE = "spykfunc"
MORPHOLOGY_REPAIR_MODULE = "py-morphology-repair-workflow"
ANALYSIS_FRAMEWORK_MODULE = "py-bbp-analysis-framework"
NEURODAMUS_NEOCORTEX_MODULE = "neurodamus-neocortex"
NEURODAMUS_NEOCORTEX_PLASTICITY_MODULE = "neurodamus-neocortex-plasticity"
NEURODAMUS_MULTISCALE_MODULE = "py-multiscale-run"
NEURODAMUS_THALAMUS_MODULE = "neurodamus-thalamus"
NEURODAMUS_HIPPOCAMPUS_MODULE = "neurodamus-hippocampus"
PY_NEURODAMUS_MODULE = "py-neurodamus"
EMSIM_MODULE = "emsim"

SIF_PREFIX = os.getenv("HPC_SIF_PREFIX", "/gpfs/bbp.cscs.ch/ssd/containers/hpc/spackah")
DATA_PREFIX = os.getenv("HPC_DATA_PREFIX", "/gpfs/bbp.cscs.ch/project")
PATH_PREFIX = os.getenv("HPC_PATH_PREFIX", "/gpfs/bbp.cscs.ch/home")
if ENVIRONMENT == "aws":
    BBP_WORKFLOW_SIF = f'py-bbp-workflow__{".".join(VERSION.split(".")[:3])}-amd64.sif'
else:
    # for testing on bb5
    BBP_WORKFLOW_SIF = "py-bbp-workflow____py-bbp-workflow-dev.sif"
BRAYNS_SIF = "brayns__3.4.1-amd64.sif"
NEURODAMUS_HIPPOCAMPUS_SIF = "neurodamus-hippocampus__1.8-2.16.6-2.8.1-amd64.sif"
NEURODAMUS_NEOCORTEX_SIF = "neurodamus-neocortex__1.13-2.16.6-2.8.1-amd64.sif"
NEURODAMUS_NEOCORTEX_MULTISCALE_SIF = "neurodamus-neocortex-multiscale__1.13-2.16.6-2.8.1-amd64.sif"
