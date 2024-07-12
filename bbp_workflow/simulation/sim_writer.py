# SPDX-License-Identifier: Apache-2.0

"""Simulation campaign writer."""

import json
import re
import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from bluepysnap.simulation import Simulation as SonataSim
from Cheetah.Filters import Filter
from Cheetah.Template import Template
from pint import Quantity

import libsonata
from bbp_workflow.settings import BLUE_CFG, SIM_CFG_JSON, L
from bbp_workflow.simulation.util import (
    _campaign_sims_with_indices,
    _xr_get_multi_index,
    campaign_sims,
    xr_to_dict,
)
from bbp_workflow.simulation.vocab import get_units
from bbp_workflow.util import fix_workflows_path, import_func, to_str


def generate_stim_file(*, path, noise_rate, **_):
    """Map ``noise_rate`` to input spike file at ``path`` location.

    .. literalinclude:: ../../../bbp_workflow/simulation/sim_writer.py
        :pyobject: generate_stim_file
        :dedent: 4
        :lines: 9-
    """
    file_name = "input.spikes"
    (Path(path) / file_name).write_text(f"my input spikes at {noise_rate}")
    return {"stim_file": file_name}


def user_target_processor(path, user_target=None):
    """Map ``user_target`` file path to file name while copying it to ``path`` location.

    This map function is always invoked when generating simulation campaigns, so you don't have
    to enable it explicitly.

    .. literalinclude:: ../../../bbp_workflow/simulation/sim_writer.py
        :pyobject: user_target_processor
        :dedent: 4
        :lines: 12-
    """
    if user_target:
        user_target = Path(user_target)
        assert user_target.exists(), f"Not found: {user_target}"
        shutil.copy(user_target, path)
        return {"user_target_file_name": user_target.name}
    else:
        return {}


def _apply(*param_processors):
    """Sequentially compose mapping functions.

    This is used when generating template variable values:

    .. code-block:: python

        apply_map_funs(default_depolarization_treatment,
                       default_ca_treatment)(depolarization=80, ca=1.2)

    Returns:
        Function which, when invoked with ``kwargs``, will start applying ``param_processors``
        in a sequence, gradually extending ``kwargs`` and passing them to the next parameter
        processing function.
    """

    def map_collection(**kwargs):
        dictionary = kwargs
        for param_processor in param_processors:
            func = import_func(param_processor)
            dictionary.update(func(**dictionary))
        return dictionary

    return map_collection


def parse_circuit_config(path):
    """Assert that circuit config path exists and return the parsed CircuitConfig.

    Args:
        path (str): Path to the circuit config file or sonata json.

    Returns:
        libsonata.CircuitConfig
        Parsed circuit config.
    """
    path = Path(path)
    assert path.exists(), f"Not found: {path}"
    if path.suffix.lower() == ".json":
        circuit = libsonata.CircuitConfig.from_file(path)
    else:
        circuit = {}  # used to be BlueConfig
    return circuit


def _parse_blue_config_template(config):
    placeholders = set()

    class _VarCollector:
        def __init__(self, prefix=None):
            self._prefix = prefix

        def __getitem__(self, key):
            if isinstance(key, _VarCollector):
                # this handles the case: $var1[$var2]
                return _VarCollector()
            if self._prefix:
                full_key = f"{self._prefix}.{key}"
            else:
                full_key = f"{key}"
            placeholders.add(full_key)
            return _VarCollector(prefix=full_key)

    blue_config_template = Path(fix_workflows_path(config.attrs["blue_config_template"]))
    assert blue_config_template.exists(), f"Not found: {blue_config_template}"
    blue_config_template = Template.compile(file=str(blue_config_template))

    # collect all the placeholders available in the template
    str(blue_config_template(searchList=[_VarCollector()]))

    return blue_config_template, placeholders


def _update_progress(i, total, set_progress_percentage):
    if set_progress_percentage:
        progress = int(i / total * 100.0)
        set_progress_percentage(progress)


class _Filter(Filter):

    def filter(self, val, **kwargs):  # pylint: disable=arguments-differ
        if isinstance(val, Quantity):
            units = get_units(re.sub(r"^\$", "", kwargs["rawExpr"]))
            if units:
                return str(val.to(units).magnitude)
            return str(val.magnitude)
        return super().filter(val, **kwargs)


def _write_sim_config(simulation_path, template, placeholders, is_sonata, validate_output):
    # pylint: disable=unused-argument
    content = str(template)
    if is_sonata:
        path = simulation_path / SIM_CFG_JSON
    else:
        path = simulation_path / BLUE_CFG
    if not path.exists():
        path.write_text(content)
        if validate_output and is_sonata:
            SonataSim(path)
    else:
        L.warning('Sim config at "%s" already exists, skipping!', path)
    # (simulation_path / 'BlueConfig.json').write_text(
    #     to_str({key: template.getVar(key) for key in placeholders
    #             if not isinstance(template.getVar(key), Mapping)},
    #            indent=4, sort_keys=True))


def _generate_sim(counter, loc, config, param_processors, optional_seed, validate_output):
    # loc ex. {'depolarization': 75.0, 'ca': 1.15}
    circuit = parse_circuit_config(config.attrs["circuit_config"])
    is_sonata = isinstance(circuit, libsonata.CircuitConfig)
    blue_config_template, placeholders = _parse_blue_config_template(config)
    # even if something was not used in template we need to record it in BlueConfig.json
    placeholders |= set(config.coords.to_index().names) | set(config.attrs.keys())

    path_prefix = Path(config.attrs["path_prefix"])
    simulation_path = path_prefix / config.name / str(counter)
    simulation_path.mkdir(parents=True, exist_ok=True)
    template = blue_config_template(
        filter=_Filter,
        searchList=[
            {"path": str(simulation_path)},
            config.attrs,
            loc,
            user_target_processor(
                path=str(simulation_path),
                user_target=fix_workflows_path(config.attrs.get("user_target")),
            ),
            optional_seed,
            _apply(*param_processors)(path=str(simulation_path), **config.attrs, **loc),
            circuit if not is_sonata else json.loads(circuit.expanded_json),
        ],
    )
    _write_sim_config(simulation_path, template, placeholders, is_sonata, validate_output)
    return (loc, str(simulation_path.relative_to(path_prefix)))


def _get_seeds(config, rnd):
    rnd_seed_range = config.attrs.get("rnd_seed_range")
    if rnd_seed_range:
        assert "seed" not in config.attrs, (
            'Using "rnd_seed_range" although "seed" already ' 'present in "attrs"!'
        )
        assert "seed" not in config.coords, (
            'Using "rnd_seed_range" although "seed" already ' 'present in "coords"!'
        )
        low = rnd_seed_range[0]
        high = rnd_seed_range[1]
        return [
            {"seed": seed} for seed in low + rnd.choice(high - low + 1, config.size, replace=False)
        ]
    else:
        return [{}] * config.size


def generate_campaign(
    config,
    coords_filter_func,
    param_processors,
    workers,
    set_progress_percentage,
    rnd,
    validate_output=True,
):
    """Generate simulation campaign.

    Args:
        config (xarray.DataArray): Simulation campaign configuration. Config must contain at least
            the following:

            * ``name``: Full path will be: ``attrs.path_prefix/name``.
            * ``attrs.path_prefix``: base location of the generated campaign.
            * ``attrs.blue_config_template``: path to the blue config cheetah template file.
            * ``attrs.circuit_config``: circuit config path. CircuitConfig file properties will
              be available for use in the template placeholders. For convenience ``Run_Default``
              section properties will be available directly(ex. ``$MorphologyPath``). Properties
              from the other sections need to be accessed by direct path(ex.
              ``$Projection_Thalamocortical_input_VPM.Path``).
            * ``attrs.user_target``: optional path to the custom user target file. It will be
              copied to the generated simulation folders. And ``user_target_file_name`` placeholder
              will be available for use in the template. See :func:`user_target_processor`.
            * ``attrs.rnd_seed_range``: optional list of two parameters to numpy randint function
              to generate ``seed`` placeholder value for simulation campaign.
        coords_filter_func (str): Full name(package.module.func) of a coords filter function. Should
            return True for coords values to keep. As an additional arguments will receive:
            `idx` and `{coord_name}_idx`. If not provided all coords will be kept.
        param_processors (tuple): Collection of parameter processing functions. Check some examples:
            :func:`default_ca_treatment<bbp_workflow.sci.default_ca_treatment>`
            and :func:`generate_stim_file`.
        workers (int): Parallelization level of campaign generation.
        set_progress_percentage (typing.Callable): Luigi progress callback.
        rnd (numpy.random.Generator): Random number generator.

    Returns:
        Copy of ``config`` with ``config.data`` pointing to the locations of the blue configs.
    """
    # pylint: disable=too-many-locals
    _update_progress(1, 100, set_progress_percentage)

    if coords_filter_func:
        coords_filter_func = import_func(coords_filter_func)
    else:
        # default filter accepts all coords
        coords_filter_func = lambda *_, **__: True  # pylint: disable=unnecessary-lambda-assignment

    seeds = _get_seeds(config, rnd)

    if workers and workers > 1:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            if _xr_get_multi_index(config) is None:
                futures = [
                    executor.submit(
                        _generate_sim,
                        indices["idx"],
                        loc,
                        config,
                        param_processors,
                        seeds[indices["idx"]],
                        validate_output,
                    )
                    for loc, indices, _ in _campaign_sims_with_indices(config)
                    if coords_filter_func(**loc, **indices, **config.attrs)
                ]
            else:  # coupled-coords has no filter func
                futures = [
                    executor.submit(
                        _generate_sim,
                        idx,
                        loc,
                        config,
                        param_processors,
                        seeds[idx],
                        validate_output,
                    )
                    for idx, (loc, _) in enumerate(campaign_sims(config, include_empty=True))
                ]
            total_sims = len(futures)
            for i, future in enumerate(as_completed(futures)):
                loc, path = future.result()
                config.loc[loc] = path
                _update_progress(i + 1, total_sims, set_progress_percentage)
    else:
        no_multi_index = _xr_get_multi_index(config) is None
        if no_multi_index:
            sim_indices = [
                (indices["idx"], loc)
                for loc, indices, _ in _campaign_sims_with_indices(config)
                if coords_filter_func(**loc, **indices, **config.attrs)
            ]
        else:  # coupled-coords has no filter func
            sim_indices = [
                (idx, loc) for idx, (loc, _) in enumerate(campaign_sims(config, include_empty=True))
            ]
        total_sims = len(sim_indices)
        for i, (idx, loc) in enumerate(sim_indices):
            loc, path = _generate_sim(
                idx, loc, config, param_processors, seeds[idx], validate_output
            )
            config.loc[loc] = path
            _update_progress(i + 1, total_sims, set_progress_percentage)

    path_base = Path(config.attrs["path_prefix"]) / config.name
    path_base.mkdir(parents=True, exist_ok=True)
    (path_base / "config.json").write_text(to_str(xr_to_dict(config), indent=5))  # FIXME quantity

    return config
