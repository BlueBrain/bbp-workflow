# SPDX-License-Identifier: Apache-2.0

"""Utils."""

import io
import json
import mimetypes
import os
import re
import zipfile
from collections.abc import Mapping
from contextlib import closing, contextmanager
from importlib import import_module
from ipaddress import ip_address
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import numpy as np
import xarray as xr
from luigi import BoolParameter
from luigi.parameter import _no_value as PARAM_NO_VALUE
from pint import Quantity

from bbp_workflow.settings import DATA_PREFIX, ENVIRONMENT, HTTPS_PROXY, SIF_PREFIX


def fix_workflows_path(path):  # FIXME drop once everyone removes "workflows/" prefix
    """Remove "workflows/" prefix from path."""
    if path:
        path = Path(path)
        if str(path.parent) == "workflows":
            return str(path.name)
    return path


def param_repr(description, default):
    """Produce parameter help string representation for sphinx-doc."""
    if description:
        help_str = description
    else:
        help_str = ""
    if default is not PARAM_NO_VALUE:
        help_str += f"({default})"
    return help_str


def params_from_cfg(task_cls, cfg_section, default_section):
    """Lookup params from cfg with defaults."""
    params = {}
    for param_name_, _ in task_cls.get_params():
        param_name = param_name_.replace("_", "-")  # correct for underscore
        if param_name in cfg_section:
            params[param_name_] = cfg_section[param_name]
        elif param_name_ in cfg_section:  # try name with underscore anyway
            params[param_name_] = cfg_section[param_name_]
        elif param_name in default_section:
            params[param_name_] = default_section[param_name]
        elif param_name_ in default_section:
            params[param_name_] = default_section[param_name_]
    return task_cls(**params)


def _ignore_slurm_config_key(key):
    """List of slurm config keys which should be ignored."""
    return key in ["env"]


def map_slurm_params(conf, skip=None):
    """Map params to strings.

    Args:
        conf (SlurmCfg): Slurm configuration.
    """
    rename = {"job_output": "output", "job_array": "array"}
    params = []
    if conf is None:
        return params
    for key, param in conf.get_params():
        if skip and key in skip:
            continue
        val = getattr(conf, key)
        if val:
            key = rename.get(key, key)
            key = key.replace("_", "-")
            if isinstance(param, BoolParameter):
                params.append(f"--{key}")
            else:
                params.append(f"--{key}={val}")

    return params


def to_sbatch_params(conf):
    """Sbatch param helper.

    Converts {key: value} to string #SBATCH --key=val if val is not None
    also replace _ to - in key.
    """
    return "\n".join([f"#SBATCH {param}" for param in map_slurm_params(conf)])


def to_srun_params(conf, job_id=None):
    """Srun params helper.

    Converts {key: value} to string --key=val if val is not None
    also replace _ to - in key.
    """
    params = map_slurm_params(conf, skip=["wait"])
    if job_id:
        params.append(f"--jobid={job_id}")
    return " ".join(params)


def kg_env_exports(kg_env=None):
    """Take nexus relevant env var and produce export statements."""
    ret = []
    if kg_env:
        for k, v in kg_env.items():
            ret.append(f'export {k}="{v}"')
    else:
        for env_var in (
            "NEXUS_TOKEN",
            "NEXUS_WORKFLOW",
            "NEXUS_BASE",
            "NEXUS_USERINFO",
            "NEXUS_ORG",
            "NEXUS_PROJ",
            "NEXUS_NO_PROV",
            "NEXUS_DRY_RUN",
            "KC_SCR",
        ):
            if env_var in os.environ:
                ret.append(f'export {env_var}="{os.environ[env_var]}"')
    return ret


def cmd_join(cmd_args):
    """Take space/new_line separated cmd args and produce one liner."""
    if cmd_args:
        return " ".join([var.strip() for var in re.split(r" |\n", cmd_args) if var.strip()])
    else:
        return ""


def env_exports(env):
    """Take comma separated env vars and produce array of export statements."""
    if env:
        return [f"export {var.strip()}" for var in re.split(r",|\n", env) if var.strip()]
    else:
        return []


def to_env_commands(env_cfg, kg_env=None, python_path=None):
    """Make shell commands out of EnvCfg.

    Args:
        env_cfg (EnvCfg): Environment configuration params.
    Returns:
        list: [modulepath export, module load command, env exports].
    """
    result = []
    if env_cfg is None:
        return result

    if env_cfg.modules:
        if env_cfg.module_path:
            result.append(f"export MODULEPATH={env_cfg.module_path}:$MODULEPATH")

        if env_cfg.module_archive:
            result.append(f"module load {env_cfg.module_archive}")
        else:
            result.append("module load unstable")

        result.append(f"module load {env_cfg.modules}")

    result.extend(kg_env_exports(kg_env))

    if env_cfg.enable_internet:
        result.append(f"export https_proxy={HTTPS_PROXY}")

    if env_cfg.virtual_env:
        result.append(f"source {env_cfg.virtual_env}/bin/activate")

    if python_path:
        result.append(f'export PYTHONPATH="{python_path}":$PYTHONPATH')

    if env_cfg.env:
        result.extend(env_exports(env_cfg.env))

    return result


def make_web_link(base, org, proj, resource_id):
    """Make link to nexus web from the resource id."""
    if base and "staging" in str(base):
        prefix = "https://staging.nise.bbp.epfl.ch/nexus"
    else:
        prefix = "https://bbp.epfl.ch/nexus/web"

    if org is None:
        org = "bbp"

    resource_id = quote(resource_id, safe="")

    return f"{prefix}/{org}/{proj}/resources/{resource_id}"


def _fix_bbp_link(url):
    """Fix url by adding ``.bbp.epfl.ch`` suffix if not present."""
    split = urlsplit(url)
    try:
        ip_address(split.hostname)
        # got ip address => pass it as is
        return url
    except ValueError:
        # got host name => add suffix
        suffix = ""
        if ".bbp.epfl.ch" not in split.hostname:
            suffix += ".bbp.epfl.ch"
        if split.port:
            suffix += f":{split.port}"

        return urlunsplit(split._replace(netloc=split.hostname + suffix))


class _Encoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, np.integer):
            return int(o)
        elif isinstance(o, np.floating):
            return float(o)
        elif isinstance(o, np.ndarray):
            return o.tolist()
        elif isinstance(o, Quantity):
            if isinstance(o.magnitude, np.ndarray):
                return o.magnitude.tolist()
            else:
                return o.magnitude
        else:
            return super().default(o)


def xr_from_dict(array_dict):
    """Note: deprecated, use bbp_workflow.simulation.util.xr_from_dict instead."""
    array = xr.DataArray.from_dict(array_dict)
    # check if it is MultiIndex
    if len(array_dict["dims"]) == 1:
        index_name_tuple = array_dict["dims"]
        if all(v["dims"] == index_name_tuple for _, v in array_dict["coords"].items()):
            array = array.set_index(**{index_name_tuple[0]: list(array_dict["coords"].keys())})
    return array


def to_str(dictionary, **kwargs):
    """Serialize dictionary to string."""
    return json.dumps(dictionary, cls=_Encoder, **kwargs)


def deep_update(target, update_with):
    """Deep dictionary update."""
    for k, v in update_with.items():
        if isinstance(v, Mapping):
            target[k] = deep_update(target.get(k, {}), v)
        else:
            target[k] = v
    return target


def unfreeze(value):
    """Recursively walks ``Mapping``s and ``list``s and converts them to ``dict`` and ``tuples``."""
    if isinstance(value, Mapping):
        return dict(((k, unfreeze(v)) for k, v in value.items()))
    elif isinstance(value, (list, tuple)):
        return list(unfreeze(v) for v in value)
    return value


def _match_notebook_url(log_line):
    match = re.match(r".+\s(https?://\w+:\d+/?\?token=\w+)$", log_line)
    if match:
        return match.group(1)
    return None


def import_func(func_fq_name):
    """Import function from fully qualified string name."""
    mod_name, func_name = func_fq_name.rsplit(".", 1)
    mod = import_module(mod_name)
    func = getattr(mod, func_name)
    return func


def ood_url(path):
    """Return URL to the path accessible through OpenOnDemand."""
    try:
        relative = path.relative_to("/gpfs/bbp.cscs.ch/project")
        project = relative.parts[0]
        relative = relative.relative_to(project)
        if relative.parts[0] == "scratch":
            relative = relative.relative_to("scratch")
            path = Path("/gpfs/bbp.cscs.ch/data/scratch") / project / relative
        else:
            path = Path("/gpfs/bbp.cscs.ch/data/project") / project / relative
    except ValueError:
        try:
            relative = path.relative_to("/gpfs/bbp.cscs.ch/home")
            path = Path("/gpfs/bbp.cscs.ch/ssd/home") / relative
        except ValueError:
            pass
    return f"https://ood.bbp.epfl.ch/pun/sys/files/api/v1/fs{path}"


def job_log_url(job_log_path, chdir, job_id):
    """Create link to the slurm log file accessible through OpenOnDemand.

    In case job_log_path is None, assume standard log naming and location at chdir.
    """
    if job_log_path:
        path = Path(job_log_path)
    else:
        if not chdir:
            return ""
        path = Path(chdir) / f"slurm-{job_id}.out"
    # based on user request print tail command for convenience
    print(f"\n  TO FOLLOW:\n  tail -f {path}\n", flush=True)
    return ood_url(path)


def metadata_path(output_path, chdir=None):
    """Return (path, name) to metadata file based on task output path and cwd."""
    output_path = Path(output_path)
    if not chdir or output_path == Path(chdir):
        return (str(output_path.parent), str(output_path.name))

    try:
        relative = str(output_path.relative_to(chdir))
        return (chdir, relative.replace("/", "--"))
    except ValueError:
        return (str(output_path.parent), str(output_path.name))


@contextmanager
def _make_zip(path):
    """Zip files at path and return an in-memory bytes buffer.

    Returns:
        In-memory bytes buffer containing the zip archive.
    """
    path = path.resolve()
    with closing(io.BytesIO()) as buf:
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
            for archive_dir_path, _, file_names in os.walk(path):
                for file_name in file_names:
                    file_path = Path(archive_dir_path) / file_name
                    z.write(file_path, file_path.relative_to(path))
        buf.seek(0)
        yield buf


def _get_content_type(path):
    """Return the guessed content-type for the given path."""
    content_type, _ = mimetypes.guess_type(path)
    return content_type or "application/octet-stream"


def _render_template(template, **kwargs):
    """Render and return the template after substituting the given kwargs.

    Example:
        >>> _render_template("Hello $key!", key="world")
        'Hello world!'
    """
    # consider using cheetah3 or jinja2 for more complex substitutions
    pattern = r"(\\\$)|\$([a-z][a-z0-9_]*)"  # match \$ or $key
    return re.sub(
        pattern,
        lambda k: "$" if k.group(1) else str(kwargs[k.group(2)]),
        template,
        flags=re.IGNORECASE,
    )


def singularity_run(container, cmd, pwd="$(pwd)"):
    """Build singularity run command."""
    return (
        f"singularity run --no-eval -B {DATA_PREFIX}:{DATA_PREFIX}:ro "
        f"-B {pwd} --pwd {pwd} {SIF_PREFIX}/{container} eval '{cmd}'"
    )


def amend_cmd(container, container_cmd, default_cmd=None):
    """Amend with singularity if in cloud env else default or treat container_cmd as normal."""
    if default_cmd is not None:
        cmd = default_cmd
    else:
        cmd = container_cmd
    if ENVIRONMENT in {"aws", "bbp"}:
        cmd = singularity_run(container, container_cmd)
    return cmd
