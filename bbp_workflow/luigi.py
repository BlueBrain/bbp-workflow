# SPDX-License-Identifier: Apache-2.0

"""Various tools augmenting luigi."""

import os
import re
import shutil
import sys
import time
from collections import ChainMap
from copy import deepcopy
from json import JSONEncoder, dumps, loads
from json.decoder import (
    _CONSTANTS,
    WHITESPACE,
    WHITESPACE_STR,
    JSONDecodeError,
    JSONDecoder,
    JSONObject,
    py_scanstring,
)
from json.scanner import NUMBER_RE
from pathlib import Path

try:
    from _json import scanstring as c_scanstring
except ImportError:
    c_scanstring = None

from luigi import DictParameter, Parameter, Task, configuration
from luigi.contrib.simulate import RunAnywayTarget as Target
from luigi.contrib.ssh import RemoteContext as RmtContext
from luigi.contrib.ssh import RemoteTarget as RmtTarget
from luigi.freezing import FrozenOrderedDict
from numpy import ndarray
from pint import Quantity
from pint import application_registry as UREG
from pint.errors import PintError


class CompleteTask(Task):
    """Complete task."""

    def complete(self):
        """"""
        return True


class Requirement:
    """Requirement specification."""

    def __init__(self, task_class, inherit_params=True, **params):
        self.task_class = task_class
        self.inherit_params = inherit_params
        self.params = params

    def __get__(self, task, cls):
        """."""
        if task is None:
            return self
        if self.inherit_params:
            return task.clone(self.task_class, **self.params)
        else:
            return self.task_class(**self.params)


class Requires:
    """Composition to replace :meth:`luigi.task.Task.requires`."""

    def __get__(self, task, _):
        """."""
        return lambda: self(task)

    def __call__(self, task):
        """Return the requirements of a task.

        Assumes the task class has :class:`Requirement` descriptors, which
        can clone the appropriate dependences from the task instance.
        """
        cls_hier_vars = ChainMap(*(dict(vars(cls)) for cls in type(task).mro())).items()
        return {k: v.__get__(task, self) for k, v in cls_hier_vars if isinstance(v, Requirement)}


class extends:
    """
    Special task inheritance with parameter look-up from parent classes config file sections.

    **Usage**:

    .. code-block:: cfg
       :caption: test.cfg

        [AnotherTask]
        a: hello

        [YetAnotherTask]
        b: world

    .. code-block:: python
       :caption: test.py

        class AnotherTask(luigi.Task):
            a = luigi.Parameter()

        class YetAnotherTask(luigi.Task):
            b = luigi.Parameter()

        @extends(AnotherTask, YetAnotherTask)
        class MyTask(luigi.Task):

            def run(self):
               print(self.a) # this will be defined
               print(self.b) # this will be defined
               # ...
    """

    def __init__(self, *tasks_to_extend):
        """Init."""
        super().__init__()
        if not tasks_to_extend:
            raise TypeError("tasks_to_extend cannot be empty")

        self.tasks_to_extend = tasks_to_extend

    def __call__(self, task_that_extends):
        """Call."""
        # Get all parameter objects from each of the underlying tasks
        for task_to_extend in self.tasks_to_extend:
            for param_name, param_obj in task_to_extend.get_params():
                # pylint: disable=protected-access
                new_param_obj = deepcopy(param_obj)
                new_param_obj._config_path = {
                    "section": task_to_extend.__name__,
                    "name": param_name,
                }
                setattr(task_that_extends, param_name, new_param_obj)
        return task_that_extends


class inherits:  # FIXME copy/paste from luigi with added deepcopy
    """
    Task inheritance.

    *New after Luigi 2.7.6:* multiple arguments support.

    **Usage**:

    .. code-block:: python

        class AnotherTask(luigi.Task):
            m = luigi.IntParameter()

        class YetAnotherTask(luigi.Task):
            n = luigi.IntParameter()

        @inherits(AnotherTask)
        class MyFirstTask(luigi.Task):
            def requires(self):
               return self.clone_parent()

            def run(self):
               print(self.m) # this will be defined
               # ...

        @inherits(AnotherTask, YetAnotherTask)
        class MySecondTask(luigi.Task):
            def requires(self):
               return self.clone_parents()

            def run(self):
               print(self.n) # this will be defined
               # ...
    """

    def __init__(self, *tasks_to_inherit):
        """Init."""
        super().__init__()
        if not tasks_to_inherit:
            raise TypeError("tasks_to_inherit cannot be empty")

        self.tasks_to_inherit = tasks_to_inherit

    def __call__(self, task_that_inherits):
        """Call."""
        # Get all parameter objects from each of the underlying tasks
        for task_to_inherit in self.tasks_to_inherit:
            for param_name, param_obj in task_to_inherit.get_params():
                # Check if the parameter exists in the inheriting task
                if not hasattr(task_that_inherits, param_name):
                    # If not, add it to the inheriting task
                    setattr(task_that_inherits, param_name, deepcopy(param_obj))  # FIXME deepcopy

        # Modify task_that_inherits by adding methods
        def clone_parent(_self, **kwargs):
            return _self.clone(cls=self.tasks_to_inherit[0], **kwargs)

        task_that_inherits.clone_parent = clone_parent

        def clone_parents(_self, **kwargs):
            return [
                _self.clone(cls=task_to_inherit, **kwargs)
                for task_to_inherit in self.tasks_to_inherit
            ]

        task_that_inherits.clone_parents = clone_parents

        return task_that_inherits


class RunAnywayTarget(Target):
    """Does the same as the base class and ignores permission exception on file cleanup."""

    temp_time = 10 * 24 * 3600  # 10 days in seconds

    def __init__(self, task_obj):  # pylint: disable=super-init-not-called
        self.task_id = task_obj.task_id

        if self.unique.value == 0:
            with self.unique.get_lock():
                if self.unique.value == 0:
                    self.unique.value = os.getpid()

        # Deleting old files > temp_time
        if os.path.isdir(self.temp_dir):
            limit = time.time() - self.temp_time
            for fn in os.listdir(self.temp_dir):
                path = os.path.join(self.temp_dir, fn)
                if os.path.isdir(path) and os.stat(path).st_mtime < limit:
                    try:
                        shutil.rmtree(path)
                    except PermissionError:
                        pass

    def done(self, value=None):
        """Create temporary file to mark the task as `done`."""
        # pylint: disable=arguments-differ
        path = Path(self.get_path())
        path.parent.mkdir(parents=True, exist_ok=True)
        if value:
            path.write_text(value, encoding="utf8")
        else:
            path.touch()

    def get_data(self):
        """Return the content of the output path."""
        path = Path(self.get_path())
        if path.exists():
            return path.read_text(encoding="utf8")
        return None


def _is_experimental_units():
    return configuration.get_config().getboolean("DEFAULT", "experimental-units", False)


class QuantityParameter(Parameter):
    """Parameter whose value is a quantity."""

    def parse(self, x):
        """Parse a quantity from the string."""
        value = UREG(x)
        if not _is_experimental_units() and isinstance(value, Quantity):
            return value.magnitude
        return value


class OptionalQuantityParameter(QuantityParameter):
    """Optional parameter whose value is a quantity."""

    def serialize(self, x):
        """Serialize the given value if the value is not None else return an empty string."""
        if x is None:
            return ""
        else:
            return super().serialize(x)

    def parse(self, x):
        """Parse the given value if it is a string (empty strings are parsed to None)."""
        if not isinstance(x, str):
            return x
        elif x:
            return super().parse(x)
        else:
            return None

    def normalize(self, x):
        """Normalize the given value if it is not None."""
        if x is None:
            return None
        return super().normalize(x)


def _skip_whitespace(s, end, nextchar):
    if nextchar in WHITESPACE_STR:
        end = WHITESPACE.match(s, end + 1).end()
        nextchar = s[end : end + 1]
    return nextchar, end


SEP_RE = re.compile(r"[,\]}]")


def _find_sep(s, end):
    m = SEP_RE.search(s, end)
    if m:
        return m.start()
    else:
        return -1


def _JSONArray(s_and_end, scan_once, _w=WHITESPACE.match, _ws=WHITESPACE_STR):
    s, end = s_and_end
    values = []
    nextchar = s[end : end + 1]
    nextchar, end = _skip_whitespace(s, end, nextchar)
    # Look-ahead for trivial empty array
    if nextchar == "]":
        return values, end + 1
    _append = values.append
    while True:
        try:
            value, end = scan_once(s, end)
        except StopIteration as err:
            raise JSONDecodeError("Expecting value", s, err.value) from None
        _append(value)
        nextchar = s[end : end + 1]
        nextchar, end = _skip_whitespace(s, end, nextchar)
        end += 1
        if nextchar == "]":
            sep_idx = _find_sep(s, end)
            if sep_idx != -1:
                try:
                    return UREG.Quantity(values, s[end:sep_idx]), sep_idx
                except PintError as ex:
                    raise JSONDecodeError("Expecting valid Units", s, end) from ex
            else:
                break
        elif nextchar != ",":
            raise JSONDecodeError("Expecting ',' delimiter", s, end - 1)
        try:
            if s[end] in _ws:
                end += 1
                if s[end] in _ws:
                    end = _w(s, end + 1).end()
        except IndexError:
            pass

    return values, end


scanstring = c_scanstring or py_scanstring


class _FrozenOrderedDict(FrozenOrderedDict):

    def __init__(self, pairs):
        """If units are not enabled extract just magnitudes and drop units."""
        if not _is_experimental_units():
            pairs = [
                (
                    key,
                    (
                        (
                            value.magnitude.tolist()
                            if isinstance(value.magnitude, ndarray)
                            else value.magnitude
                        )
                        if isinstance(value, Quantity)
                        else value
                    ),
                )
                for key, value in pairs
            ]
        super().__init__(pairs)


def _py_make_scanner(context):
    parse_object = context.parse_object
    parse_array = context.parse_array
    parse_string = context.parse_string
    match_number = NUMBER_RE.match
    strict = context.strict
    parse_float = context.parse_float
    parse_int = context.parse_int
    parse_constant = context.parse_constant
    object_hook = context.object_hook
    object_pairs_hook = context.object_pairs_hook
    memo = context.memo

    def _scan_once(string, idx):  # pylint: disable=too-many-branches,too-many-return-statements
        try:
            nextchar = string[idx]
        except IndexError:
            raise StopIteration(idx) from None

        if nextchar == '"':
            return parse_string(string, idx + 1, strict)
        elif nextchar == "{":
            return parse_object(
                (string, idx + 1), strict, _scan_once, object_hook, object_pairs_hook, memo
            )
        elif nextchar == "[":
            return parse_array((string, idx + 1), _scan_once)
        elif nextchar == "n" and string[idx : idx + 4] == "null":
            return None, idx + 4
        elif nextchar == "t" and string[idx : idx + 4] == "true":
            return True, idx + 4
        elif nextchar == "f" and string[idx : idx + 5] == "false":
            return False, idx + 5

        m = match_number(string, idx)
        if m is not None:
            integer, frac, exp = m.groups()
            if frac or exp:
                res = parse_float(integer + (frac or "") + (exp or ""))
            else:
                res = parse_int(integer)
            sep_idx = _find_sep(string, m.end())
            if sep_idx != -1:
                try:
                    return UREG.Quantity(res, string[m.end() : sep_idx]), sep_idx
                except (PintError, AttributeError) as ex:
                    raise JSONDecodeError("Expecting valid Units", string, m.end()) from ex
            return res, m.end()
        elif nextchar == "N" and string[idx : idx + 3] == "NaN":
            return parse_constant("NaN"), idx + 3
        elif nextchar == "I" and string[idx : idx + 8] == "Infinity":
            return parse_constant("Infinity"), idx + 8
        elif nextchar == "-" and string[idx : idx + 9] == "-Infinity":
            return parse_constant("-Infinity"), idx + 9
        else:
            raise StopIteration(idx)

    def scan_once(string, idx):
        try:
            return _scan_once(string, idx)
        finally:
            memo.clear()

    return scan_once


class Decoder(JSONDecoder):
    """."""

    def __init__(self, *, object_pairs_hook=None, parse_constant=None, strict=True):
        # pylint: disable=super-init-not-called
        """"""
        self.object_hook = None
        self.parse_float = float
        self.parse_int = int
        self.parse_constant = parse_constant or _CONSTANTS.__getitem__
        self.strict = strict
        self.object_pairs_hook = object_pairs_hook
        self.parse_object = JSONObject
        self.parse_array = _JSONArray
        self.parse_string = scanstring
        self.memo = {}
        self.scan_once = _py_make_scanner(self)

    def raw_decode(self, s, idx=0):
        """"""
        try:
            obj, end = self.scan_once(s, idx)
        except StopIteration as err:
            raise JSONDecodeError("Expecting value", s, err.value) from None
        return obj, end


class _QuantityDictParamEncoder(JSONEncoder):

    def default(self, o):
        """"""
        if isinstance(o, FrozenOrderedDict):
            return o.get_wrapped()
        elif isinstance(o, Quantity):
            if isinstance(o.magnitude, ndarray):
                return (o.magnitude.tolist(), str(o.units))
            else:
                return (o.magnitude, str(o.units))
        return JSONEncoder.default(self, o)


class QuantityDictParameter(DictParameter):
    """Parameter whose value is a quantity."""

    def parse(self, source):
        """Parse."""
        # TOML based config convert params to python types itself.
        try:
            if not isinstance(source, str):
                return source
            return loads(source, object_pairs_hook=_FrozenOrderedDict, cls=Decoder)
        except JSONDecodeError as e:
            print(">>>ERROR parsing json parameter:", file=sys.stderr, flush=True)
            for i, s in enumerate(e.doc.splitlines()):
                print(s, file=sys.stderr, flush=True)
                if i + 1 == e.lineno:
                    print(
                        " " * (e.colno - 1) + "^ " + e.msg + " ERROR<<<",
                        file=sys.stderr,
                        flush=True,
                    )
            raise

    def serialize(self, x):
        return dumps(x, cls=_QuantityDictParamEncoder)


class RemoteTarget(RmtTarget):
    """No host key check target."""

    def __init__(self, *args, **kwargs):
        kwargs["no_host_key_check"] = True
        super().__init__(*args, **kwargs)


class RemoteContext(RmtContext):
    """No host key check context."""

    def __init__(self, *args, **kwargs):
        kwargs["no_host_key_check"] = True
        super().__init__(*args, **kwargs)
