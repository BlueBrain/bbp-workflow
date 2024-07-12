# SPDX-License-Identifier: Apache-2.0

"""Workflow Custom Parameters."""

import luigi


class CWLModelParameter(luigi.Parameter):
    """CWL Model Parameter."""

    def __init__(self, model, *args, **kwargs):
        self._model = model
        super().__init__(*args, **kwargs)

    def normalize(self, x):
        """Normalize the given value to a dataclass initialized object."""
        if isinstance(x, self._model):
            return x
        return self._model.from_string(x)

    def serialize(self, x):
        """Serialize a dataclass object."""
        return x.to_string()
