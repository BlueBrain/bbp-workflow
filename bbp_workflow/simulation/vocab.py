# SPDX-License-Identifier: Apache-2.0

"""Vocabulary of sim terms with corresponding units."""

from pint import application_registry as UREG

VOCAB_PREFIX_UNITS = {
    "g_": UREG.nS,
    "tau_": UREG.ms,
}

VOCAB_UNITS = {
    "amp_end": UREG.nA,
    "amp_start": UREG.nA,
    "amp_var": UREG.nA**2,
    "celsius": UREG.degC,
    "delay": UREG.ms,
    "decay_time": UREG.ms,
    "dt": UREG.ms,
    "duration": UREG.ms,
    "end_time": UREG.ms,
    "extracellular_calcium": UREG.mM,
    "frequency": UREG.Hz,
    "mean": UREG.nA,  # this looks wrong but currently(in sonata spec) it is the only mean
    "rate": UREG.Hz,
    "rise_time": UREG.ms,
    "spike_threshold": UREG.mV,
    "synapse_delay_override": UREG.ms,
    "start_time": UREG.ms,
    "tstop": UREG.ms,
    "v_init": UREG.mV,
    "voltage": UREG.mV,
    "width": UREG.ms,
}


def get_units(term):
    """Get units of the term from vocab."""
    units = VOCAB_UNITS.get(term)
    if units is None:
        for key, value in VOCAB_PREFIX_UNITS.items():
            if term.startswith(key):
                return value
    return units
