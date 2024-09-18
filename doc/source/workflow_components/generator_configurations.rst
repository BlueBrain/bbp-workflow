Generator Configurations
========================

.. _cell_composition_config:

CellCompositionConfig
---------------------

.. code-block:: json

   {
      "http://api.brain-map.org/api/v2/data/Structure/997": {
        "variantDefinition": {
          "algorithm": "cell_composition_manipulation",
          "version": "v3"
        },
        "inputs": [
          {
            "name": "base_cell_composition_id",
            "type": "Dataset",
            "id": "https://bbp.epfl.ch/neurosciencegraph/data/cellcompositions/54818e46-cf8c-4bd6-9b68-34dffbc8a68c?tag=v1.1.0"
          }
        ],
        "configuration": {
          "version": 1,
          "unitCode": {
            "density": "mm^-3"
          },
          "overrides": {}
        }
      }
    }

To add an override:

.. code-block:: json

    {
      "overrides": {
        "http://api.brain-map.org/api/v2/data/Structure/23": {
          "label": "Anterior amygdalar area",
          "about": "BrainRegion",
          "hasPart": {
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronMType?rev=6": {
              "label": "GEN_mtype",
              "about": "MType",
              "hasPart": {
                "https://bbp.epfl.ch/ontologies/core/bmo/GenericExcitatoryNeuronEType?rev=6": {
                  "label": "GEN_etype",
                  "about": "EType",
                  "composition": {
                    "neuron": {
                      "density": 1200
                    }
                  }
                }
              }
            }
          }
        }
      }
    }

For the variant definition of the manipulation algorithm refer to `blue-cwl documentation <https://blue-cwl.readthedocs.io/en/latest/registry.html#cell-composition-cell-composition-manipulation-v3>`__.


.. _cell_position_config:

CellPositionConfig
------------------

The ``CellPositionConfig`` has the following layout:

.. code-block:: json

   {
      "http://api.brain-map.org/api/v2/data/Structure/997": {
        "variantDefinition": {
          "algorithm": "neurons_cell_position",
          "version": "v3"
        },
        "inputs": [],
        "configuration": {
          "place_cells": {
            "soma_placement": "basic",
            "density_factor": 1,
            "sort_by": [
              "region",
              "mtype"
            ],
            "seed": 0,
            "mini_frequencies": false
          }
        }
      }
    }


.. _mmodel_config:

MorphologyAssignmentConfig
--------------------------

.. code-block:: json

    {
      "variantDefinition": {
        "topological_synthesis": {
          "algorithm": "topological_synthesis",
          "version": "v3"
        },
        "placeholder_assignment": {
          "algorithm": "placeholder_assignment",
          "version": "v3"
        }
      },
      "defaults": {
        "topological_synthesis": {
          "@id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/fae6eb46-3007-41c6-af69-941a82aada68",
          "@type": "CanonicalMorphologyModelConfig"
        },
        "placeholder_assignment": {
          "@id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/9503a07d-8337-48eb-8637-acc26b0f13bf",
          "@type": "PlaceholderMorphologyConfig"
        }
      },
      "configuration": {
        "topological_synthesis": {
          "http://api.brain-map.org/api/v2/data/Structure/23": {
            "https://bbp.epfl.ch/ontologies/core/bmo/GenericInhibitoryNeuronMType": {}
          }
        }
      }
    }

For the variant definition of the placement algorithm refer to `blue-cwl documentation <https://blue-cwl.readthedocs.io/en/latest/registry.html#mmodel-neurons-mmodel-v3>`__

.. _me_model_config:

MEModelConfig
-------------

.. code-block:: json

    {
      "variantDefinition": {
        "neurons_me_model": {
          "algorithm": "neurons_me_model",
          "version": "v3"
        }
      },
      "defaults": {
        "neurons_me_model": {
          "@id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/2ec96e9f-7254-44b5-bbcb-fdea3e18f110",
          "@type": [
            "PlaceholderEModelConfig",
            "Entity"
          ]
        }
      },
      "overrides": {
        "neurons_me_model": {}
      }
    }


For the variant definition of the placement algorithm refer to `blue-cwl documentation <https://blue-cwl.readthedocs.io/en/latest/registry.html#memodel-neurons-memodel-v3>`__


.. _macro_config:

MacroConnetomeConfig
--------------------

.. code-block::

    {
      "initial": {
        "connection_strength": {
          "id": "https://bbp.epfl.ch/neurosciencegraph/data/connectomestrength/8e285d4b-4d09-4357-98ae-9e9fc61face6",
          "type": [
            "Entity",
            "Dataset",
            "WholeBrainConnectomeStrength"
          ],
          "rev": 10
        }
      },
      "overrides": {
        "connection_strength": {
          "id": "https://bbp.epfl.ch/neurosciencegraph/data/wholebrainconnectomestrengths/9357f9b4-8e94-45cd-b701-8d18648a17a6",
          "type": [
            "Entity",
            "Dataset",
            "WholeBrainConnectomeStrength"
          ],
          "rev": 1
        }
      },
      "_ui_data": {
        "editHistory": []
      }
    }


.. _micro_config:

MicroConnectomeConfig
---------------------

The ``MicroConnectomeConfig`` has the following layout:

.. code-block::

   {
      "variants": {
        "placeholder__erdos_renyi": {
          "algorithm": "placeholder",
          "version": "v3",
          "params": {
            "weight": {
              "type": "float32",
              "unitCode": "#synapses/connection",
              "default": 0
            },
            "nsynconn_mean": {
              "type": "float32",
              "unitCode": "#synapses/connection",
              "default": 3
            },
            "nsynconn_std": {
              "type": "float32",
              "unitCode": "#synapses/connection",
              "default": 1.5
            },
            "delay_velocity": {
              "type": "float32",
              "unitCode": "um/ms",
              "default": 250
            },
            "delay_offset": {
              "type": "float32",
              "unitCode": "ms",
              "default": 0.8
            }
          }
        },
        "placeholder__distance_dependent": {
          "algorithm": "placeholder",
          "version": "v3",
          "params": {
            "weight": {
              "type": "float32",
              "unitCode": "#synapses/connection",
              "default": 0
            },
            "exponent": {
              "type": "float32",
              "unitCode": "1/um",
              "default": 0.008
            },
            "nsynconn_mean": {
              "type": "float32",
              "unitCode": "#synapses/connection",
              "default": 3
            },
            "nsynconn_std": {
              "type": "float32",
              "unitCode": "#synapses/connection",
              "default": 1.5
            },
            "delay_velocity": {
              "type": "float32",
              "unitCode": "um/ms",
              "default": 250
            },
            "delay_offset": {
              "type": "float32",
              "unitCode": "ms",
              "default": 0.8
            }
          }
        }
      },
      "initial": {
        "variants": {
          "id": "https://bbp.epfl.ch/neurosciencegraph/data/a46a442c-5baa-4a5c-9907-bfb359dd9e5d",
          "rev": 9,
          "type": [
            "Entity",
            "Dataset",
            "MicroConnectomeVariantSelection"
          ]
        },
        "placeholder__erdos_renyi": {
          "id": "https://bbp.epfl.ch/neurosciencegraph/data/microconnectomedata/009413eb-e51b-40bc-9199-8b98bfc53f87",
          "rev": 7,
          "type": [
            "Entity",
            "Dataset",
            "MicroConnectomeData"
          ]
        },
        "placeholder__distance_dependent": {
          "id": "https://bbp.epfl.ch/neurosciencegraph/data/microconnectomedata/c7e1d215-2dad-4216-8565-6b1e4c161f46",
          "rev": 7,
          "type": [
            "Entity",
            "Dataset",
            "MicroConnectomeData"
          ]
        }
      },
      "overrides": {
        "variants": {
          "id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/deee5e86-1d7b-45f6-8fad-259a71c35c6a",
          "type": [
            "Entity",
            "Dataset",
            "MicroConnectomeVariantSelectionOverrides"
          ],
          "rev": 1
        },
        "placeholder__erdos_renyi": {
          "id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/36426136-201d-4dfd-93d9-b541e113a6bf",
          "type": [
            "Entity",
            "Dataset",
            "MicroConnectomeDataOverrides"
          ],
          "rev": 1
        },
        "placeholder__distance_dependent": {
          "id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/4bb03c2b-b99d-4a5d-8a8b-12e1a30619aa",
          "type": [
            "Entity",
            "Dataset",
            "MicroConnectomeDataOverrides"
          ],
          "rev": 1
        }
      },
      "_ui_data": {
        "editHistory": []
      }
    }


.. _synapse_config:

SynapseConfig
-------------

.. code-block:: json

    {
      "variantDefinition": {
        "algorithm": "synapses",
        "version": "v2"
      },
      "defaults": {
        "synapse_properties": {
          "id": "https://bbp.epfl.ch/neurosciencegraph/data/synapticassignment/d57536aa-d576-4b3b-a89b-b7888f24eb21",
          "type": [
            "Dataset",
            "SynapticParameterAssignment"
          ],
          "rev": 9
        },
        "synapses_classification": {
          "id": "https://bbp.epfl.ch/neurosciencegraph/data/synapticparameters/cf25c2bf-e6e4-4367-acd8-94004bfcfe49",
          "type": [
            "Dataset",
            "SynapticParameter"
          ],
          "rev": 6
        }
      },
      "configuration": {
        "synapse_properties": {
          "id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/839a8b83-1620-4fe7-8f58-658ded0ea1e8",
          "type": [
            "Dataset",
            "SynapticParameterAssignment"
          ],
          "rev": 1
        },
        "synapses_classification": {
          "id": "https://bbp.epfl.ch/data/bbp/mmb-point-neuron-framework-model/d133e408-bd00-41ca-9334-e5fab779ad99",
          "type": [
            "Dataset",
            "SynapticParameter"
          ],
          "rev": 3
        }
      }
    }

For the variant definition of the placement algorithm refer to `blue-cwl documentation <https://blue-cwl.readthedocs.io/en/latest/registry.html#connectome-filtering-synapses-v21>`__
