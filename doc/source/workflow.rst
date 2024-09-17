The Building Workflow
=====================

Quickstart
----------

Launching a building workflow can be achieved with the following command:

.. code-block:: shell

   ./bbp-workflow-launch.sh bbp_workflow.generation.workflow.SBOWorkflow \
        --config-url $CONFIG_URL \
        --output-dir $OUTPUT_DIR \
        --host bbpv1.epfl.ch \
        --kg-base $NEXUS_BASE \
        --kg-org $NEXUS_ORG \
        --account $ACCOUNT \
        --workers 1

Or respectively from the bbp-workflow server:

.. code-block:: shell

    bbp-workflow launch --config $LUIGI_CFG -f bbp_workflow.generation SBOWorkflow \
            config-url=$CONFIG_URL \
            target=cellPositionConfig \
            output-dir=$OUTPUT_DIR \
            host=$HOST \
            account=$ACCOUNT \

where $LUIGI_CFG is structured as follows:

.. code-block:: text

    [DEFAULT]
    workers: 1
    kg-base: https://staging.nise.bbp.epfl.ch/nexus/v1
    kg-org: bbp
    kg-proj: mmb-point-neuron-framework-model

    [SBOWorkflow]
    kg-base: https://staging.nise.bbp.epfl.ch/nexus/v1
    kg-org: bbp
    kg-proj: mmb-point-neuron-framework-model


.. _workflow:

Workflow
--------

The :class:`Workflow <bbp_workflow.generation.workflow.SBOWorkflow>` task receives a :ref:`ModelBuildingConfig <model_config>` resource id and a target task name.

The target task will trigger the execution of all its required upstream tasks, ensuring that each necessary dependency is processed in sequence until the target task is reached and executed.

For example, if cellPositionConfig is selected as the target task, Luigi will first process the ``CellPositionGenerator`` requirements, which directly depends on ``CellCompositionGenerator``. Therefore ``CellCompositionGenerator`` will be executed before ``CellPositionGenerator`` can run.


.. _model_config:

ModelBuildingConfig
-------------------

The ModelBuildingConfig incorporates all the generator configs as follows:


.. graphviz::

   digraph foo{

    rankdir = "LR"
    splines = "ortho"

    ModelBuildingConfig[
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    CellCompositionConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        width = 3
    ]

    CellPositionConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        width = 3
    ]

    MorphologyAssignmentConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        width = 3
    ]

    MEModelConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        width = 3
    ]

    MacroConnectomeConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        width = 3
    ]

    MicroConnectomeConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        width = 3
    ]

    SynapseConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        width = 3
    ]

    ModelBuildingConfig -> CellCompositionConfig [label="configs[cellCompositionConfig]", labelheight=2];
    ModelBuildingConfig -> CellPositionConfig [label="configs[cellPositionConfig]"];
    ModelBuildingConfig -> MorphologyAssignmentConfig [label="configs[morphologyAssignmentConfig]"];
    ModelBuildingConfig -> MEModelConfig [label="configs[meModelConfig]"];
    ModelBuildingConfig -> MacroConnectomeConfig [label="configs[macroConnectomeConfig]"];
    ModelBuildingConfig -> MicroConnectomeConfig [label="configs[microConnectomeConfig]"];
    ModelBuildingConfig -> SynapseConfig [label="configs[synapseConfig]"];


   }


.. note::

   It is not necessary for the config to include all the generator configs, however it must contain all configs up until the target task selected when executing the :ref:`workflow <workflow>`.


.. _generator_layout:

Generator: Task Layout
----------------------

A Generator is a Luigi Task in a predefined hierarchy of :ref:`generators <generators>` which has NEXUS resources as inputs and outputs.

.. graphviz::


   digraph generator_layout {

    rankdir = "LR"

    ModelBuildingConfig [
      shape = Mrecord style = filled fillcolor = lemonchiffon
      width = 2
    ]
    GeneratorConfig [
      shape = Mrecord style = filled fillcolor = lemonchiffon
      width = 2
    ]

    UpstreamResource [
      shape = Mrecord style = filled fillcolor = lemonchiffon
      width = 2
    ]
    Generator [
      shape = Mrecord color = black
      label = "{Generator|main_config_url\lgenerator_config_name}"
      width = 2
    ]
    GeneratorTaskActivity [
      shape = record style = filled fillcolor = lightblue
      width = 2
    ]
    Resource [
      shape = Mrecord style = filled fillcolor = lemonchiffon
      width = 2
    ]

    ModelBuildingConfig -> Generator;
    ModelBuildingConfig -> GeneratorConfig;
    UpstreamResource -> Generator;
    Generator -> GeneratorTaskActivity [label = "target"];
    GeneratorTaskActivity -> Resource [label = "generated"]
    GeneratorTaskActivity -> GeneratorConfig [label = "used_config"]

   }

The Generator produces an activity with the generated entity, registered to the knowledge graph. The Generated is completed if a target entity can be found in the database with the specific ``used_config`` input.


.. note::

   Since the workflow registers and searches for resources in the knowledge graph, retriggering a task requires deprecating the corresponding activity associated with the input configuration. Without this, the target will always be found, and Luigi will consider the task as already completed, preventing re-execution.


.. _generators:

Generator: Hierarchy
--------------------

.. graphviz::

   digraph SBOWorkflow{

    ModelBuildingConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    CellCompositionConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    CellPositionConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    MorphologyAssignmentConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    MEModelConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    MacroConnectomeConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    MacroConnectomeConfig2 [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        label = "MacroConnectomeConfig"
    ]

    MicroConnectomeConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    SynapseConfig [
        shape = Mrecord style = filled fillcolor = lemonchiffon
    ]

    CellComposition [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        target = "_top"
    ]

    CellPositionDetailedCircuit [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        label = "DetailedCircuit"
    ]

    MModelDetailedCircuit [
        shape = Mrecord style = filled fillcolor = lemonchiffon
        label = "DetailedCircuit"
    ]

    MEModelDetailedCircuit[
        shape = Mrecord style = filled fillcolor = lemonchiffon
        label = "DetailedCircuit"
    ]

    MicroDetailedCircuit[
        shape = Mrecord style = filled fillcolor = lemonchiffon
        label = "DetailedCircuit"
    ]

    FiltDetailedCircuit[
        shape = Mrecord style = filled fillcolor = lemonchiffon
        label = "DetailedCircuit"
    ]

    CellCompositionActivity [
      shape = record style = filled fillcolor = lightblue
      label = "GeneratorTaskActivity"
    ]

    CellPositionActivity [
      shape = record style = filled fillcolor = lightblue
      label = "GeneratorTaskActivity"
    ]

    MModelActivity [
      shape = record style = filled fillcolor = lightblue
      label = "GeneratorTaskActivity"
    ]

    MEModelActivity [
      shape = record style = filled fillcolor = lightblue
      label = "GeneratorTaskActivity"
    ]

    MacroActivity [
      shape = record style = filled fillcolor = lightblue
      label = "GeneratorTaskActivity"
    ]

    MicroActivity [
      shape = record style = filled fillcolor = lightblue
      label = "GeneratorTaskActivity"
    ]

    FiltActivity [
      shape = record style = filled fillcolor = lightblue
      label = "GeneratorTaskActivity"
    ]

     CellCompositionGenerator [
        shape = Mrecord color = black
     ]

     CellPositionGenerator [
        shape = Mrecord color = black
     ]

     MorphologyAssignmentGenerator [
        shape = Mrecord color = black
     ]

     MEModelGenerator [
        shape = Mrecord color = black
     ]

     MacroConnectomeGenerator [
        shape = Mrecord color = black
     ]

     MicroConnectomeGenerator [
        shape = Mrecord color = black
     ]

     ConnectomeFilteringGenerator [
        shape = Mrecord color = black
     ]


     ModelBuildingConfig -> CellCompositionConfig;
     ModelBuildingConfig -> CellPositionConfig;
     ModelBuildingConfig -> MorphologyAssignmentConfig;
     ModelBuildingConfig -> MEModelConfig;
     ModelBuildingConfig -> MacroConnectomeConfig;
     ModelBuildingConfig -> MicroConnectomeConfig;
     ModelBuildingConfig -> SynapseConfig;

     ModelBuildingConfig -> CellCompositionGenerator;
     CellCompositionGenerator -> CellCompositionActivity [label = "target"];
     CellCompositionActivity -> CellComposition [label = "generated"];
     CellCompositionActivity -> CellCompositionConfig [label = "used_config"];
     CellComposition -> CellPositionGenerator;

     ModelBuildingConfig -> CellPositionGenerator;
     CellPositionGenerator -> CellPositionActivity [label = "target"];
     CellPositionActivity -> CellPositionDetailedCircuit [label = "generated"];
     CellPositionActivity -> CellPositionConfig [label = "used_config"];
     CellPositionDetailedCircuit -> MorphologyAssignmentGenerator;


     ModelBuildingConfig -> MorphologyAssignmentGenerator;
     MorphologyAssignmentGenerator -> MModelActivity [label = "target"];
     MModelActivity -> MModelDetailedCircuit [label="generated"];
     MModelActivity -> MorphologyAssignmentConfig [label = "used_config"];
     MModelDetailedCircuit -> MEModelGenerator;

     ModelBuildingConfig -> MEModelGenerator;
     MEModelGenerator -> MEModelActivity [label = "target"];
     MEModelActivity -> MEModelDetailedCircuit [label = "generated"];
     MEModelActivity -> MEModelConfig [label = "used_config"];
     MEModelDetailedCircuit -> MicroConnectomeGenerator;

     ModelBuildingConfig -> MacroConnectomeGenerator;
     MacroConnectomeGenerator -> MacroActivity [label = "target"];
     MacroActivity -> MacroConnectomeConfig2 [label = "generated"];
     MacroActivity -> MacroConnectomeConfig [label = "used_config"];
     MacroConnectomeConfig2 -> MicroConnectomeGenerator;

     ModelBuildingConfig -> MicroConnectomeGenerator;
     MicroConnectomeGenerator -> MicroActivity [label = "target"];
     MicroActivity -> MicroDetailedCircuit [label = "generated"];
     MicroActivity -> MicroConnectomeConfig [label = "used_config"];
     MicroDetailedCircuit -> ConnectomeFilteringGenerator;

     ModelBuildingConfig -> ConnectomeFilteringGenerator;
     ConnectomeFilteringGenerator -> FiltActivity [label = "target"];
     FiltActivity -> FiltDetailedCircuit [label = "generated"];
     FiltActivity -> SynapseConfig [label = "used_config"];

   }

Generator: Anatomy & Variants
-----------------------------

Types
~~~~~

There are two main types of generators:

* Relay Generators
* Multi Variant Generators

Relay generators are simple tasks that propagate the input config downstream by creating an activity with a clone of the config. An example of a Relay generator is the MacroConnectomeGenerator.

Multi variant generators are the most common tasks, scattering variant tasks and then merging them to produce the final result. Each Generator that derives from MultiVariantGenerator implements a
scatter and optionally a merge method.

Variants
~~~~~~~~

A Generator may launch one or more variant tasks. A variant is an executable tool identified by the triplet ``(generator_name, variant_name, version)``. For more info see the `variant documentation
<https://blue-cwl.readthedocs.io/en/latest/concepts/variant.html#what-is-a-variant>`_.

.. _blue_cwl_variant: https://blue-cwl.readthedocs.io/en/latest/concepts/variant.html#what-is-a-variant


Generator: Configurations
-------------------------

CellCompositionGenerator
~~~~~~~~~~~~~~~~~~~~~~~~

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


For the variant definition of the manipulation algorithm refer to `blue-cwl documentation <https://blue-cwl.readthedocs.io/en/latest/registry.html#cell-composition-cell-composition-manipulation-v3>`__.


CellPositionGenerator
~~~~~~~~~~~~~~~~~~~~~

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


MorphologyAssignmentGenerator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

MEModelGenerator
~~~~~~~~~~~~~~~~

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

.. code-block::

For the variant definition of the placement algorithm refer to `blue-cwl documentation <https://blue-cwl.readthedocs.io/en/latest/registry.html#memodel-neurons-memodel-v3>`__


MacroConnectomeGenerator
~~~~~~~~~~~~~~~~~~~~~~~~

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


MicroConnectomeGenerator
~~~~~~~~~~~~~~~~~~~~~~~~

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

ConnectomeFilteringGenerator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

