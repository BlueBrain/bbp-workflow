Overview
========

.. _workflow:

The Workflow Task
-----------------

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


.. _generator_anatomy:

Generator: Anatomy & Variants
-----------------------------

.. _generator_types:

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
