Generator Graph
===============

.. _generators:

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
