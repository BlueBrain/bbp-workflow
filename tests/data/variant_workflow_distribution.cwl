cwlVersion: v1.2

class: Workflow

id: workflow_me_type_property

label: mock-generator-workflow

inputs:
- id: configuration
  type: NexusType
- id: circuit
  type: NexusType
- id: variant
  type: NexusType
- id: output_dir
  type: Directory

outputs:
  partial_circuit:
    type: NexusType
    outputSource: register/resource_file

steps:
- id: dirs
  run:
    cwlVersion: v1.2
    class: CommandLineTool
    id: create-directories
    label: create-directories
    environment:
      env_type: MODULE
      modules:
        - unstable
        - py-cwl-registry
    baseCommand:
    - cwl-registry
    - execute
    - connectome-filtering-synapses
    - dirs
    inputs:
    - id: output_stage_dir
      type: string
      inputBinding:
        prefix: --output-stage-dir
    - id: output_build_dir
      type: string
      inputBinding:
        prefix: --output-build-dir
    outputs:
    - id: stage_dir
      type: Directory
      outputBinding:
        glob: $(inputs.output_stage_dir)
    - id: build_dir
      type: Directory
      outputBinding:
        glob: $(inputs.output_build_dir)
  in:
    output_stage_dir:
      source: output_dir
      valueFrom: $(self.path)/stage
    output_build_dir:
      source: output_dir
      valueFrom: $(self.path)/build
  out:
  - build_dir
  - stage_dir
- id: stage
  run:
    cwlVersion: v1.2
    class: CommandLineTool
    id: connectome-filtering-staging
    label: connectome-filtering-staging
    environment:
      env_type: MODULE
      modules:
        - unstable
        - py-cwl-registry
    baseCommand:
    - cwl-registry
    - execute
    - connectome-filtering-synapses
    - stage
    inputs:
    - id: configuration
      type: NexusType
      inputBinding:
        prefix: --configuration-id
    - id: circuit
      type: NexusType
      inputBinding:
        prefix: --circuit-id
    - id: variant
      type: NexusType
      inputBinding:
        prefix: --variant-id
    - id: stage_dir
      type: Directory
      inputBinding:
        prefix: --staging-dir
    - id: output_configuration_file
      type: string
      inputBinding:
        prefix: --output-configuration-file
    - id: output_circuit_file
      type: string
      inputBinding:
        prefix: --output-circuit-file
    - id: output_variant_file
      type: string
      inputBinding:
        prefix: --output-variant-file
    - id: output_atlas_file
      type: string
      inputBinding:
        prefix: --output-atlas-file
    - id: output_edges_file
      type: string
      inputBinding:
        prefix: --output-edges-file
    outputs:
    - id: configuration_file
      type: File
      outputBinding:
        glob: $(inputs.output_configuration_file)
    - id: circuit_file
      type: File
      outputBinding:
        glob: $(inputs.output_circuit_file)
    - id: variant_file
      type: File
      outputBinding:
        glob: $(inputs.output_variant_file)
    - id: atlas_file
      type: File
      outputBinding:
        glob: $(inputs.output_atlas_file)
    - id: edges_file
      type: File
      outputBinding:
        glob: $(inputs.output_edges_file)
  in:
    configuration: configuration
    circuit: circuit
    variant: variant
    stage_dir:
      source: dirs/stage_dir
    output_configuration_file:
      source: dirs/stage_dir
      valueFrom: $(self.path)/staged_configuration_file.json
    output_circuit_file:
      source: dirs/stage_dir
      valueFrom: $(self.path)/circuit_config.json
    output_variant_file:
      source: dirs/stage_dir
      valueFrom: $(self.path)/variant.cwl
    output_atlas_file:
      source: dirs/stage_dir
      valueFrom: $(self.path)/atlas.json
    output_edges_file:
      source: dirs/stage_dir
      valueFrom: $(self.path)/edges.h5
  out:
  - configuration_file
  - circuit_file
  - variant_file
  - atlas_file
  - edges_file
- id: transform
  run:
    cwlVersion: v1.2
    class: CommandLineTool
    id: connectome-filtering-transform
    label: connectome-filtering-transform
    environment:
      env_type: MODULE
      modules:
        - unstable
        - py-cwl-registry
    baseCommand:
    - cwl-registry
    - execute
    - connectome-filtering-synapses
    - recipe
    inputs:
    - id: circuit_file
      type: File
      inputBinding:
        prefix: --circuit-file
    - id: source_node_population_name
      type: string
      inputBinding:
        prefix: --source-node-population-name
    - id: target_node_population_name
      type: string
      inputBinding:
        prefix: --target-node-population-name
    - id: atlas_file
      type: File
      inputBinding:
        prefix: --atlas-file
    - id: configuration_file
      type: File
      inputBinding:
        prefix: --configuration-file
    - id: output_recipe_file
      type: string
      inputBinding:
        prefix: --output-recipe-file
    outputs:
    - id: recipe_file
      type: File
      outputBinding:
        glob: $(inputs.output_recipe_file)
  in:
    circuit_file:
      source: stage/circuit_file
    source_node_population_name:
      valueFrom: root__neurons
    target_node_population_name:
      valueFrom: root__neurons
    atlas_file:
      source: stage/atlas_file
    configuration_file:
      source: stage/configuration_file
    output_recipe_file:
      source: dirs/build_dir
      valueFrom: $(self.path)/recipe.json
  out:
  - recipe_file
- id: connectome_filtering
  run:
    cwlVersion: v1.2
    class: CommandLineTool
    id: connectome-filtering-functionalizer
    label: connectome-filtering-functionalizer
    environment:
      env_type: MODULE
      modules:
      - unstable
      - spykfunc
    executor:
      type: slurm
      slurm_config:
        partition: prod
        nodes: 5
        exclusive: true
        time: '8:00:00'
        account: proj134
        constraint: nvme
        mem: 0
      remote_config:
        host: bbpv1.epfl.ch
      env_vars:
        PYARROW_IGNORE_TIMEZONE: '1'
    baseCommand:
    - dplace
    - functionalizer
    inputs:
    - id: edges_file
      type: File
      inputBinding:
        position: 1
    - id: circuit_config
      type: File
      inputBinding:
        position: 2
        prefix: --circuit-config
    - id: work_dir
      type: string
      inputBinding:
        position: 3
        prefix: --work-dir
    - id: output_dir
      type: string
      inputBinding:
        position: 4
        prefix: --output-dir
    - id: from
      type: string
      inputBinding:
        position: 5
        prefix: --from
    - id: to
      type: string
      inputBinding:
        position: 6
        prefix: --to
    - id: filters
      type: string[]
      inputBinding:
        position: 7
        prefix: --filters
        itemSeparator: ' '
    - id: recipe
      type: File
      inputBinding:
        position: 6
        prefix: --recipe
    outputs:
    - id: parquet_dir
      type: Directory
      outputBinding:
        glob: $(inputs.output_dir)/circuit.parquet
  in:
    edges_file:
      source: stage/edges_file
    circuit_config:
      source: stage/circuit_file
    output_dir:
      source: dirs/build_dir
    work_dir:
      source: dirs/build_dir
      valueFrom: $(self.path)/workdir
    from:
      valueFrom: root__neurons
    to:
      valueFrom: root__neurons
    filters:
      valueFrom:
      - SynapseProperties
    recipe:
      source: transform/recipe_file
  out:
  - parquet_dir
- id: parquet_to_sonata
  run:
    cwlVersion: v1.2
    class: CommandLineTool
    id: connectome-filtering-transform
    label: connectome-filtering-transform
    environment:
      env_type: MODULE
      modules:
      - unstable
      - parquet-converters
    executor:
      type: slurm
      slurm_config:
        partition: prod
        nodes: 3
        ntasks_per_node: 10
        exclusive: true
        time: '8:00:00'
        account: proj134
        constraint: nvme
        mem: 0
      remote_config:
        host: bbpv1.epfl.ch
    baseCommand:
    - parquet2hdf5
    inputs:
    - id: parquet_dir
      type: Directory
      inputBinding:
        position: 0
    - id: output_edges_file
      type: string
      inputBinding:
        position: 1
    - id: output_edge_population_name
      type: string
      inputBinding:
        position: 2
    outputs:
    - id: edges_file
      type: File
      outputBinding:
        glob: $(inputs.output_edges_file)
  in:
    parquet_dir:
      source: connectome_filtering/parquet_dir
    output_edges_file:
      source: dirs/build_dir
      valueFrom: $(self.path)/edges.h5
    output_edge_population_name:
      valueFrom: root_neurons__root_neurons__chemical
  out:
  - edges_file
- id: register
  run:
    cwlVersion: v1.2
    class: CommandLineTool
    id: connectome-filtering-register
    label: connectome-filtering-register
    environment:
      env_type: MODULE
      modules:
        - unstable
        - py-cwl-registry
    baseCommand:
    - cwl-registry
    - execute
    - connectome-filtering-synapses
    - register
    inputs:
    - id: circuit
      type: NexusType
      inputBinding:
        prefix: --circuit-id
    - id: edges_file
      type: File
      inputBinding:
        prefix: --edges-file
    - id: output_dir
      type: string
      inputBinding:
        prefix: --output-dir
    - id: output_resource_file
      type: string
      inputBinding:
        prefix: --output-resource-file
    outputs:
    - id: resource_file
      type: NexusType
      outputBinding:
        glob: $(inputs.output_resource_file)
  in:
    circuit: circuit
    output_dir:
      source: dirs/build_dir
    edges_file:
      source: parquet_to_sonata/edges_file
    output_resource_file:
      source: output_dir
      valueFrom: $(self.path)/workflow_resource.json
  out:
  - resource_file
