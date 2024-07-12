.. bbp_workflow documentation master file

BBP Workflow documentation
==========================

Overview of the tasks available in the workflow engine:


**Simulation**:

.. inheritance-diagram:: bbp_workflow.simulation.task
   :top-classes: bbp_workflow.task.KgTask, bbp_workflow.task.SlurmCfg,
    bbp_workflow.task.LookupKgEntity, bbp_workflow.simulation.task.NrdmsPySim,
    bbp_workflow.task.Notebook
   :parts: 1


**Base Tasks**:

.. inheritance-diagram:: bbp_workflow.task
   :top-classes: luigi.task.Config, luigi.task.ExternalTask, bbp_workflow.task.KgTask
   :parts: 1


**Targets**:

.. inheritance-diagram:: bbp_workflow.target
   :parts: 1


.. toctree::
   :hidden:
   :maxdepth: 2

   Home <self>
   api
   dependencies
   changelog
