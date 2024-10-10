Quickstart
==========

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
