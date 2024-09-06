bbp-workflow
============

Automated pipelines of batch jobs using python and the luigi framework.


Documentation
=============

* `latest release <https://bbp-workflow.readthedocs.io/en/stable/>`_
* `latest snapshot <https://bbp-workflow.readthedocs.io/en/latest/>`_

Installation
============

bbp-workflow can be pip installed with the following command:

.. code-block:: bash

    $ pip install bbp-workflow

Tests
=====

.. code-block:: bash

    pip install tox
    tox

Local Run
=========

.. code-block:: bash

    make install_to_venv
    ./bbp-workflow-launch.sh --config workflows/foo.cfg foo Bar dynamic-param=world

Acknowledgements
================

The development of this software was supported by funding to the Blue Brain Project, a research center of the École polytechnique fédérale de Lausanne (EPFL), from the Swiss government’s ETH Board of the Swiss Federal Institutes of Technology.

For license and authors, see LICENSE.txt and AUTHORS.txt respectively.

Copyright (c) 2022-2024 Blue Brain Project/EPFL
