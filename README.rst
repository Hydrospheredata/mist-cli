===========
 Mist CLI
===========
.. image:: https://codecov.io/gh/Hydrospheredata/mist-cli/branch/master/graph/badge.svg
    :target: https://codecov.io/github/Hydrospheredata/mist-cli
    :alt: codecov.io
.. image:: https://travis-ci.org/Hydrospheredata/mist-cli.svg?branch=master
    :target: https://travis-ci.org/Hydrospheredata/mist-cli
    :alt: travis-ci.org

CLI interface for mist server for creating and updating endpoint and context configuration.

Installation
--------------
.. code-block:: bash

    pip install mist-cli

Autocompletion
---------------
Instantly run the script every time when bash starts that generates content of **mist-cli-complete.sh**.

.. code-block:: bash

    echo 'eval "$(_MIST_CLI_COMPLETE=source mist-cli)"' >> ~/.bash_profile


Or you can just add **mist-cli-complete.sh** somewhere in the system and execute it when it needed.

Also, you can generate that script by yourself:

.. code-block:: bash

    _MIST_CLI_COMPLETE=source mist-cli > mist-cli-complete.sh


Usage mist-cli apply
---------------
Apply method accepts **-f** or **--file** parameter that should contain file or folder with ***.conf** files that represent your mist configuration (Artifact, Function or Context).

The content of the file is a simple model that describes deployment of a particular entry of your config.

All conf files should follow some format (e.g `00artifact.conf <example/my-awesome-job/00artifact.conf>`_)
where field **version** is only supported in Artifact model type.
By default, you can name your config files as you want and all configs will be processed without any order.
So you can define this order with 2 numbers followed by a name.
So, for example, you name your stage **test-stage** and want it run with priority **10**
you should name the file like **10test-stage.conf**.

For easy development process, you can skip validation of your configuration, for example,
by default, *Function* models will be validated against context and artifact existence.
So if you want to update *Function* with *context = foo* and *path = test-job_0_0_1.py*
make sure that artifact with that key and context with that name exists in *Mist*.
You can easily skip this kind of validation with **--validate** flag.

