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
Instantly run script every time when bash starts that is generate content of `mist-cli-complete.sh`.

.. code-block:: bash

    echo 'eval "$(_MIST_CLI_COMPLETE=source mist-cli)"' >> ~/.bash_profile


Or you can just add `mist-cli-complete.sh` somewhere in the system and execute it when it needed.

Also you can generate that script by yourself:

.. code-block:: bash

    _MIST_CLI_COMPLETE=source mist-cli > mist-cli-complete.sh


Usage
------
You can deploy configuration in several ways:

#. mist-cli deploy
#. mist-cli apply
#. mist-cli deploy-dev

mist-cli apply
---------------
Apply method accepts `-f` or `--file` parameter that should contain only directories with conf files or just files with
conf files.
Content of the file is simple model that describes deployment of particular entry of your config.
Every conf file is a stage of your deployment, so it must be prioritised.
Priority of the stage defined with first 2 symbols of your filename.
So for example you name you stage `test-stage` and want it run with priority `10`.
Resulting filename should be `10test-stage.conf`.
You can define as many stages as you want, but most common usage will be define 3 stage for particular job
(look for example setup for [simple-context job](example/simple-context)).
There some rules of applying deployment of you configuration that should be mind:

#. Artifact of one version with different content (different sha1) could not be deployed to mist
#. Endpoint should contain only existing `job link` or `job paths`
#. Endpoint should contain context that exists either in current deploy stages or exists remotely on mist instance.
#. File path for artifact deployment should be absolute path

