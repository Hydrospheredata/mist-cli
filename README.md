# Mist CLI
[![Build Status](https://travis-ci.org/Hydrospheredata/mist-cli.svg?branch=master)](https://travis-ci.org/Hydrospheredata/mist-cli)
[![codecov](https://codecov.io/gh/Hydrospheredata/mist-cli/branch/master/graph/badge.svg)](https://codecov.io/gh/Hydrospheredata/mist-cli)
CLI interface for mist server for creating and updating endpoint and context configuration.

## Installation
```bash
git clone git@github.com:Hydrospheredata/mist-cli.git
cd mist-cli
pip install .
```
## Autocompletion

Instantly run script every time when bash starts that is generate content of `mist-cli-complete.sh`.
```bash
echo 'eval "$(_MIST_CLI_COMPLETE=source mist-cli)"' >> ~/.bash_profile
```

Or you can just add `mist-cli-complete.sh` somewhere in the system and execute it when it needed.

Also you can generate that script by yourself:
```bash
_MIST_CLI_COMPLETE=source mist-cli > mist-cli-complete.sh
```

## Usage
You can deploy configuration in several ways:
1. mist-cli deploy
2. mist-cli apply
3. mist-cli deploy-dev

### mist-cli deploy
Deploy method works as all in one config with shared versioning for jobs through cli `--job-version` parameter and
common job path for all endpoints through `--job-path` parameter.
Configuration file must be in some predefined format. Example file could be found [here](docs/sample.conf)

### mist-cli deploy-dev
Deploy dev method adding prefixes and suffixes to all config entry: job, endpoint name, context name. Also its adds
`--user` parameter that will be substituted to every entry's name.

### mist-cli apply
Apply method accepts `-f` or `--folder` parameter that should contain only directories with conf files or just files with conf files.
Content of the file is simple model that describes deployment of particular entry of your config.
Every conf file is a stage of your deployment, so it must be prioritised.
Priority of the stage defined with first 2 symbols of your filename.
So for example you name you stage `test-stage` and want it run with priority `10`.
Resulting filename should be `10test-stage.conf`.
You can define as many stages as you want, but most common usage will be define 3 stage for particular job
(look for example setup for [simple-context job](example/simple-context)).
There some rules of applying deployment of you configuration that should be mind:
1. Artifact of one version with different content (different sha1) could not be deployed to mist
2. Endpoint should contain only existing `job link` or `job paths`
3. Endpoint should contain context that exists either in current deploy stages or exists remotely on mist instance.
4. File path for artifact deployment should be absolute path

