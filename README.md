# Mist CLI

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
echo 'eval "$(_FOO_BAR_COMPLETE=source foo-bar)"' >> ~/.bash_profile
```

Or you can just add `mist-cli-complete.sh` somewhere in the system and execute it when it needed.

Also you can generate that script by yourself:
```bash
_MIST_CLI_COMPLETE=source mist-cli > mist-cli-complete.sh
```