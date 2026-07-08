# Engineer Setup

This project uses [direnv](https://direnv.net/) and [nix](https://nixos.org) to streamline local development and provide a consistent build environment.

## Prerequisites:

Supported operating systems:
- Ubuntu Linux 20.04+
- MacOS Ventura 13.3.1 (a)+

Supported Shells:
- bash
- zsh

## Installation Instructions

### Quick Check

To check whether the required tools are already installed, run:

```text
for i in nix direnv; do command -v "${i}" || echo "${i} not found, please install";done
```

If both tools are found, run `make help` or `make debug` at the root of the repo to verify your setup.

### Install Nix

The Nix installer is interactive and requires sudo access. You can inspect the script before running it if needed.

```text
# install nix
curl --proto '=https' --tlsv1.2 -L https://nixos.org/nix/install | sh

# add yourself as a nix trusted user
echo "trusted-users = root $(whoami)" | sudo tee -a /etc/nix/nix.conf
sudo pkill nix-daemon
```

### Install Direnv

```text
# install direnv
bash <(curl -sfL https://direnv.net/install.sh)
```

To complete the [direnv](https://direnv.net/) install, be sure to [add the shell hook](https://direnv.net/docs/hook.html) to your shell profile.

You may encounter a problem when trying to do `direnv allow` caused by nix that is not configured to use flakes.
It may manifest itself by an error looking something like this:
```
error: experimental Nix feature 'nix-command' is disabled; add '--extra-experimental-features nix-command' to enable it
```
In that case, add an entry to your nix config, typically located under `~/.config/nix/nix.conf`:
```
experimental-features = nix-command flakes
```

## Testing Local Environment Is Configured Correctly

Once you have Nix and Direnv installed, run `direnv allow` at the root of repository to ensure the environment is active. From there you can run `make debug` to ensure everything is setup correctly.

## Additional Software

### Docker
Docker is used in this project for various activities including testing, it is **strongly** suggested that you have docker installed.

Links:
- [MacOS](https://docs.docker.com/desktop/install/mac-install/)
- [Ubuntu](https://docs.docker.com/desktop/install/ubuntu/)

## Configure IntelliJ

* Install IntelliJ [command line](https://www.jetbrains.com/help/idea/working-with-the-ide-features-from-command-line.html#toolbox)
* Install scala and BSP plugins in IDEA.
* Import project into IDEA:

```text
make apps/idea
```

To get IDE support in mill's build files under settings in `Language & Frameworks` → `Scala` → `Worksheet` → `Treat .sc files as` choose `Always Ammonite`.

## Configure VSCode and Metals

* Install [VSCode](https://code.visualstudio.com/)
* Install the [Scala (Metals)](https://scalameta.org/metals/docs/editors/vscode/#installation) extension
* Install the [direnv](https://marketplace.visualstudio.com/items?itemName=Rubymaniac.vscode-direnv) extension
* Open the `apps` folder in VSCode (File -> Open Folder...)
* Accept Metals' invitation to import the build 
