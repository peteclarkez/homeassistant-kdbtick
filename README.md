# Home Assistant kdb+tick Component

This component allows the system to publish events to [kdb+tick](https://code.kx.com/q/kb/kdb-tick/)

To get started put the follwing files

```bash

/custom_components/kdbtick/__init__.py
/custom_components/kdbtick/manifest.json

```

into

```bash

<config directory>/custom_components/kdbtick/__init__.py
<config directory>/custom_components/kdbtick/manifest.json

```

## Issues with hassio

The requirements in the manifest include `qpython>=2.0.0`. When starting the integration on hassio installation on raspberrypi3B, this fails as it needs `gcc` in order to build the optomized compression code. As `gcc` isn't installed by default this will require it's insalled via `apk`. The way I got this working was to use the ssh+web integration.

Once it's setup and you can connect, disable the protected mode. THis will allow you to access the docker commands and get access to the home assistant container.

- Run `docker ps` to get the running containers.
- find the `homeassistant` container and connect to it via `docker exec -it <conainterid> /bin/bash`
  - `docker exec -it $(docker ps | grep raspberrypi3-homeassistant | awk '{print $1}') /bin/bash`
- once in the container install gcc via `apk add build-base`
- You can then manually install qpython `pip install qpython`
  - `apk add build-base && pip install qpython`
- Once this is done reboot *the host* via the supervisor tab.
  - THere was an issue where kdb wasn't ready when the system came back and homeassistant would need restarted after.

> TODO find out how to either disable the need for this in the qpython build or else find out how to install `gcc`.
> This might require a custom build of qpython to be deployed or linked via github.

**Example configuration.yaml:**

```yaml

kdbtick:
  host: '192.168.0.100'
  port: 2001

```

## Q Source Code

The equivalent kdb code is in this repo
 - https://github.com/peteclarkez/hassio-kdb