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

**Example configuration.yaml:**

```yaml

kdbtick:
  host: '192.168.0.100'
  port: 2001

```

## Q Source Code

Q Code has been added here to be added to the tick installation.
It is only intended to be here temporarily until the tick docker image is created.
