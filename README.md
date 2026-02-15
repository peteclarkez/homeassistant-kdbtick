# Home Assistant KDB-X Tick Component

This component allows Home Assistant to publish state change events to a [KDB-X Tick](https://code.kx.com/q/kb/kdb-tick/) database.

## Installation

### HACS (Recommended)

1. Open HACS in your Home Assistant instance
2. Click the three dots in the top right corner and select **Custom repositories**
3. Add `https://github.com/peteclarkez/homeassistant-kdbtick` with category **Integration**
4. Click **Install**
5. Restart Home Assistant

### Manual

1. Copy the `custom_components/kdbtick` directory to your Home Assistant `<config>/custom_components/` directory
2. Restart Home Assistant

## Configuration

1. Go to **Settings** → **Devices & Services**
2. Click **Add Integration** and search for **KDB-X Tick**
3. Enter your connection details:

| Option | Default | Description |
|--------|---------|-------------|
| Host | `localhost` | KDB-X tickerplant hostname or IP |
| Port | `5010` | Tickerplant port |
| Table Name | `hass_event` | Target table name |
| Update Function | `.u.updjson` | KDB-X function to call |

### Options

After setup, click **Configure** on the integration to access additional options:

- **Include entities** — only publish events for these entities (empty = all)
- **Exclude entities** — exclude specific entities from publishing
- **Enable debug logging** — toggle verbose logging for troubleshooting

## Events

The integration listens for two event types:

- **State changes** — published whenever an entity's state changes, including the numeric value, string value, and all attributes
- **Logbook entries** — published as events with domain `event`

### State Change Payload

```json
{
  "time": 1704877539.956502,
  "host": "hass_event",
  "event": {
    "domain": "sensor",
    "entity_id": "temperature",
    "attributes": {"unit": "C", "friendly_name": "Temperature"},
    "value": 22.5,
    "svalue": "22.5"
  }
}
```

### Connection Handling

- If the connection fails on startup, the integration retries every 60 seconds
- If a send fails mid-operation, the connection is automatically re-established on the next event
- A startup event is sent when the connection is first established

## KDB-X Server

This component is designed to work with the [hassio-kdb](https://github.com/peteclarkez/hassio-kdb) add-on which provides a complete KDB-X tick system with:

- Tickerplant (port 5010) - receives events
- RDB (port 5011) - real-time database
- HDB (port 5012) - historical database
- Gateway (port 5013) - unified query interface

## Querying Data

Connect to the Gateway to query your Home Assistant data:

```q
h:hopen `:192.168.0.100:5013

/ Get recent events
h "select from hass_event where time>.z.p-01:00:00"

/ Get temperature history
h "select time, evalue from hass_event where entity_id=`temperature"

/ Get all entity domains
h "select distinct edomain from hass_event"
```

## Troubleshooting

### Connection Issues

If the component fails to connect, it will retry every 60 seconds. Check:

1. The KDB-X tickerplant is running
2. The host/port are correct
3. There's no firewall blocking the connection

### Enable Debug Logging

1. Go to **Settings** → **Devices & Services** → **KDB-X Tick**
2. Click **Configure**
3. Enable the **Debug logging** toggle
4. Click **Submit**

## Changelog

### v5.0.0
- UI-based configuration via config flow (replaces `configuration.yaml`)
- HACS support
- Entity include/exclude filtering via options UI
- Debug logging toggle in options UI
- Pure Python KDB+ IPC client (no external dependencies)
- Logbook entry forwarding
- Automatic reconnection with 60-second retry

### v2.0.0
- Migrated from qpython to PyKX
- Updated default port to 5010 (standard tickerplant port)
- Improved connection handling with automatic reconnection

### v1.0.0
- Initial release using qpython
