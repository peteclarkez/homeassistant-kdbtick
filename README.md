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

Add the following to your `configuration.yaml`:

```yaml
kdbtick:
  host: '192.168.0.100'  # KDB-X tickerplant host
  port: 5010             # Tickerplant port (default: 5010)
  name: 'hass_event'     # Table name (default: hass_event)
  filter:                # Optional: filter which entities to publish
    include_domains:
      - sensor
      - binary_sensor
    exclude_entities:
      - sensor.some_noisy_sensor
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `host` | `localhost` | KDB-X tickerplant hostname or IP |
| `port` | `5010` | Tickerplant port |
| `name` | `hass_event` | Target table name |
| `updF` | `.u.updjson` | Update function to call |
| `filter` | `{}` | Entity filter (include/exclude domains/entities) |

## Data Format

Events are published as JSON to the `.u.updjson` function with the following structure:

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
h "select time, nvalue from hass_event where entity_id=`temperature"

/ Get all sensor domains
h "select distinct sym from hass_event"
```

## Troubleshooting

### Connection Issues

If the component fails to connect, it will retry every 60 seconds. Check:

1. The KDB-X tickerplant is running
2. The host/port are correct
3. There's no firewall blocking the connection

### Check Logs

Enable debug logging in `configuration.yaml`:

```yaml
logger:
  default: info
  logs:
    custom_components.kdbtick: debug
```

## Changelog

### v2.0.0
- Migrated from qpython to PyKX
- Updated default port to 5010 (standard tickerplant port)
- Improved connection handling with automatic reconnection
- Cleaner code structure with KdbConnection class

### v1.0.0
- Initial release using qpython
