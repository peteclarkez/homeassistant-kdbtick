"""Support to send data to a KDB-X tick instance."""

import json
import logging
import time

from .kx.c import c as kdb

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    CONF_HOST,
    CONF_NAME,
    CONF_PORT,
    EVENT_STATE_CHANGED,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_LOGBOOK_ENTRY,
)
from homeassistant.core import HomeAssistant
from homeassistant.helpers import event as event_helper, state as state_helper
from homeassistant.helpers.json import JSONEncoder

_LOGGER = logging.getLogger(__name__)

DOMAIN = "kdbtick"
CONF_FUNC = "updF"
CONF_INCLUDE_ENTITIES = "include_entities"
CONF_EXCLUDE_ENTITIES = "exclude_entities"

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5010
DEFAULT_NAME = "hass_event"
DEFAULT_FUNC = ".u.updjson"

RETRY_INTERVAL = 60  # seconds


class KdbConnection:
    """Manage connection to KDB-X tickerplant."""

    def __init__(self, host: str, port: int):
        """Initialize the connection manager."""
        self.host = host
        self.port = port
        self._conn = None

    def is_connected(self) -> bool:
        """Check if connection is active."""
        if self._conn is None:
            return False
        try:
            self._conn.k("1+1")
            return True
        except Exception:
            return False

    def connect(self) -> bool:
        """Establish connection to KDB-X."""
        try:
            if self._conn is not None:
                self.close()
            self._conn = kdb(self.host, self.port)
            _LOGGER.info("Connected to KDB-X at %s:%d", self.host, self.port)
            return True
        except Exception as err:
            _LOGGER.error("Failed to connect to KDB-X: %s", err)
            self._conn = None
            return False

    def close(self):
        """Close the connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def send(self, func: str, table_name: str, payload: str) -> bool:
        """Send data to KDB-X using the specified function."""
        if not self.is_connected():
            if not self.connect():
                return False

        try:
            # table_name as str → symbol (-11), payload wrapped as CharVector → char vector (10)
            self._conn.k(func, table_name, kdb.CharVector(payload))
            return True
        except Exception as err:
            _LOGGER.error("Failed to send to KDB-X: %s", err)
            self._conn = None
            return False


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up KDB-X Tick from a config entry."""
    _LOGGER.info("Starting KDB-X Tick setup")

    host = entry.data.get(CONF_HOST, DEFAULT_HOST)
    port = entry.data.get(CONF_PORT, DEFAULT_PORT)
    name = entry.data.get(CONF_NAME, DEFAULT_NAME)
    updf = entry.data.get(CONF_FUNC, DEFAULT_FUNC)

    # Build entity filter from options
    include = set(entry.options.get(CONF_INCLUDE_ENTITIES, []))
    exclude = set(entry.options.get(CONF_EXCLUDE_ENTITIES, []))

    def entity_filter(entity_id: str) -> bool:
        """Filter entities based on include/exclude lists."""
        if include:
            return entity_id in include
        if exclude:
            return entity_id not in exclude
        return True

    conn = KdbConnection(host, port)

    if not await hass.async_add_executor_job(conn.connect):
        _LOGGER.error("Initial connection failed. Retrying in %d seconds.", RETRY_INTERVAL)

        async def retry_setup(_now):
            """Retry setup after delay."""
            await async_setup_entry(hass, entry)

        event_helper.async_call_later(hass, RETRY_INTERVAL, retry_setup)
        return True

    _LOGGER.info("KDB-X connection established to %s:%d", host, port)

    # Send startup event
    startup_payload = {
        "time": time.time(),
        "host": name,
        "event": {
            "domain": DOMAIN,
            "entity_id": "kdb.connect",
            "attributes": {
                "integration": "kdbtick",
                "version": "2.0.0",
                "using": "kx",
            },
            "value": -1.1,
            "svalue": "connected",
        },
    }
    await hass.async_add_executor_job(
        conn.send, updf, name, json.dumps(startup_payload, cls=JSONEncoder)
    )

    def publish_payload(payload):
        """Publish a payload to KDB-X."""
        if not conn.send(updf, name, json.dumps(payload, cls=JSONEncoder)):
            _LOGGER.warning("Failed to publish to KDB-X at %s:%d", host, port)

    async def kdbtick_event_listener(event):
        """Listen for new messages on the bus and send them to KDB-X."""
        state = event.data.get("new_state")
        if state is None or not entity_filter(state.entity_id):
            return

        try:
            _nstate = state_helper.state_as_number(state)
        except ValueError:
            _nstate = -1.1

        try:
            _sstate = str(state.state)
        except ValueError:
            _sstate = "unknown"

        payload = {
            "time": event.time_fired.timestamp(),
            "host": name,
            "event": {
                "domain": state.domain,
                "entity_id": state.object_id,
                "attributes": dict(state.attributes),
                "value": _nstate,
                "svalue": _sstate,
            },
        }

        await hass.async_add_executor_job(publish_payload, payload)

    async def logbook_entry_listener(event):
        """Listen for logbook entries and send them as events."""
        entry_name = event.data.get("name")
        message = event.data.get("message")

        attributes = {
            "title": "Home Assistant Event",
            "text": f"%%% \n **{entry_name}** {message} \n %%%",
            "entity": event.data.get("entity_id"),
            "domain": event.data.get("domain"),
        }

        payload = {
            "time": time.time(),
            "host": name,
            "event": {
                "domain": "event",
                "entity_id": event.data.get("entity_id"),
                "attributes": attributes,
                "value": -1.1,
                "svalue": " ",
            },
        }

        await hass.async_add_executor_job(publish_payload, payload)
        _LOGGER.debug("Sent logbook event %s", event.data.get("entity_id"))

    # Register listeners and store unsub handles for cleanup
    unsub_state = hass.bus.async_listen(EVENT_STATE_CHANGED, kdbtick_event_listener)
    unsub_logbook = hass.bus.async_listen(EVENT_LOGBOOK_ENTRY, logbook_entry_listener)

    def shutdown(event):
        """Shut down the connection."""
        _LOGGER.info("KDB-X Tick shutting down")
        conn.close()

    unsub_stop = hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, shutdown)

    # Store references for unload
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][entry.entry_id] = {
        "conn": conn,
        "unsub": [unsub_state, unsub_logbook, unsub_stop],
    }

    # Reload listeners when options change
    entry.async_on_unload(entry.add_update_listener(_async_update_listener))

    return True


async def _async_update_listener(hass: HomeAssistant, entry: ConfigEntry):
    """Reload integration when options change."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    data = hass.data[DOMAIN].pop(entry.entry_id, None)
    if data:
        for unsub in data["unsub"]:
            unsub()
        data["conn"].close()

    return True
