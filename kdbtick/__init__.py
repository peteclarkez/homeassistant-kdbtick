"""Support to send data to a kdb+ tick instance."""
import asyncio
import json
import logging
import time

import numpy

from qpython import qconnection
from qpython.qtype import QException

import voluptuous as vol

from homeassistant.const import (
    CONF_HOST,
    CONF_NAME,
    CONF_PORT,
    EVENT_STATE_CHANGED,
    EVENT_HOMEASSISTANT_STOP,
    EVENT_LOGBOOK_ENTRY,
)
from homeassistant.helpers import event as event_helper, state as state_helper
from homeassistant.helpers.aiohttp_client import async_get_clientsession
import homeassistant.helpers.config_validation as cv
from homeassistant.helpers.entityfilter import FILTER_SCHEMA
from homeassistant.helpers.json import JSONEncoder

_LOGGER = logging.getLogger(__name__)

DOMAIN = "kdbtick"
CONF_FILTER = "filter"
CONF_FUNC = "updF"

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 2001
DEFAULT_NAME = "hass_event"
DEFAULT_FUNC = ".u.updjson"

RETRY_INTERVAL = 60  # seconds
RETRY_MESSAGE = f"%s Retrying in {RETRY_INTERVAL} seconds."

CONFIG_SCHEMA = vol.Schema(
    {
        DOMAIN: vol.Schema(
            {
                vol.Optional(CONF_HOST, default=DEFAULT_HOST): cv.string,
                vol.Optional(CONF_PORT, default=DEFAULT_PORT): cv.port,
                vol.Optional(CONF_NAME, default=DEFAULT_NAME): cv.string,
                vol.Optional(CONF_FUNC, default=DEFAULT_FUNC): cv.string,
                vol.Optional(CONF_FILTER, default={}): FILTER_SCHEMA,
            }
        )
    },
    extra=vol.ALLOW_EXTRA,
)


async def async_setup(hass, config):
    return dosetup(hass, config)


def dosetup(hass, config):
    _LOGGER.info("Starting kdbtick setup")

    """Set up the kdb+ component."""
    conf = config[DOMAIN]
    host = conf.get(CONF_HOST)
    port = conf.get(CONF_PORT)
    name = conf.get(CONF_NAME)
    updf = conf.get(CONF_FUNC)
    entity_filter = conf[CONF_FILTER]

    def connTest(qconn):
        if not qconn.is_connected():
            return False
        try:
            q.sendSync(" ")
        except QException as err:
            _LOGGER.error(err)
            return False
        except ConnectionError as err:
            # _LOGGER.error(err)
            return False
        return True

    def reconnect(qconn):
        try:
            if qconn.is_connected():
                qconn.close()
            qconn.open()
        except QException as err:
            _LOGGER.error(err)
            return False
        except ConnectionError as err:
            # _LOGGER.exception(RETRY_MESSAGE, err)
            _LOGGER.error(err)
            return False
        return True

    # create connection object
    q = qconnection.QConnection(host=host, port=port)
    # initialize connection
    reconnect(q)

    if not connTest(q):
        if not reconnect(q):
            # _LOGGER.warn(
            #     "Error Initialising Connection, IPC version: %s. Is connected: %s"
            #     % (q.protocol_version, q.is_connected())
            # )
            _LOGGER.error(
                RETRY_MESSAGE,
            )
            event_helper.async_call_later(
                hass, RETRY_INTERVAL, lambda _: dosetup(hass, config)
            )
            # event_helper.async_call_later(
            #     hass, RETRY_INTERVAL, lambda _: async_setup(hass, config)
            # )
            return True

    _LOGGER.info(q)
    _LOGGER.info(
        "IPC version: %s. Is connected: %s" % (q.protocol_version, q.is_connected())
    )

    payload = {
        "time": time.time(),
        "host": name,
        "event": {
            "domain": DOMAIN,
            "entity_id": "kdb.connect",
            "meta": "kdb+ integration has started",
            "attributes": dict(
                dkbtick="Ford", model="Mustang", testedAgainst="2012.01.4"
            ),
            "value": -1.1,
            "svalue": " ",
        },
    }

    try:
        q.sendAsync(updf, numpy.string_(name), json.dumps(payload, cls=JSONEncoder))
    except QException as err:
        _LOGGER.error(err)
    except ConnectionError as err:
        _LOGGER.error(err)

    def publishPayload(payload):

        if not connTest(q):
            if not reconnect(q):
                _LOGGER.warn(
                    "Error reconnecting, IPC version: %s. Is connected: %s"
                    % (q.protocol_version, q.is_connected())
                )
                return

        try:
            q.sendAsync(updf, numpy.string_(name), json.dumps(payload, cls=JSONEncoder))
        except QException as err:
            _LOGGER.error(err)
        except ConnectionError as err:
            _LOGGER.error(err)

    async def kdbtick_event_listener(event):
        """Listen for new messages on the bus and sends them to kdb+."""

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

        publishPayload(payload)

    def shutdown(event):
        """Shut down the thread."""
        _LOGGER.info("kdbtick, Shutdown Message")

        # instance.queue.put(None)
        # instance.join()
        # influx.close()

    def logbook_entry_listener(event):
        """Listen for logbook entries and send them as events."""
        name = event.data.get("name")
        message = event.data.get("message")

        attributes: dict(
            title="Home Assistant Event",
            text=f"%%% \n **{name}** {message} \n %%%",
            entity=event.data.get("entity_id"),
            domain=event.data.get("domain"),
        )

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

        publishPayload(payload)

        _LOGGER.info("Sent event %s", event.data.get("entity_id"))

    hass.bus.async_listen(EVENT_LOGBOOK_ENTRY, logbook_entry_listener)
    hass.bus.async_listen(EVENT_STATE_CHANGED, kdbtick_event_listener)
    hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, shutdown)

    return True