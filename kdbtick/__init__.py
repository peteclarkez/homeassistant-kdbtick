"""Support to send data to a kdb+ tick instance."""
import asyncio
import json
import logging
import time

import numpy

from qpython import qconnection
from qpython.qtype import QException

import voluptuous as vol

from homeassistant.const import CONF_HOST, CONF_NAME, CONF_PORT, EVENT_STATE_CHANGED
from homeassistant.helpers import state as state_helper
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
            _LOGGER.error(err)
            return False
        return True

    # create connection object
    q = qconnection.QConnection(host=host, port=port)
    # initialize connection
    reconnect(q)

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
            "attributes": dict(brand="Ford", model="Mustang", year=1964),
            "value": 0.0,
        },
    }

    if not connTest(q):
        if not reconnect(q):
            _LOGGER.warn(
                "Error reconnecting, IPC version: %s. Is connected: %s"
                % (q.protocol_version, q.is_connected())
            )
            return
    try:
        q.sendAsync(
            updf, numpy.string_(name), json.dumps(payload, cls=JSONEncoder)
        )

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
            _state = state_helper.state_as_number(state)
        except ValueError:
            _state = state.state

        payload = {
            "time": event.time_fired.timestamp(),
            "host": name,
            "event": {
                "domain": state.domain,
                "entity_id": state.object_id,
                "attributes": dict(state.attributes),
                "value": _state,
            },
        }

        if not connTest(q):
            if not reconnect(q):
                _LOGGER.warn(
                    "Error reconnecting, IPC version: %s. Is connected: %s"
                    % (q.protocol_version, q.is_connected())
                )
                return

        try:
            q.sendAsync(
                updf, numpy.string_(name), json.dumps(payload, cls=JSONEncoder)
            )
        except QException as err:
            _LOGGER.error(err)
        except ConnectionError as err:
            _LOGGER.error(err)

    hass.bus.async_listen(EVENT_STATE_CHANGED, kdbtick_event_listener)

    return True