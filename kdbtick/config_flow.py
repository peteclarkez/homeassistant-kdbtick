"""Config flow for KDB-X Tick integration."""

import logging

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_HOST, CONF_NAME, CONF_PORT
from homeassistant.core import callback
from homeassistant.helpers import selector

from .kx import c as kdb

_LOGGER = logging.getLogger(__name__)

DOMAIN = "kdbtick"
CONF_FUNC = "updF"

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5010
DEFAULT_NAME = "hass_event"
DEFAULT_FUNC = ".u.updjson"

CONF_INCLUDE_ENTITIES = "include_entities"
CONF_EXCLUDE_ENTITIES = "exclude_entities"


class KdbtickConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for KDB-X Tick."""

    VERSION = 1

    async def async_step_user(self, user_input=None):
        """Handle the initial step."""
        errors = {}

        if user_input is not None:
            try:
                await self.hass.async_add_executor_job(
                    _test_connection, user_input[CONF_HOST], user_input[CONF_PORT]
                )
            except Exception:
                errors["base"] = "cannot_connect"

            if not errors:
                await self.async_set_unique_id(
                    f"{user_input[CONF_HOST]}:{user_input[CONF_PORT]}"
                )
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title=f"KDB-X {user_input[CONF_HOST]}:{user_input[CONF_PORT]}",
                    data=user_input,
                )

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_HOST, default=DEFAULT_HOST): str,
                    vol.Required(CONF_PORT, default=DEFAULT_PORT): vol.All(
                        int, vol.Range(min=1, max=65535)
                    ),
                    vol.Optional(CONF_NAME, default=DEFAULT_NAME): str,
                    vol.Optional(CONF_FUNC, default=DEFAULT_FUNC): str,
                }
            ),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(_config_entry):
        """Get the options flow handler."""
        return KdbtickOptionsFlow()


class KdbtickOptionsFlow(config_entries.OptionsFlow):
    """Handle options flow for KDB-X Tick."""

    async def async_step_init(self, user_input=None):
        """Manage entity filter options."""
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        current = self.config_entry.options

        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Optional(
                        CONF_INCLUDE_ENTITIES,
                        default=current.get(CONF_INCLUDE_ENTITIES, []),
                    ): selector.EntitySelector(
                        selector.EntitySelectorConfig(multiple=True)
                    ),
                    vol.Optional(
                        CONF_EXCLUDE_ENTITIES,
                        default=current.get(CONF_EXCLUDE_ENTITIES, []),
                    ): selector.EntitySelector(
                        selector.EntitySelectorConfig(multiple=True)
                    ),
                }
            ),
        )


def _test_connection(host, port):
    """Test connection to KDB-X server (runs in executor)."""
    conn = kdb(host, port)
    try:
        conn.k("1+1")
    finally:
        conn.close()
