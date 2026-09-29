"""Publish Trenčín's local time and live Open-Meteo conditions to openHASP.

Set HASP_MQTT_HOST to the same broker configured on the display. Optionally
set HASP_MQTT_PORT, HASP_MQTT_USERNAME, HASP_MQTT_PASSWORD, and HASP_NODE
(the firmware default hostname is ``plate``). Install dependencies with
``python -m pip install requests paho-mqtt``.
"""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
from datetime import datetime
from threading import Event
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import paho.mqtt.client as mqtt
    import requests
except ImportError as exc:
    raise SystemExit(
        "Missing Python package. Install dependencies with: "
        "python -m pip install requests paho-mqtt"
    ) from exc


LOG = logging.getLogger("feed_daemon")
LATITUDE = 48.8945
LONGITUDE = 18.0444
TIMEZONE_NAME = "Europe/Bratislava"
UPDATE_INTERVAL_SECONDS = 30
MQTT_HOST = os.getenv("HASP_MQTT_HOST", "").strip()
MQTT_PORT = int(os.getenv("HASP_MQTT_PORT", "1883"))
MQTT_NODE = os.getenv("HASP_NODE", "plate").strip()
MQTT_USERNAME = os.getenv("HASP_MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("HASP_MQTT_PASSWORD")
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"

WEATHER_DESCRIPTIONS = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog",
    48: "Rime fog",
    51: "Light drizzle",
    53: "Drizzle",
    55: "Heavy drizzle",
    56: "Freezing drizzle",
    57: "Heavy freezing drizzle",
    61: "Light rain",
    63: "Rain",
    65: "Heavy rain",
    66: "Freezing rain",
    67: "Heavy freezing rain",
    71: "Light snow",
    73: "Snow",
    75: "Heavy snow",
    77: "Snow grains",
    80: "Rain showers",
    81: "Heavy showers",
    82: "Violent showers",
    85: "Snow showers",
    86: "Heavy snow showers",
    95: "Thunderstorm",
    96: "Thunderstorm with hail",
    99: "Severe thunderstorm with hail",
}

STOP = Event()
SESSION = requests.Session()
SESSION.headers["User-Agent"] = "openHASP-Trencin-weather-display/1.0"


def publish_text(client: mqtt.Client, object_id: int, text: str) -> None:
    topic = f"hasp/{MQTT_NODE}/command/p1b{object_id}.text"
    result = client.publish(topic, text, qos=1, retain=False)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        LOG.warning("MQTT publish failed for %s (rc=%s)", topic, result.rc)


def fetch_weather() -> tuple[float, str, float]:
    response = SESSION.get(
        WEATHER_URL,
        params={
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "current": "temperature_2m,weather_code,wind_speed_10m",
            "timezone": TIMEZONE_NAME,
        },
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    current = payload.get("current")
    if not isinstance(current, dict):
        raise ValueError("Open-Meteo response has no current conditions")

    temperature = float(current["temperature_2m"])
    weather_code = int(current["weather_code"])
    wind_speed = float(current["wind_speed_10m"])
    description = WEATHER_DESCRIPTIONS.get(weather_code, "Conditions updated")
    return temperature, description, wind_speed


def update_display(client: mqtt.Client, local_timezone: ZoneInfo) -> None:
    now = datetime.now(local_timezone)
    publish_text(client, 2, now.strftime("%H:%M"))
    publish_text(client, 3, now.strftime("%A  /  %d %B").upper())

    temperature, description, wind_speed = fetch_weather()
    publish_text(client, 4, f"{temperature:+.1f}°")
    publish_text(client, 5, description)
    publish_text(client, 6, f"Wind  {wind_speed:.1f} km/h")
    LOG.info(
        "%s | %s %+.1f C | wind %.1f km/h",
        now.strftime("%H:%M:%S"),
        description,
        temperature,
        wind_speed,
    )


def make_client() -> mqtt.Client:
    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"openhasp-feed-{MQTT_NODE}",
        )
    else:
        client = mqtt.Client(client_id=f"openhasp-feed-{MQTT_NODE}")
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def main() -> int:
    if not MQTT_HOST:
        LOG.error("Set HASP_MQTT_HOST to the broker configured on the display.")
        return 2
    if not MQTT_NODE:
        LOG.error("HASP_NODE must match the display's openHASP hostname.")
        return 2
    if not 1 <= MQTT_PORT <= 65535:
        LOG.error("HASP_MQTT_PORT must be between 1 and 65535.")
        return 2
    try:
        local_timezone = ZoneInfo(TIMEZONE_NAME)
    except ZoneInfoNotFoundError:
        LOG.error("Timezone data for %s is unavailable.", TIMEZONE_NAME)
        return 2

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    client = make_client()
    connected = Event()

    def on_connect(
        mqtt_client: mqtt.Client,
        userdata: object,
        flags: object,
        reason_code: object,
        *extra: object,
    ) -> None:
        if int(reason_code) == 0:
            connected.set()
            LOG.info("Connected to MQTT broker %s:%d for node %s", MQTT_HOST, MQTT_PORT, MQTT_NODE)
        else:
            LOG.error("MQTT connection rejected: %s", reason_code)

    def on_disconnect(
        mqtt_client: mqtt.Client,
        userdata: object,
        *disconnect_args: object,
    ) -> None:
        reason_code = disconnect_args[-2] if len(disconnect_args) >= 2 else (
            disconnect_args[0] if disconnect_args else 0
        )
        connected.clear()
        if int(reason_code) != 0:
            LOG.warning("MQTT disconnected: %s; reconnecting", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    def request_stop(signum: int, frame: object) -> None:
        LOG.info("Received signal %s; stopping.", signum)
        STOP.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        client.loop_start()
        if not connected.wait(timeout=20):
            LOG.error("Could not connect to the MQTT broker at %s:%d.", MQTT_HOST, MQTT_PORT)
            return 1

        while not STOP.is_set():
            if connected.is_set():
                try:
                    update_display(client, local_timezone)
                except (requests.RequestException, ValueError, KeyError, TypeError) as exc:
                    LOG.error("Live weather/time update failed: %s", exc)
            STOP.wait(UPDATE_INTERVAL_SECONDS)
    except OSError as exc:
        LOG.error("MQTT connection error: %s", exc)
        return 1
    finally:
        client.loop_stop()
        client.disconnect()
        SESSION.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
