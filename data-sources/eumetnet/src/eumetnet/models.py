# SPDX-FileCopyrightText: 2025- European Centre for Medium-Range Weather Forecasts (ECMWF)
# SPDX-License-Identifier: Apache-2.0

from pydantic_settings import BaseSettings, SettingsConfigDict


class NetAtmoMQTTConfig(BaseSettings):
    # Broker connection fields (host, credentials, client_id) come from MQTT_*
    # env vars — a k8s Secret via envFrom, since the Helm chart never renders
    # secrets into the ConfigMap; flush tuning comes from the config file.
    model_config = SettingsConfigDict(env_prefix="MQTT_", extra="ignore")

    host: str
    port: int = 8883
    username: str
    password: str
    client_id: str
    keepalive: int = 120
    use_tls: bool = True
    flush_interval_seconds: int = 60
    flush_max_records: int = 50000
    max_buffer_size: int = 200000
