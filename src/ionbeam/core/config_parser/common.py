

class ConfigError(Exception):
    "Generic error from the config parser."
    pass


class ConfigMatchError(ConfigError):
    "Raised when the yaml data keys do not match the specification from the python dataclasses."
    pass
