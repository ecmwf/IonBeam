from uuid import UUID
from ..bases import Source

class ConfigError(Exception):
    "Generic error from the config parser."
    pass


class ConfigMatchError(ConfigError):
    "Raised when the yaml data keys do not match the specification from the python dataclasses."
    pass

def print_action_chains(actions):
    sources = [a for a in actions if isinstance(a, Source)]
    processors = [a for a in actions if not isinstance(a, Source)]

    chains = [[s,] for s in sources]
    for a in processors:
        if not isinstance(a.match, UUID): 
            chains.append([str(a.match), a,])
            continue
        for c in chains:
            matches = a.match.int == c[-1].id.int
            if matches: c.append(a)
                

    for c in chains:
        print(" --> ".join(str(a) for a in c))