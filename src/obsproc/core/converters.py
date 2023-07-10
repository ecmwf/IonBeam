unit_conversions = {
    "°C -> K": lambda t: t + 273.15,
    "Degrees -> °": lambda d: d,
    "m/s -> m.s-1": lambda s: s,
    "km/h -> m/s": lambda s: s * 0.277778,
    "mbar -> hPa": lambda p: p,
    "mbar -> Pa": lambda p: p * 100,
    "hPa -> Pa": lambda p: p * 100,
    "°C/100m -> K/100m": lambda x: x,
}
