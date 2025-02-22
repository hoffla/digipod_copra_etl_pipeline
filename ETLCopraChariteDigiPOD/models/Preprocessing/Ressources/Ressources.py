RASS_MAPPING = {
            4: "sehr streitlustig",  # Very combative
            3: "sehr agitiert",  # Very agitated
            2: "agitiert",  # Agitated
            1: "unruhig",  # Restless
            0: "aufmerksam, ruhig",  # Alert, calm
            -1: "schläfrig",  # Drowsy
            -2: "leichte Sedierung",  # Light sedation
            -3: "mäßige Sedierung",  # Moderate sedation
            -4: "tiefe Sedierung",  # Deep sedation
            -5: "nicht erweckbar"  # Unarousable
        }


class IncrementalGenerator:
    def __init__(self):
        self.bases = {
            "name": 219,
            "dosis": 220,
            "date": 309,
            "time": 310,
            "unit": 474
        }
        self.increments = {
            "name": 3,
            "dosis": 3,
            "date": 2,
            "time": 2,
            "unit": 1,
        }

        self.current_values = self.bases.copy()

    def __call__(self, key):
        if key not in self.bases:
            raise ValueError(f"Unknown Key: {key}")

        value = self.current_values[key]
        self.current_values[key] += self.increments[key]
        return value