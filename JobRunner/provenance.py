from datetime import datetime, timezone


class Provenance(object):
    def __init__(self, params):
        self.subactions = []
        (module, method) = params["method"].split(".")
        self.actions = dict()
        t = datetime.now(timezone.utc).astimezone().replace(microsecond=0)
        desc = "KBase SDK method run via the KBase Execution Engine"
        # TODO may need to check that service-ver is set
        self.prov = {
            "time": t.isoformat(),
            "service": module,
            "service_ver": params["service_ver"],
            "method": method,
            "method_params": params["params"],
            "input_ws_objects": [],
            "subactions": [],
            "description": desc,
        }

    def add_subaction(self, data):
        if data["name"] not in self.actions:
            self.actions[data["name"]] = data
            action = data
            self.prov["subactions"].append(action)

    def get_prov(self):
        return [self.prov]
