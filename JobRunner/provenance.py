
class Provenance(object):
    def __init__(self):
        self.subactions = []
        # TODO fill in prov


    def add_subaction(self, action):
            # 'name': 'bogus',
            # 'ver': 'bbogus',
            # 'code_url': '<url>',  # TODO
            # 'commit': '<hash>'  # TODO
        self.subactions.append(action)

    def get_prov(self):
        return {'subactions': self.subactions}