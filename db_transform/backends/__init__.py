""" """

class MigrationBackend(object):
    """ """
    def __init__(self, max_records, **kwargs):
        self.max_records = max_records
        if not max_records and max_records != 0:
            self.max_records = 0

    def parse(self, **kwargs):
        raise NotImplementedError

    def get_data(self, **kwargs):
        raise NotImplementedError

    def get_fields(self, **kwargs):
        raise NotImplementedError
