import dateutil.parser

from plenario.database import app_engine


# ===============
# Validator Class
# ===============

class Validator(object):
    """Meant for validating query string arguments given to views."""

    def __init__(self):

        # parameters to check
        self.params = []
        self.datasets = []
        self.shape = []

    def consider(self, *params):

        self.params = params
        return self

    def on(self, *datasets):

        self.datasets = datasets
        return self

    def shape_of(self, shape):

        return self

    def validate(self):

        for dataset in self.datasets:
            if not validators['dataset'](dataset):
                return False  # this should be where the err message is

        for param in self.params:
            if not validators[param](param):
                # get the actual param from request object
                return False  # this should be where the err message is


# ===================
# Validator Functions
# ===================

def agg_validator(agg_str):

    return agg_str in ['day', 'week', 'month', 'quarter', 'year']


def date_validator(date_str):

    try:
        dateutil.parser.parse(date_str)
        return True
    except (ValueError, OverflowError):
        return False

# =======================
# Validator-Param Mapping
# =======================

validators = {
    'dataset': app_engine.has_table,
    'agg': agg_validator,
    'obs_date__ge': date_validator,
    'obs_date__le': date_validator
}
