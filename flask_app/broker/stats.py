from flask_app.broker.constants import (APIService)
from sppy.frontend.templates import stats_template
from flask_app.broker.base import _S2nService

# .............................................................................
class StatsSvc(_S2nService):
    SERVICE_TYPE = APIService.Stats

    # ...............................................
    # ...............................................
    @classmethod
    def get_stats(self, **params):
        """Institution and collection level stats

        Return:
            HTML page that fetches and formats stats
        """
        return stats_template()
