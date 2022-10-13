from lmtrex.common.lmconstants import (APIService)
from lmtrex.frontend.templates import stats_template
from lmtrex.flask_app.broker.base import _S2nService

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
