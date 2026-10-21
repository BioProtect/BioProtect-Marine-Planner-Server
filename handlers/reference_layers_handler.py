"""View-only map layers for the "Reference layers" dialog.

GET /server/reference-layers -> {"wdpa": {"loaded": bool, "release": str|None}}

The tiles come from Martin (wdpa_tiles); this only says whether there is
anything to show, and which release it is for the attribution.
"""

from handlers.base_handler import BaseHandler
from services.service_error import ServicesError, raise_error


class ReferenceLayersHandler(BaseHandler):

    def initialize(self, pg):
        super().initialize(pg=pg)

    async def get(self):
        try:
            await self._get_authenticated_user_id()
            rows = await self.pg.execute(
                """
                SELECT (SELECT release FROM bioprotect.wdpa_release LIMIT 1) AS release,
                       EXISTS (SELECT 1 FROM bioprotect.wdpa_areas) AS loaded
                """,
                return_format="Dict")
            self.send_response({"wdpa": rows[0]})
        except ServicesError as e:
            raise_error(self, e.args[0])
