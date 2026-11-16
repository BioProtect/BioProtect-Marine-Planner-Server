"""getActivities reads bioprotect.pad (seeded by 02_seed.sql), not a CSV."""

import json

from conftest import api


async def test_get_activities_lists_pad_activities_once_each(server):
    _, body = await api(server, "GET", "/server/getActivities")
    acts = json.loads(body["data"])
    names = [a["activity"] for a in acts]
    assert len(names) == len(set(names)) > 0
    # an activity in two PAD categories keeps its first one
    drilling = next(a for a in acts if a["activity"] == "Exploratory drilling")
    assert drilling["category"] == "Research"
