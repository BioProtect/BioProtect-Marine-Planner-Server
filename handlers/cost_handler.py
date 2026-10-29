"""
Cost profile management handlers.

Handles updating, deleting, activating cost profiles and creating
costs from cumulative impact data.
"""

from handlers.base_handler import BaseHandler
from services.service_error import ServicesError, raise_error

UNIFORM_COST_NAME = "Equal area"
"""The name of the cost profile that is equal area."""


class DeleteCostHandler(BaseHandler):
    """Deletes a cost profile and its associated values from the database.

    Args:
        cost_profile_id (int): The cost profile ID to delete.
    Returns:
        {"info": "Cost profile deleted"}
    """

    async def get(self):
        try:
            self.validate_args(self.request.arguments, ['cost_profile_id'])
            cost_profile_id = int(self.get_argument("cost_profile_id"))

            # Check if this profile is the active profile on any project
            active_check = await self.pg.execute(
                "SELECT id, name FROM bioprotect.projects "
                "WHERE active_cost_profile_id = %s;",
                data=[cost_profile_id],
                return_format="Array"
            )
            if active_check:
                project_name = active_check[0].get("name", "a project")
                raise ServicesError(
                    f"Cannot delete: this cost profile is currently "
                    f"active on project '{project_name}'.")

            # Delete (cost_profile_values cascade automatically)
            result = await self.pg.execute(
                "DELETE FROM bioprotect.cost_profiles WHERE id = %s "
                "RETURNING id;",
                data=[cost_profile_id],
                return_format="Array"
            )
            if not result:
                raise ServicesError(
                    f"Cost profile {cost_profile_id} not found.")

            self.send_response({"info": "Cost profile deleted"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class SetActiveCostProfileHandler(BaseHandler):
    """Sets a cost profile as the active profile for a project.

    Args:
        project_id (int): The project ID.
        cost_profile_id (int): The cost profile ID to set as active.
    Returns:
        {"info": "Cost profile activated"}
    """

    async def get(self):
        try:
            self.validate_args(self.request.arguments,
                          ['project_id', 'cost_profile_id'])
            project_id = int(self.get_argument("project_id"))
            cost_profile_id = int(self.get_argument("cost_profile_id"))

            # Verify the cost profile belongs to this project
            result = await self.pg.execute(
                "SELECT id FROM bioprotect.cost_profiles "
                "WHERE id = %s AND project_id = %s;",
                data=[cost_profile_id, project_id],
                return_format="Array"
            )
            if not result:
                raise ServicesError(
                    f"Cost profile {cost_profile_id} not found "
                    f"for project {project_id}.")

            await self.pg.execute(
                "UPDATE bioprotect.projects "
                "SET active_cost_profile_id = %s WHERE id = %s;",
                data=[cost_profile_id, project_id]
            )
            self.send_response({"info": "Cost profile activated"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class GetCostProfileActivitiesHandler(BaseHandler):
    """Returns the activities that make up a cost profile.

    Args:
        cost_profile_id (int): The cost profile ID.
    Returns:
        {"info": ..., "data": [{id, activity, activity_name, filename,
            description, source, created_by, creation_date, extent}]}
    """

    async def get(self):
        try:
            self.validate_args(self.request.arguments, ['cost_profile_id'])
            cost_profile_id = int(self.get_argument("cost_profile_id"))

            rows = await self.pg.execute(
                """
                SELECT ma.id,
                       ma.activity,
                       ma.activity_name,
                       ma.filename,
                       ma.description,
                       ma.source,
                       ma.created_by,
                       to_char(ma.creation_date,
                               'DD/MM/YY HH24:MI:SS')::text AS creation_date,
                       ma.extent::text AS extent
                  FROM bioprotect.cost_profile_activities cpa
                  JOIN bioprotect.metadata_activities ma
                    ON ma.id = cpa.activity_id
                 WHERE cpa.cost_profile_id = %s
                 ORDER BY lower(ma.activity);
                """,
                data=[cost_profile_id],
                return_format="Array"
            )

            self.send_response({
                "info": "Cost profile activities returned",
                "data": rows or []
            })
        except ServicesError as e:
            raise_error(self, e.args[0])
