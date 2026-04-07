"""
Cost profile management handlers.

Handles updating, deleting, activating cost profiles and creating
costs from cumulative impact data.
"""

import os

import pandas as pd

from handlers.base_handler import BaseHandler
from handlers.websocket_handler import SocketHandler
from services.service_error import ServicesError, raise_error
from services.file_service import update_file_parameters
from services.project_service import write_csv
from functions.utils import create_cost_from_impact

UNIFORM_COST_NAME = "Equal area"
"""The name of the cost profile that is equal area."""


def validate_args(arguments, req_arguments):
    """Validates that all required arguments are present in the request."""
    missing = [key for key in req_arguments if key not in arguments]
    if missing:
        raise ServicesError(
            f"Missing required arguments: {', '.join(missing)}")


def file_to_df(file_name):
    """Reads a CSV file and returns the data as a DataFrame."""
    return (pd.read_csv(file_name, sep=None, engine='python')
            if os.path.exists(file_name) else pd.DataFrame())


class UpdateCostsHandler(BaseHandler):
    """Updates a project's costs in the PUNAME file using a named cost profile.

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        costname (string): The name of the cost profile to use (*.cost file).
    Returns:
        {"info": "Costs updated"}
    """

    async def get(self):
        try:
            validate_args(self.request.arguments,
                          ['user', 'project', 'costname'])
            costname = self.get_argument("costname")
            cost_file_path = os.path.join(
                self.input_folder, f"{costname}.cost")
            puname_file_path = os.path.join(
                self.input_folder, self.projectData["files"]["PUNAME"])

            puname_df = file_to_df(puname_file_path)

            if costname == UNIFORM_COST_NAME:
                puname_df['cost'] = 1
            else:
                if not os.path.exists(cost_file_path):
                    raise ServicesError(
                        f"The cost file '{costname}' does not exist.")
                cost_df = pd.read_csv(
                    cost_file_path, sep=None, engine='python')
                puname_df = cost_df.join(puname_df[['status']])

            input_dat_path = os.path.join(self.project_folder, "input.dat")
            update_file_parameters(input_dat_path, {'COSTS': costname})

            await write_csv(self, "PUNAME", puname_df)
            self.send_response({"info": 'Costs updated'})
        except ServicesError as e:
            raise_error(self, e.args[0])


class DeleteCostHandler(BaseHandler):
    """Deletes a cost profile file.

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        costname (string): The name of the cost profile to delete (*.cost file).
    Returns:
        {"info": "Cost deleted"}
    """

    def get(self):
        try:
            validate_args(self.request.arguments,
                          ['user', 'project', 'costname'])
            costname = self.get_argument("costname")
            cost_file_path = os.path.join(
                self.input_folder, f"{costname}.cost")

            if not os.path.exists(cost_file_path):
                raise ServicesError(
                    f"The cost file '{costname}' does not exist.")

            os.remove(cost_file_path)
            self.send_response({"info": 'Cost deleted'})
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
            validate_args(self.request.arguments,
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


class CreateCostsFromImpactHandler(SocketHandler):
    """Creates a cost file from cumulative impact data.

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        pu_filename (string): Planning unit filename.
        impact_filename (string): Impact file table name.
        impact_type (string): Type of impact.
    Returns:
        {"info": "New cost file created from Cumulative Impact"}
    """

    async def open(self):
        print('CreateCostsFromImpactHandler: ')
        try:
            await super().open({
                'info': "Creating Costs from Cumulative Impact..."
            })
        except ServicesError as e:
            print('ServicesError as e: ', e)
            pass
        else:
            validate_args(self.request.arguments,
                          ['user', 'project', 'pu_filename',
                           'impact_filename', 'impact_type'])
            sql = "select filename from bioprotect.%s;" % self.get_argument(
                'impact_filename')
            records = await self.pg.execute(sql, return_format="Array")
            impact_filename = records[0][0]
            file_loc = "data/uploaded_rasters/" + impact_filename
            create_cost_from_impact(self.get_argument('user'),
                                    self.get_argument('project'),
                                    self.get_argument('pu_filename'),
                                    file_loc,
                                    self.get_argument('impact_type'))
            self.close({
                'info': "New cost file created from Cumulative Impact",
            })
