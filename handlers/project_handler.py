from datetime import datetime
import fnmatch
import glob
import shutil
import uuid
import json
from os import rename, sep, walk
from os.path import basename, exists, join, normpath, splitext
from types import SimpleNamespace

import pandas as pd
from handlers.base_handler import BaseHandler
from psycopg2 import sql
from services.file_service import (get_key_value, get_keys,
                                   copy_directory, delete_all_files,
                                   file_to_df, get_key_values_from_file,
                                   normalize_dataframe, read_file,
                                   write_to_file)
from services.project_service import clone_a_project, get_project_data, set_folder_paths
from services.service_error import ServicesError, raise_error
from services.user_service import get_users
from services.queries import get_pu_grids_query


class ProjectHandler(BaseHandler):
    """
    REST HTTP handler for project-related operations, including creating, cloning, renaming, deleting,
    fetching, and updating projects.
    """

    def initialize(self, pg, get_species_data, update_species):
        super().initialize()
        self.pg = pg
        self.get_species_data = get_species_data
        self.update_species = update_species

    def validate_args(self, args, required_keys):
        # sourcery skip: use-named-expression
        """Checks that all of the arguments in argumentList are in the arguments dictionary."""
        missing = [key for key in required_keys if key not in args]
        if missing:
            raise ServicesError(f"Missing required arguments: {
                                ', '.join(missing)}")

    def json_serial(self, obj):
        """Convert datetime objects to a JSON-serializable format."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    async def get_project_by_id(self, project_id):
        """Fetch project details based on project ID."""
        query = """
            SELECT * FROM projects WHERE id = $1;
        """
        result = await self.pg.execute(query, [project_id], return_format="Dict")
        # ✅ Returns first result or None if not found
        return result[0] if result else None

    def create_project_folder(self, project_name, template_folder):
        project_path = join(self.folder_user, project_name)

        # Ensure the project does not already exist
        if exists(project_path):
            raise ServicesError(f"The project '{project_name}' already exists")

        copy_directory(template_folder, project_path)
        # Update the folder paths for the new project in the handler object
        set_folder_paths(self, {
            'user': [self.user.encode("utf-8")],
            'project': [project_name.encode("utf-8")]
        })

    def update_file_parameters(self, filename, new_params):
        """
        Updates specific parameters in a file. Parameters matching the keys in `new_params`
        are updated, while others remain unchanged.
        """
        if not new_params:
            return  # Exit if there are no parameters to update

        file_content = read_file(filename)
        lines = file_content.splitlines()
        updated_lines = []
        for line in lines:
            updated_line = next(
                (f"{key} {value}" for key,
                 value in new_params.items() if line.startswith(key)),
                line  # Keep the line unchanged if no match
            )
            updated_lines.append(updated_line)

        # Write the updated content back to the file
        write_to_file(filename, "\n".join(updated_lines))

    async def get_projects_for_user(self, user_id):
        """
        Gets all projects for a user along with full projectData (metadata, files, run parameters, renderer).

        Args:
            user_id (int): The ID of the user.

        Returns:
            list[dict]: Each dict contains full project data.
        """

        print('user_id: ', user_id)
        projects = await self.pg.execute("""
            SELECT p.*, pu.alias AS planning_unit_alias
            FROM public.projects p
            LEFT JOIN marxan.metadata_planning_units pu
                ON p.planning_unit_id = pu.unique_id
            WHERE p.user_id = $1
            ORDER BY LOWER(p.name)
        """, [user_id], return_format="Dict")

        project_data_list = []

        for project in projects:
            project_id = project["id"]

            # Fetch run parameters
            run_params = await self.pg.execute("""
                SELECT key, value FROM public.project_run_parameters WHERE project_id = $1
            """, [project_id], return_format="Dict")

            # Fetch input files
            files = await self.pg.execute("""
                SELECT file_type, file_name FROM public.project_files WHERE project_id = $1
            """, [project_id], return_format="Dict")
            files_dict = {f["file_type"]: f["file_name"] for f in files}

            # Fetch renderer config
            renderer = await self.pg.execute("""
                SELECT key, value FROM public.project_renderer WHERE project_id = $1
            """, [project_id], return_format="Dict")
            renderer_dict = {r["key"]: r["value"] for r in renderer}

            # Fetch planning unit metadata (optional)
            pu_metadata = {}
            if project.get("planning_unit_id"):
                df = await self.pg.execute("""
                    SELECT alias, description, domain, _area AS area, creation_date,
                        created_by, original_n AS country
                    FROM marxan.metadata_planning_units
                    WHERE unique_id = $1
                """, [project["planning_unit_id"]], return_format="DataFrame")

                if not df.empty:
                    row = df.iloc[0]
                    pu_metadata = {
                        'pu_alias': row.get('alias'),
                        'pu_description': row.get('description'),
                        'pu_domain': row.get('domain'),
                        'pu_area': row.get('area'),
                        'pu_creation_date': row.get('creation_date'),
                        'pu_created_by': row.get('created_by'),
                        'pu_country': row.get('country'),
                    }

            # Merge into full project data structure
            project_data_list.append({
                'id': project_id,
                'name': project["name"],
                'user_id': user_id,
                'description': project.get("description", "No description"),
                'createdate': project.get("date_created", "Unknown"),
                'oldVersion': project.get("old_version", False),
                'private': project.get("is_private", False),
                'costs': project.get("costs"),
                'iucn_category': project.get("iucn_category"),
                'metadata': {
                    "DESCRIPTION": project.get("description"),
                    "CREATEDATE": project.get("date_created"),
                    "OLDVERSION": project.get("old_version"),
                    "IUCN_CATEGORY": project.get("iucn_category"),
                    "PRIVATE": project.get("is_private"),
                    "COSTS": project.get("costs"),
                    "PLANNING_UNIT_NAME": pu_metadata.get("pu_alias"),
                    **pu_metadata
                },
                'files': files_dict,
                'runParameters': run_params,
                'renderer': renderer_dict
            })
        print('project_data_list: ', project_data_list)
        return project_data_list

    # async def get_project_data(pg, project_id, obj):
    #     """
    #     Fetches the project data from the database and sets it on the passed object
    #     in the projectData attribute. This replaces the old input.dat file parsing logic.

    #     Args:
    #         obj (BaseHandler): The request handler instance.
    #     Returns:
    #         None
    #     """

    #     # 1. Get main project record
    #     project_row = await pg.execute(
    #         """SELECT p.* FROM public.projects WHERE id = $1""",
    #         [project_id],
    #         return_format="Dict"
    #     )
    #     if not project_row:
    #         raise ServicesError(f"No project data found for project_id {project_id}")
    #     project_row = project_row[0]

    #     # 2. Get planning unit metadata (if set)
    #     pu_metadata = {}
    #     if project_row.get("planning_unit_id"):
    #         df = await pg.execute("""
    #             SELECT alias, description, domain, _area AS area, creation_date, created_by, original_n AS country
    #             FROM marxan.metadata_planning_units
    #             WHERE unique_id = $1
    #         """, [project_row["planning_unit_id"]], return_format="DataFrame")

    #         if not df.empty:
    #             row = df.iloc[0]
    #             pu_metadata = {
    #                 'pu_alias': row.get('alias'),
    #                 'pu_description': row.get('description'),
    #                 'pu_domain': row.get('domain'),
    #                 'pu_area': row.get('area'),
    #                 'pu_creation_date': row.get('creation_date'),
    #                 'pu_created_by': row.get('created_by'),
    #                 'pu_country': row.get('country'),
    #             }

    #     # 3. Get run parameters
    #     run_params = await pg.execute("""
    #         SELECT key, value FROM public.project_run_parameters WHERE project_id = $1
    #     """, [project_id], return_format="Dict")

    #     # 4. Get input files
    #     files = await pg.execute("""
    #         SELECT file_type, file_name FROM public.project_files WHERE project_id = $1
    #     """, [project_id], return_format="Dict")
    #     files_dict = {row["file_type"]: row["file_name"] for row in files}

    #     # 5. Get renderer settings
    #     renderer = await pg.execute("""
    #         SELECT key, value FROM public.project_renderer WHERE project_id = $1
    #     """, [project_id], return_format="Dict")
    #     renderer_dict = {r["key"]: r["value"] for r in renderer}

    #     # 6. Compose metadata dictionary
    #     metadata_dict = {
    #         "DESCRIPTION": project_row.get("description"),
    #         "CREATEDATE": project_row.get("created_at"),
    #         "OLDVERSION": project_row.get("old_version"),
    #         "IUCN_CATEGORY": project_row.get("iucn_category"),
    #         "PRIVATE": project_row.get("is_private"),
    #         "COSTS": project_row.get("costs"),
    #         "PLANNING_UNIT_NAME": pu_metadata.get("pu_alias"),  # for backward compatibility
    #         **pu_metadata
    #     }

    #     # 7. Attach to the object
    #     obj.projectData = {
    #         'project': project_row.get("name"),
    #         'metadata': metadata_dict,
    #         'files': files_dict,
    #         'runParameters': run_params,
    #         'renderer': renderer_dict
    #     }

    def get_costs(self):
        """
        Sets the custom cost profiles for a project in the costNames attribute of the given object.

        Args:
            obj (BaseHandler): The request handler instance with `input_folder` and `costNames` attributes.
        """
        # Retrieve all cost files in the input folder
        cost_files = glob.glob(join(self.input_folder, "*.cost"))

        # Extract and store the base names of the cost files
        cost_names = [splitext(basename(file))[
            0] for file in cost_files]

        # Append the default cost profile and sort the list
        cost_names = sorted(cost_names + ["Equal area"])
        self.costNames = cost_names

    async def post(self):
        """
        Handles POST requests for creating and updating projects.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'create':
                await self.create_project()
            elif action == 'create_import':
                await self.create_import_project()
            elif action == 'create_group':
                await self.create_project_group()
            elif action == 'update':
                await self.update_project_parameters()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def get(self):
        """
        Handles GET requests for various project-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'get':
                await self.get_project()
            elif action == 'list':
                await self.get_projects()
            elif action == 'list_with_grids':
                await self.get_projects_with_grids()
            elif action == 'clone':
                await self.clone_project()
            elif action == 'delete':
                await self.delete_project()
            elif action == 'delete_cluster':
                await self.delete_projects()
            elif action == 'rename':
                await self.rename_project()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    # POST /projects?action=create
    # Body:
    # {
    #     "user": "username",
    #     "project": "project_name",
    #     "description": "Project description",
    #     "planning_grid_name": "grid_name",
    #     "interest_features": "feature1,feature2",
    #     "target_values": "value1,value2",
    #     "spf_values": "spf1,spf2"
    # }

    async def create_project(self):
        self.validate_args(self.request.arguments, [
            'user', 'project', 'description', 'planning_grid_name', 'interest_features', 'target_values', 'spf_values'
        ])

        user = self.get_argument('user')
        project = self.get_argument('project')
        description = self.get_argument('description')
        planning_grid_name = self.get_argument('planning_grid_name')
        interest_features = self.get_argument('interest_features')
        target_values = self.get_argument('target_values')
        spf_values = self.get_argument('spf_values')

        self.create_project_folder(
            self, project, self.proj_paths.EMPTY_PROJECT_TEMPLATE_FOLDER)

        self.update_file_parameters(
            join(self.project_folder, "input.dat"),
            {
                'DESCRIPTION': description,
                'CREATEDATE': datetime.now().strftime("%d/%m/%y %H:%M:%S"),
                'PLANNING_UNIT_NAME': planning_grid_name
            }
        )

        await self.update_species(self, interest_features, target_values, spf_values, True)

        query = sql.SQL(
            """
            SELECT puid AS id, 1::double precision AS cost, 0::integer AS status
            FROM marxan.{}
            """
        ).format(sql.Identifier(planning_grid_name))

        await self.pg.execute(
            query,
            return_format="File",
            filename=join(self.input_folder, "pu.dat")
        )

        self.update_file_parameters(
            join(self.project_folder, "input.dat"),
            {'PUNAME': "pu.dat"}
        )

        self.send_response({
            'info': f"Project '{project}' created",
            'name': project,
            'user': user
        })

    # POST /projects?action=create_import
    # Body:
    # {
    #     "user": "username",
    #     "project": "project_name"
    # }
    async def create_import_project(self):
        self.validate_args(self.request.arguments, ['user', 'project'])

        project = self.get_argument('project')
        self.create_project_folder(
            self, project, self.proj_paths.EMPTY_PROJECT_TEMPLATE_FOLDER)

        self.send_response({
            'info': f"Project '{project}' created",
            'name': project
        })

    # GET /projects?action=get&user=username&project=project_name
    async def get_project(self):
        project_id = self.get_argument('projectId', None)

        try:
            project_id = int(project_id) if project_id else None
        except ValueError:
            raise ServicesError("Invalid project ID")

        project = await self.get_project_by_id(project_id) if project_id else None

        if project is None:
            raise ServicesError(f"That project does not exist")

        # Define project paths
        self.project = project
        self.folder_user = join("./users", self.current_user)
        self.project_path = join(self.folder_user, project['name']) + sep
        self.input_folder = join(self.project_path, "input") + sep

        # 1. Load project data
        self.projectData = await self.fetch_project_data(self.project_path)

        # 2. Load species data
        await self.get_species_data(self)

        # 3. Load and normalize planning unit data
        self.speciesPreProcessingData = file_to_df(
            join(self.input_folder, "feature_preprocessing.dat"))

        df = file_to_df(
            join(self.input_folder, self.projectData["files"]["PUNAME"]))
        self.planningUnitsData = normalize_dataframe(df, "status", "id")

        protected_areas_df = file_to_df(
            join(self.input_folder, "protected_area_intersections.dat"))
        self.protectedAreaIntersectionsData = normalize_dataframe(
            protected_areas_df, "iucn_cat", "puid")

        # 4. Get project costs
        self.get_costs()

        # 5. Update user data file - shouldnt need to do this - should be updating the db
        self.update_file_parameters(join(self.folder_user, "user.dat"), {
                                    'LASTPROJECT': project})

        data = {
            'user': self.current_user,
            'project': self.projectData['project'],
            'metadata': self.projectData['metadata'],
            'files': self.projectData['files'],
            'runParameters': self.projectData['runParameters'],
            'renderer': self.projectData['renderer'],
            'features': self.speciesData.to_dict(orient="records"),
            'feature_preprocessing': self.speciesPreProcessingData.to_dict(orient="split")["data"],
            'planning_units': self.planningUnitsData,
            'protected_area_intersections': self.protectedAreaIntersectionsData,
            'costnames': self.costNames
        }
        response = json.dumps(data, default=self.json_serial)
        self.send_response(response)

    async def get_first_project_by_user(self):
        """Fetch the first project associated with a user."""
        query = """
            SELECT p.*
            FROM projects p
            JOIN user_projects up ON p.id = up.project_id
            WHERE up.user_id = $1
            ORDER BY p.date_created ASC
            LIMIT 1;
        """
        result = await self.pg.execute(query, [self.current_user], return_format="Dict")
        project = result[0] if result else None

    async def fetch_project_data(self, project_path):
        """Fetches categorized project data from input.dat file."""
        input_file_params = ["PUNAME", "SPECNAME",
                             "PUVSPRNAME", "BOUNDNAME", "BLOCKDEF"]
        run_params = ['BLM', 'PROP', 'RANDSEED', 'NUMREPS', 'NUMITNS',
                      'STARTTEMP', 'NUMTEMP', 'COSTTHRESH', 'THRESHPEN1',
                      'THRESHPEN2', 'SAVERUN', 'SAVEBEST', 'SAVESUMMARY',
                      'SAVESCEN', 'SAVETARGMET', 'SAVESUMSOLN', 'SAVEPENALTY',
                      'SAVELOG', 'RUNMODE', 'MISSLEVEL', 'ITIMPTYPE', 'HEURTYPE',
                      'CLUMPTYPE', 'VERBOSITY', 'SAVESOLUTIONSMATRIX']
        metadata_params = ['DESCRIPTION', 'CREATEDATE', 'PLANNING_UNIT_NAME',
                           'OLDVERSION', 'IUCN_CATEGORY', 'PRIVATE', 'COSTS']
        renderer_params = ['CLASSIFICATION', 'NUMCLASSES',
                           'COLORCODE', 'TOPCLASSES', 'OPACITY']

        params_array, files_dict, metadata_dict, renderer_dict = [], {}, {}, {}

        key_mappings = {
            **{key: files_dict for key in input_file_params},
            **{key: params_array for key in run_params},
            **{key: renderer_dict for key in renderer_params},
            **{key: metadata_dict for key in metadata_params},
        }

        # Load input.dat file
        input_file_path = join(project_path, "input.dat")
        file_content = read_file(input_file_path)

        # Process file content
        for key in get_keys(file_content):
            key_value = get_key_value(file_content, key)

            if key in key_mappings:
                target_dict = key_mappings[key]

                value = key_value[1]

                # Convert datetime objects to ISO format
                if isinstance(value, datetime):
                    value = value.isoformat()

                if target_dict is params_array:
                    target_dict.append(
                        {'key': key_value[0], 'value': value})  # List case
                else:
                    target_dict[key_value[0]] = value  # Dictionary case

                if key == 'PLANNING_UNIT_NAME':
                    df = await self.pg.execute(
                        "SELECT * FROM marxan.get_planning_units_metadata($1)",
                        data=[key_value[1]], return_format="DataFrame")

                    if df.empty:
                        metadata_dict.update({
                            'pu_alias': key_value[1],
                            'pu_description': 'No description',
                            'pu_domain': 'Unknown domain',
                            'pu_area': 'Unknown area',
                            'pu_creation_date': 'Unknown date',
                            'pu_created_by': 'Unknown',
                            'pu_country': 'Unknown'
                        })
                    else:
                        row = df.iloc[0]
                        metadata_dict.update({
                            'pu_alias': row.get('alias', key_value[1]),
                            'pu_country': row.get('country', 'Unknown'),
                            'pu_description': row.get('description', 'No description'),
                            'pu_domain': row.get('domain', 'Unknown domain'),
                            'pu_area': row.get('area', 'Unknown area'),
                            'pu_creation_date': row.get('creation_date', 'Unknown date'),
                            'pu_created_by': row.get('created_by', 'Unknown')
                        })

        return {
            'project': self.project,
            'metadata': metadata_dict,
            'files': files_dict,
            'runParameters': params_array,
            'renderer': renderer_dict
        }

    async def get_projects(self):
        # if the user is an admin get all all_projects
        # if the user isnt an admin get all projects for user

        self.validate_args(self.request.arguments, ['user'])
        try:
            user_id = int(self.get_secure_cookie("user_id"))
            self.projects = await self.get_projects_for_user(user_id)

        except AttributeError:
            print("AttributeError - user_id error")
            raise ServicesError(f"The user does not exist.")

        self.send_response({"projects": self.projects})

    # GET /projects?action=list_with_grids
    async def get_projects_with_grids(self):
        user_folder = self.proj_paths.USERS_FOLDER
        matches = [
            join(root, filename)
            for root, _, filenames in walk(user_folder)
            for filename in fnmatch.filter(filenames, 'input.dat')
        ]

        projects = []
        for match in matches:
            user = match[len(user_folder):].split(sep)[0]
            project = match.split(sep)[-2]
            values = get_key_values_from_file(match)

            projects.append({
                'user': user,
                'project': project,
                'feature_class_name': values['PLANNING_UNIT_NAME'],
                'description': values['DESCRIPTION']
            })

        df = pd.DataFrame(projects).set_index("feature_class_name")
        grids = await self.pg.execute(get_pu_grids_query, return_format="Dict")
        df2 = pd.DataFrame(grids).set_index("feature_class_name")
        df = df.join(df2).replace({pd.NA: None})

        self.send_response({
            'info': "Projects data returned",
            'data': df.to_dict(orient="records")
        })

    # GET /projects?action=clone&user=username&project=project_name

    async def clone_project(self):
        self.validate_args(self.request.arguments, ['user', 'project'])

        cloned_name = clone_a_project(self.project_folder, self.folder_user)

        self.send_response({
            'info': f"Project '{cloned_name}' created",
            'name': cloned_name
        })

    # GET /projects?action=delete&user=username&project=project_name
    async def delete_project(self):
        self.validate_args(self.request.arguments, ['user', 'project'])

        await self.get_projects()

        if len(self.projects) == 1:
            raise ServicesError("You cannot delete all projects")

        # Validate that the project folder exists before attempting to delete it
        if not exists(self.project_folder):
            raise ServicesError(f"The project folder does not exist.")

        try:
            shutil.rmtree(self.project_folder)
        except Exception as e:  # Catching all exceptions is a general approach
            raise ServicesError(f"Error deleting project folder: {e}") from e

        # Optionally, you could log the deletion
        print(f"Successfully deleted project folder: {self.project_folder}")
        self.send_response({
            'info': f"Project '{self.get_argument('project')}' deleted",
            'project': self.get_argument('project')
        })

    # GET /projects?action=delete_cluster&projectNames=project1,project2,project3
    async def delete_projects(self):
        self.validate_args(self.request.arguments, ['projectNames'])

        project_names = self.get_argument("projectNames").split(",")
        for project_name in project_names:
            project_path = join(self.proj_paths.CLUMP_FOLDER, project_name)
            if exists(project_path):
                shutil.rmtree(project_path)

        self.send_response({"info": "Projects deleted"})

    # GET /projects?action=rename&user=username&project=project_name&newName=new_project_name
    async def rename_project(self):
        self.validate_args(self.request.arguments, [
                           'user', 'project', 'newName'])
        new_name = self.get_argument('newName')

        rename(self.project_folder, join(self.folder_user, new_name))

        self.update_file_parameters(
            join(self.folder_user, "user.dat"),
            {'LASTPROJECT': new_name}
        )

        self.send_response({
            'info': f"Project renamed to '{new_name}'",
            'project': self.get_argument('project')
        })

    async def create_project_group(self):
        self.validate_args(self.request.arguments, [
                           'user', 'project', 'copies', 'blmValues'])

        blm_values = self.get_argument("blmValues").split(",")
        projects = []

        for i in range(int(self.get_argument("copies"))):
            project_name = uuid.uuid4().hex
            projects.append({'projectName': project_name, 'clump': i})
            shutil.copytree(self.project_folder, join(
                self.proj_paths.CLUMP_FOLDER, project_name))

            delete_all_files(
                join(self.proj_paths.CLUMP_FOLDER, project_name, "output"))

            self.update_file_parameters(
                join(self.proj_paths.CLUMP_FOLDER, project_name, "input.dat"),
                {'BLM': blm_values[i], 'NUMREPS': '1'}
            )

        self.send_response({
            'info': "Project group created",
            'data': projects
        })

    # POST /projects?action=update
    # Body:
    # {
    #     "user": "username",
    #     "project": "project_name",
    #     "param1": "value1",
    #     "param2": "value2"
    # }
    async def update_project_parameters(self):
        self.validate_args(self.request.arguments, ['user', 'project'])

        params = {
            argument: self.get_argument(argument)
            for argument in self.request.arguments
            if argument not in ['user', 'project', 'callback']
        }

        self.update_file_parameters(
            join(self.project_folder, "input.dat"), params)

        self.send_response({
            'info': ", ".join(params.keys()) + " parameters updated"
        })
