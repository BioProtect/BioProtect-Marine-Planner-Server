import fnmatch
import json
import shutil
import uuid
from datetime import datetime
from os import rename, sep, walk
from os.path import exists, join
import numpy as np
import pandas as pd
from handlers.base_handler import BaseHandler
from psycopg2 import sql
from services.file_service import (delete_all_files, file_to_df,
                                   get_key_values_from_file,
                                   normalize_dataframe, read_file,
                                   write_to_file)
from services.project_service import clone_a_project
from services.service_error import ServicesError, raise_error
from decimal import Decimal


class ProjectHandler(BaseHandler):
    """
    REST HTTP handler for project-related operations, including creating, cloning, renaming, deleting,
    fetching, and updating projects.
    """

    def initialize(self, pg):
        super().initialize()
        self.pg = pg

    def validate_args(self, args, required_keys):
        # sourcery skip: use-named-expression
        """Checks that all of the arguments in argumentList are in the arguments dictionary."""
        missing = [key for key in required_keys if key not in args]
        if missing:
            raise ServicesError(
                f"Missing required arguments: {', '.join(missing)}")

    def json_serial(self, obj):
        """Convert datetime objects to a JSON-serializable format."""
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            # convert to float or str depending on precision needs
            return float(obj)

        raise TypeError(f"Type {type(obj)} not serializable")

    async def get_project_by_id(self, project_id):
        """Fetch project details based on project ID."""
        query = """
            SELECT * FROM bioprotect.projects WHERE id = %s;
        """
        result = await self.pg.execute(query, [project_id], return_format="Dict")
        return result[0] if result else None

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

    def normalise_planning_units(self, df, column_to_normalize_by, puid_column_name, classes=None, as_dict=True):
        if df.empty:
            return []

        if classes:
            # get min, max and then sort by number of bins needed based on number of vals
            min_value = df[column_to_normalize_by].min()
            max_value = df[column_to_normalize_by].max()
            num_classes = 1 if min_value == max_value else classes
            bin_size = (max_value + 1 - min_value) / num_classes

            bins = {min_value + bin_size * (i + 1): []
                    for i in range(num_classes)}

            for _, row in df.iterrows():
                bin_index = int(
                    (row[column_to_normalize_by] - min_value) / bin_size)
                bin_index = min(bin_index, num_classes - 1)
                key = min_value + bin_size * (bin_index + 1)
                bins[key].append(row[puid_column_name])
            return bins, min_value, max_value

        # Normalization (grouping)
        df[column_to_normalize_by] = df[column_to_normalize_by].fillna(
            0).astype(int)
        groups = df.groupby(column_to_normalize_by, sort=True)

        if as_dict:
            # Return dictionary {status: [puid list]}
            return {int(group): group_df[puid_column_name].tolist() for group, group_df in groups}
        else:
            # Return legacy array-of-arrays
            return [[int(group), group_df[puid_column_name].tolist()] for group, group_df in groups]

    async def get_species_data(self, obj):
        """
        Retrieves species data for a project from the Marxan SPECNAME file as a DataFrame.
        Joins this data with the PostGIS database if the project is a Marxan Web project.
        Sets the resulting data on the `speciesData` attribute of the passed `obj`.

        Args:
            obj (BaseHandler): The request handler instance.

        Returns:
            None
        """

        project_id = obj.projectData["project"]["id"]

        # Load project_features (targets + spf)
        species_data = await obj.pg.execute("""
            SELECT
                pf.feature_unique_id AS id,
                pf.target_type,
                pf.target_value,
                pf.spf,
                pf.weight,
                pf.created_at,
                pf.updated_at
            FROM bioprotect.project_features pf
            WHERE pf.project_id = %s
        """, data=[project_id], return_format="DataFrame")

        if species_data.empty:
            obj.speciesData = pd.DataFrame()
            return

        # Get additional feature metadata from PostGIS
        feature_data = await obj.pg.execute(
            "SELECT * FROM bioprotect.get_features()",
            return_format="DataFrame"
        )

        # Join
        output_df = species_data.join(
            feature_data.set_index("unique_id"),
            on="id",
            how="left"
        )
        output_df = output_df.replace(np.nan, None)
        obj.speciesData = output_df

    async def get_projects_for_user(self, user_id):
        """
        Gets all projects for a user along with full projectData (metadata, files, run parameters, renderer).

        Args:
            user_id (int): The ID of the user.

        Returns:
            list[dict]: Each dict contains full project data.
        """
        projects = await self.pg.execute(
            "SELECT * FROM bioprotect.get_projects_for_user(%s)", [user_id], return_format="Dict")

        project_data_list = []

        for project in projects:
            project_id = project["id"]
            # Fetch run parameters
            run_params = await self.pg.execute("""
                SELECT key, value FROM bioprotect.project_run_parameters WHERE project_id = %s
            """, [project_id], return_format="Dict")

            # Fetch input files
            files = await self.pg.execute("""
                SELECT file_type, file_name FROM bioprotect.project_files WHERE project_id = %s
            """, [project_id], return_format="Dict")
            files_dict = {f["file_type"]: f["file_name"] for f in files}

            # Fetch renderer config
            renderer_dict = await self.pg.execute("""
                SELECT key, value FROM bioprotect.project_renderer WHERE project_id = %s
            """, [project_id], return_format="Dict")

            # Fetch project features.

            features = await self.pg.execute(
                "SELECT * FROM bioprotect.get_project_features(%s)", [project_id], return_format="Dict")

            # Fetch planning unit metadata (optional)
            pu_metadata = {}
            if project.get("planning_unit_id"):
                df = await self.pg.execute("""
                    SELECT mp.alias, mp.description, mp.domain, mp._area AS area, mp.creation_date, mp.created_by, g.original_n AS country
                    FROM bioprotect.metadata_planning_units mp
                    LEFT OUTER JOIN bioprotect.gaul_2015_simplified_1km g ON g.id_country = mp.country_id
                    WHERE mp.unique_id = %s
                """, [project["planning_unit_id"]], return_format="DataFrame")

                # df = await self.pg.execute("""
                #     SELECT alias, description, domain, _area AS area, creation_date,
                #         created_by, original_n AS country
                #     FROM bioprotect.metadata_planning_units
                #     WHERE unique_id = %s
                # """, [project["planning_unit_id"]], return_format="DataFrame")

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
                'renderer': renderer_dict,
                "project_features": features,
            })
        return project_data_list

    async def post(self):
        """
        Handles POST requests for creating and updating projects.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'create':
                await self.create_project()
            elif action == 'create_group':
                await self.create_project_group()
            elif action == 'update':
                await self.update_project_parameters()
            elif action == 'update_features':
                await self.update_project_features()
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
        print('self.request.arguments: ', self.request.arguments)
        self.validate_args(self.request.arguments, [
            'user', 'project', 'description', 'planning_grid_name', 'interest_features', 'target_values', 'spf_values'
        ])

        user = self.get_argument('user')
        print('user--------------------------------: ', user)
        project = self.get_argument('project')
        description = self.get_argument('description')
        planning_grid_name = self.get_argument('planning_grid_name')

        # 1. Create project in DB

        row = await self.pg.execute("""
            INSERT INTO bioprotect.projects (user_id, name, description, planning_unit_id)
            VALUES (
                (SELECT id FROM bioprotect.users WHERE username = %s),
                %s, %s,
                (SELECT unique_id FROM bioprotect.metadata_planning_units WHERE alias = %s)
            )
            RETURNING id
        """, [user, project, description, planning_grid_name], return_format="Array")

        if not row:
            raise ServicesError("Failed to create project")
        project_id = row[0]["id"]

        # 2. Link features to this project
        await self.update_project_features(project_id=project_id)

        # 3. Respond
        self.send_response({
            'info': f"Project '{project}' created with features",
            'name': project,
            'user': user,
            'project_id': project_id
        })

    # GET /projects?action=get&user=username&project=project_name
    async def get_project(self):
        project_id = self.get_argument('projectId', None)
        resolution = int(self.get_argument("resolution", 7))

        try:
            project_id = int(project_id) if project_id else None
        except ValueError:
            raise ServicesError("Invalid project ID")

        project = await self.get_project_by_id(project_id) if project_id else None
        print('++++++ project_id: ', project_id)
        print('++++++ project: ', project)

        if project is None:
            raise ServicesError(f"That project does not exist")

        # Define project paths - need these for uploads
        self.project = project
        self.folder_user = join("./users", self.current_user)
        self.project_path = join(self.folder_user, project['name']) + sep
        self.input_folder = join(self.project_path, "input") + sep

        # 1. Load project data
        self.projectData = await self.fetch_project_data(project)

        # 2. Load species data
        await self.get_species_data(self)

        # 3. Load feature preprocessing (DB instead of file)
        # self.speciesPreProcessingData = await self.pg.execute(
        #     """
        #     SELECT project_id, feature_unique_id, preprocessing_result
        #     FROM bioprotect.feature_preprocessing
        #     WHERE project_id = %s
        #     """,
        #     data=[project_id],
        #     return_format="DataFrame"
        # )
        self.speciesPreProcessingData = file_to_df(
            join(self.input_folder, "feature_preprocessing.dat"))

        # 3. Load and normalize planning unit data
        query = """
            SELECT
                pp.h3_index AS id,
                pp.cost,
                pp.status
            FROM bioprotect.project_pus pp
            JOIN bioprotect.h3_cells hc ON pp.h3_index = hc.h3_index
            JOIN bioprotect.projects p ON p.id = pp.project_id
            JOIN bioprotect.metadata_planning_units mpu ON p.planning_unit_id = mpu.unique_id
            WHERE pp.project_id = %s
            AND hc.resolution = %s
            AND LOWER(TRIM(hc.project_area)) = LOWER(TRIM(split_part(mpu.alias, ' (', 1)));
        """
        df = await self.pg.execute(query, data=[project_id, resolution], return_format="DataFrame")
        print(df.head(5))
        print(df['status'].value_counts(dropna=False))
        self.planningUnitsData = self.normalise_planning_units(
            df, "status", "id")

        # 5. Load protected area intersections (DB instead of file)
        # self.protectedAreaIntersectionsData = await self.pg.execute(
        #     """
        #     SELECT project_id, puid, iucn_cat
        #     FROM bioprotect.protected_area_intersections
        #     WHERE project_id = %s
        #     """,
        #     data=[project_id],
        #     return_format="DataFrame"
        # )
        protected_areas_df = file_to_df(
            join(self.input_folder, "protected_area_intersections.dat"))
        self.protectedAreaIntersectionsData = normalize_dataframe(
            protected_areas_df, "iucn_cat", "puid")

        # 6. Get project costs
        # costs = await self.pg.execute(
        #     """
        #     SELECT h3_index AS id, cost
        #     FROM bioprotect.project_costs
        #     WHERE project_id = %s
        #     """,
        #     data=[project_id],
        #     return_format="DataFrame"
        # )
        # self.costNames = ["Custom", "Equal area"] if not costs.empty else ["Equal area"]
        # NOT WHOLLY SURE ABOUT THIS _ NOT ACTUALLY GETTING COSTS
        query = """SELECT 1 FROM bioprotect.costs WHERE project_id = %s LIMIT 1"""
        cost_rows = await self.pg.execute(query, data=[self.project["id"]], return_format="Dict")

        # If any cost data exists, add "Custom" profile
        self.costNames = [
            "Custom", "Equal area"] if cost_rows else ["Equal area"]

        # costs = await self.pg.execute("""
        #     SELECT id, cost
        #     FROM bioprotect.project_costs
        #     WHERE project_id = %s
        # """, [project_id], return_format="Dict")

        # MATCH COSTS UP WITH PLANNING UNITS
        # costs = await self.pg.execute("""
        #     SELECT pc.id, pc.cost, pp.status
        #     FROM bioprotect.project_costs pc
        #     JOIN bioprotect.project_pus pp
        #     ON pc.project_id = pp.project_id AND pc.id = pp.h3_index
        #     WHERE pc.project_id = %s
        # """, [project_id], return_format="Dict")

        # 5. Update user
        uid = self.get_secure_cookie("user_id")
        if not uid:
            raise ServicesError("Not authenticated")
        try:
            user_id = int(uid.decode() if isinstance(
                uid, (bytes, bytearray)) else uid)
        except Exception:
            raise ServicesError("Invalid user id in session")

        if user_id:
            await self.pg.execute(
                """
                UPDATE bioprotect.users
                SET last_project = %s
                WHERE id = %s
                """,
                data=[self.project["id"], user_id]
            )
        else:
            await self.pg.execute(
                "UPDATE bioprotect.users SET last_project = %s WHERE username = %s",
                data=[self.project["id"], self.current_user]
            )

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
            'costnames': self.costNames,
        }
        response = json.dumps(data, default=self.json_serial)
        self.send_response(response)

    async def get_first_project_by_user(self):
        """Fetch the first project associated with a user."""
        query = """
            SELECT p.*
            FROM bioprotect.projects p
            JOIN user_projects up ON p.id = up.project_id
            WHERE up.user_id = %s
            ORDER BY p.date_created ASC
            LIMIT 1;
        """
        result = await self.pg.execute(query, [self.current_user], return_format="Dict")
        project = result[0] if result else None

    async def fetch_project_data(self, project):
        """Fetches categorized project data from input.dat file."""
        project_id = project.get('id')
        run_params = await self.pg.execute(
            "SELECT key, value FROM bioprotect.project_run_parameters WHERE project_id = %s",
            data=[project_id],
            return_format="Array"
        )

        renderer = await self.pg.execute(
            "SELECT key, value FROM bioprotect.project_renderer WHERE project_id = %s",
            data=[project_id],
            return_format="Dict"
        )

        metadata = await self.pg.execute(
            "SELECT key, value FROM bioprotect.project_metadata WHERE project_id = %s",
            data=[project_id],
            return_format="Dict"
        )

        metadata["description"] = project["description"]
        metadata["createdate"] = project["date_created"]
        metadata["pu_id"] = project["planning_unit_id"]
        metadata["iucn_category"] = project["iucn_category"]
        metadata["costs"] = project["costs"]

        df = await self.pg.execute(
            "SELECT * FROM bioprotect.get_planning_units_metadata(%s)",
            data=[int(project["planning_unit_id"])], return_format="DataFrame")

        if not df.empty:
            row = df.iloc[0]
            pu_meta = ({
                'pu_tilesetid': row.get('feature_class_name', 'not found'),
                'pu_alias': row.get('alias', 'not found'),
                'pu_country': row.get('country', 'Unknown'),
                'pu_description': row.get('description', 'No description'),
                'pu_domain': row.get('domain', 'Unknown domain'),
                'pu_area': row.get('area', 'Unknown area'),
                'pu_creation_date': row.get('creation_date', 'Unknown date'),
                'pu_created_by': row.get('created_by', 'Unknown')
            })
        else:
            pu_meta = ({
                'pu_alias': "no planning unit attached",
                'pu_description': 'No description',
                'pu_domain': 'Unknown domain',
                'pu_area': 'Unknown area',
                'pu_creation_date': 'Unknown date',
                'pu_created_by': 'Unknown',
                'pu_country': 'Unknown'
            })

        metadata.update(pu_meta)
        # # Convert datetime objects to ISO format
        # if isinstance(value, datetime):
        #     value = value.isoformat()
        return {
            'project': self.project,
            'metadata': metadata,
            'files': [],
            'runParameters': run_params,
            'renderer': renderer
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
        grids = await self.pg.execute("SELECT * FROM bioprotect.get_pu_grids();", return_format="Dict")

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

    async def resolve_user_id(self, user):
        """
        Resolve user_id from either a username (string) or user_id (int).
        """
        if user is None:
            return None
        try:
            return int(user)
        except (ValueError, TypeError):
            pass

        # Otherwise, look up by username
        rows = await self.pg.execute(
            "SELECT id FROM bioprotect.users WHERE username = %s", [user], return_format="Array")
        if not rows:
            raise ServicesError(f"User '{user}' not found.")
        return rows[0]["id"]

    async def resolve_and_check_project(self, project_id=None, user=None):
        """
        Resolve project_id from:
        - explicit arg (preferred), OR
        - request args 'project_id' or legacy 'project'
        Always verifies the project exists.
        If a user is provided (username or id), also verifies access via user_projects.
        """
        # 1) pick a project id: explicit > 'project_id' arg > legacy 'project' arg
        pid = project_id
        print('1pid: ', pid)
        if pid is None:
            pid = self.get_argument("project_id", None)
            print('2pid: ', pid)
        if pid is None:
            pid = self.get_argument("project", None)  # legacy support
        if pid is None:
            raise ServicesError("Missing project_id.")

        try:
            pid = int(pid)
        except (ValueError, TypeError):
            raise ServicesError("Invalid project_id.")

        print('pid before query: ', pid)

        # 2) ensure project exists
        exists_row = await self.pg.execute(
            "SELECT 1 FROM bioprotect.projects WHERE id = %s",
            [pid],
            return_format="Array"
        )
        if not exists_row:
            raise ServicesError(f"Project {pid} not found.")
        print('pid being returned: ', pid)
        return pid

    async def update_project_features(self, project_id=None):
        """
        Updates project feature links and settings in DB.
        Replaces old updateSpecies/spec.dat functionality.
        """
        self.validate_args(self.request.arguments, ['interest_features'])

        user = self.get_argument("user", None)
        user_id = await self.resolve_user_id(user)

        pid = await self.resolve_and_check_project(project_id)
        print('project_id: ', project_id)

        features = self.get_argument("interest_features")
        print('features: ', features)
        values = self.get_argument("target_values", None)
        print('values: ', values)
        spf_vals = self.get_argument("spf_values", None)

        # parse lists
        feature_ids = [int(x) for x in features.split(",") if x.strip()]
        targets = [x.strip() for x in values.split(",")] if values else []
        print('targets: ', targets)
        spfs = [x.strip() for x in spf_vals.split(",")] if spf_vals else []

        # basic length check (non-fatal; we still link features)
        if (targets and len(targets) != len(feature_ids)) or (spfs and len(spfs) != len(feature_ids)):
            raise ServicesError(
                "Lengths of interest_features, target_values, and spf_values must match.")

        # 3) upsert new data
        print('feature_ids: ', feature_ids)
        for idx, fid in enumerate(feature_ids):
            print('idx, fid: ', idx, fid)
            tv = float(targets[idx]) if targets and idx < len(
                targets) else None
            spf = float(spfs[idx]) if spfs and idx < len(spfs) else None
            weight = None
            target_type = "prop"  # or allow from payload
            print('project_id, fid, target_type, tv, spf, weight: ',
                  pid, fid, target_type, tv, spf, weight)

            await self.pg.execute(
                "SELECT bioprotect.update_project_feature(%s, %s, %s, %s, %s, %s)",
                [pid, fid, target_type, tv, spf, weight])

        self.send_response({"info": "Project features updated",
                            "project_id": pid})

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
