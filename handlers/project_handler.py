import json
from datetime import datetime
from os import sep
from os.path import join
import numpy as np
import pandas as pd
from handlers.base_handler import BaseHandler
from psycopg2 import sql
from services.file_service import (read_file, write_to_file)
from services.service_error import ServicesError, raise_error
from decimal import Decimal


class ProjectHandler(BaseHandler):
    """
    REST HTTP handler for project-related operations, including creating, cloning, renaming, deleting,
    fetching, and updating projects.
    """

    def initialize(self, pg):
        super().initialize(pg=pg)

    # ----------------------------
    # Utility
    # ----------------------------

    async def _get_authenticated_user_id(self):
        uid = self.get_secure_cookie("user_id")
        if not uid:
            raise ServicesError("Not authenticated")

        try:
            return int(uid.decode() if isinstance(uid, (bytes, bytearray)) else uid)
        except Exception:
            raise ServicesError("Invalid session user")

    async def _resolve_planning_unit_id(self, planning_grid_name):
        """
        planning_grid_name coming from frontend is tilesetid
        which corresponds to metadata_planning_units.alias or tilesetid
        """
        row = await self.pg.execute(
            """
            SELECT unique_id
            FROM bioprotect.metadata_planning_units
            WHERE alias = %s OR tilesetid = %s
            """,
            [planning_grid_name, planning_grid_name],
            return_format="Dict"
        )

        if not row:
            raise ServicesError("Planning grid not found")

        return row[0]["unique_id"]

    async def _check_project_access(self, user_id, project_id, min_role=None):
        """
        Ensures user has access to project.
        Optionally enforce minimum role: owner > editor > viewer
        """
        row = await self.pg.execute(
            """
            SELECT role
            FROM bioprotect.user_projects
            WHERE user_id = %s AND project_id = %s
            """,
            [user_id, project_id],
            return_format="Dict"
        )

        if not row:
            raise ServicesError("Access denied")

        role = row[0]["role"]

        if min_role:
            hierarchy = {"viewer": 1, "editor": 2, "owner": 3}
            if hierarchy[role] < hierarchy[min_role]:
                raise ServicesError("Insufficient permissions")

        return role

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

    async def post(self):
        """
        Handles POST requests for creating and updating projects.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'create':
                await self.create_project()
            elif action == 'update':
                await self.update_project_parameters()
            elif action == 'update_features':
                await self.update_project_features()
            elif action == 'rename':
                await self.rename_project()
            elif action == 'delete':
                await self.delete_project()
            elif action == 'clone':
                await self.clone_project()
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

            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def get_project_by_id(self, project_id):
        """
        Fetch project details based on project ID.
        """
        query = "SELECT * FROM bioprotect.projects WHERE id = %s;"
        result = await self.pg.execute(query, [project_id], return_format="Array")
        return result[0] if result else None

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

    # ----------------------------
    # POST /projects?action=create
    # ----------------------------
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
        user_id = await self._get_authenticated_user_id()
        data = json.loads(self.request.body or "{}")

        name = data.get("project")
        description = data.get("description")
        planning_grid_name = data.get("planning_grid_name")
        interest_features = data.get("interest_features", [])
        target_values = data.get("target_values", [])
        spf_values = data.get("spf_values", [])

        if not name or not planning_grid_name:
            raise ServicesError(
                "Missing required fields: project, planning_grid_name")

        planning_unit_id = await self._resolve_planning_unit_id(planning_grid_name)

        # Create project in DB
        row = await self.pg.execute(
            """
            INSERT INTO bioprotect.projects
                (name, description, planning_unit_id)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            [name, description, planning_unit_id],
            return_format="Array"
        )

        if not row:
            raise ServicesError("Failed to create project")
        project_id = row[0]["id"]

        # 2 Link owner
        await self.pg.execute(
            """
            INSERT INTO bioprotect.user_projects
                (user_id, project_id, role)
            VALUES (%s, %s, 'owner')
            """,
            [user_id, project_id]
        )

        # Normalise features incase its a list or a csv str
        if isinstance(interest_features, str):
            feature_ids = [int(x)
                           for x in interest_features.split(",") if x.strip()]
        else:
            feature_ids = [int(x) for x in interest_features]

        if isinstance(target_values, str):
            targets = [x.strip()
                       for x in target_values.split(",") if x.strip()]
        else:
            targets = target_values or []

        if isinstance(spf_values, str):
            spfs = [x.strip() for x in spf_values.split(",") if x.strip()]
        else:
            spfs = spf_values or []

        # Link features to this project
        if feature_ids:
            await self._update_project_features_in_db(project_id, feature_ids, targets, spfs)

        # return info
        self.send_response({
            'info': f"Project '{name}' created with features",
            'name': name,
            'user': user_id,
            'project_id': project_id
        })

    # --------------------------------------------------
    # GET /projects?action=get&project_id=#
    # --------------------------------------------------

    async def get_project(self):
        user_id = await self._get_authenticated_user_id()
        project_id = self.get_argument('projectId', None)

        await self._check_project_access(user_id, project_id)

        try:
            project_id = int(project_id) if project_id else None
        except ValueError:
            raise ServicesError("Invalid project ID")

        # 1 - gets project details from the project table in the database
        project = await self.get_project_by_id(project_id) if project_id else None
        if project is None:
            raise ServicesError(f"That project does not exist")

        # 2 - Define project paths - need these for uploads
        ######################################################################
        # NEED TO CHANGE THIS
        # NEED PROJECT FOLDERS TO UPLOAD BUT GENERALLY NEED TO MOVE AWAY FROM FILE BASED STORAGE
        ######################################################################
        self.project = project
        self.folder_user = join("./users", self.current_user)
        self.project_path = join(self.folder_user, project['name']) + sep
        self.input_folder = join(self.project_path, "input") + sep

        # 3 - Load project data
        self.projectData = await self.fetch_project_data(project)

        # 4 - Load species data
        output_df = await self.pg.execute(
            "SELECT * FROM bioprotect.get_project_species(%s)",
            data=[project_id],
            return_format="DataFrame"
        )
        self.speciesData = output_df.replace(np.nan, None)

        # 5 - Load feature preprocessing (DB instead of file)
        self.speciesPreProcessingData = await self.pg.execute(
            """
            SELECT
                fp.project_id,
                fp.feature_unique_id,
                fp.pu_area,
                fp.pu_count
            FROM bioprotect.feature_preprocessing fp
            WHERE fp.project_id = %s
            """,
            data=[project_id],
            return_format="DataFrame"
        )

        # 6 - Load and normalize planning unit data
        # This is just the status values
        # THIS IS A PGADMIN FUNCTION
        df = await self.pg.execute(
            "SELECT * FROM bioprotect.get_planning_units_for_project(%s)",
            data=[project_id],
            return_format="DataFrame"
        )
        self.planningUnitsData = self.normalise_planning_units(
            df, "status", "h3_index")

        # 6. Load cost profiles for the project
        profiles = await self.pg.execute(
            """
            SELECT
                cp.id,
                cp.name,
                cp.description,
                cp.is_default,
                (cp.id = p.active_cost_profile_id) AS is_active
            FROM bioprotect.cost_profiles cp
            JOIN bioprotect.projects p
            ON p.id = cp.project_id
            WHERE cp.project_id = %s
            ORDER BY cp.is_default DESC, cp.name;
            """,
            data=[project_id],
            return_format="Dict"
        )
        # List of names for existing UI
        self.costNames = [row["name"] for row in profiles]
        # Full profile objects for updated UI / future work
        self.costProfiles = profiles

        ##################################################################
        # 5. Update user
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
                """
                UPDATE bioprotect.users 
                SET last_project = %s 
                WHERE username = %s
                """,
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
            'costnames': self.costNames,
            'costProfiles': self.costProfiles,  # new full metadata
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
        return project

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

    # --------------------------------------------------
    # GET /projects?action=list
    # --------------------------------------------------
    async def get_projects(self):
        # if the user is an admin get all all_projects
        # if the user isnt an admin get all projects for user
        user_id = await self._get_authenticated_user_id()
        try:
            self.projects = await self.get_projects_for_user(user_id)
        except AttributeError:
            print("AttributeError - user_id error")
            raise ServicesError(f"The user does not exist.")

        self.send_response({"projects": self.projects})

    # GET /projects?action=clone&user=username&project=project_name
    async def clone_project(self):
        return

# ----------------------------
    # POST /projects?action=delete
    # ----------------------------

    async def delete_project(self):
        user_id = await self._get_authenticated_user_id()

        # Support JSON and query arg
        try:
            data = json.loads(self.request.body or "{}")
        except Exception:
            data = {}

        project_id = data.get("project_id") or self.get_argument(
            "project_id", None)
        if not project_id:
            raise ServicesError("Missing project_id")

        try:
            project_id = int(project_id)
        except ValueError:
            raise ServicesError("Invalid project_id")

        await self._check_project_access(user_id, project_id, min_role="owner")

        await self.pg.execute(
            "DELETE FROM bioprotect.projects WHERE id = %s",
            [project_id]
        )

        self.send_response({"info": "Project deleted"})

    # POST `projects?action=rename

    async def rename_project(self):
        user_id = await self._get_authenticated_user_id()

        # Support JSON and query args
        try:
            data = json.loads(self.request.body or "{}")
        except Exception:
            data = {}

        project_id = data.get("project_id") or self.get_argument(
            "project_id", None)
        new_name = data.get("newName") or self.get_argument("newName", None)

        if not project_id or not new_name:
            raise ServicesError("Missing project_id or newName")

        try:
            project_id = int(project_id)
        except ValueError:
            raise ServicesError("Invalid project_id")

        await self._check_project_access(user_id, project_id, min_role="owner")

        # Perform rename
        await self.pg.execute(
            """
            UPDATE bioprotect.projects
            SET name = %s
            WHERE id = %s
            """,
            [new_name, project_id]
        )

        self.send_response({
            "info": f"Project renamed to '{new_name}'",
            "project_id": project_id,
            "new_name": new_name
        })

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

        # 1) explicit param wins
        if pid is None:
            # 2) try JSON body
            try:
                data = json.loads(self.request.body or "{}")
            except Exception:
                data = {}
            pid = data.get("project_id")

        # 3) query/form args
        if pid is None:
            pid = self.get_argument("project_id", None)
        if pid is None:
            pid = self.get_argument("project", None)  # legacy support
        if pid is None:
            raise ServicesError("Missing project_id.")

        try:
            pid = int(pid)
        except (ValueError, TypeError):
            raise ServicesError("Invalid project_id.")

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

    async def _update_project_features_in_db(self, pid, feature_ids, targets, spfs):
        """
        Core logic: write project features to DB using bioprotect.update_project_feature.
        feature_ids: list[int]
        targets: list[str|float] or []
        spfs: list[str|float] or []
        """
        # Basic length check
        if (targets and len(targets) != len(feature_ids)) or (spfs and len(spfs) != len(feature_ids)):
            raise ServicesError(
                "Lengths of features, targets, and spf must match.")

        for idx, fid in enumerate(feature_ids):
            tv = float(targets[idx]) if targets and idx < len(
                targets) else None
            spf = float(spfs[idx]) if spfs and idx < len(spfs) else None
            weight = None
            target_type = "prop"  # later if you want to support other types

            await self.pg.execute(
                "SELECT bioprotect.update_project_feature(%s, %s, %s, %s, %s, %s)",
                [pid, fid, target_type, tv, spf, weight],
            )

    # ----------------------------
    # POST /projects?action=update_features
    # ----------------------------
    # JSON Body:
    # {
    #     "project_id": 123,
    #     "interest_features": [1, 2, 3],
    #     "target_values": [10, 20, 30],
    #     "spf_values": [1, 2, 3]
    # }

    async def update_project_features(self, project_id=None):
        """
        Updates project feature links and settings in DB.
        Replaces old updateSpecies/spec.dat functionality.
        """
        try:
            data = json.loads(self.request.body or "{}")
        except Exception:
            data = {}

        pid = data.get("project_id") or project_id
        pid = await self.resolve_and_check_project(pid)

        interest_features = data.get("interest_features", [])
        target_values = data.get("target_values", [])
        spf_values = data.get("spf_values", [])

        # Normalise possible CSV -> lists, but prefer lists
        if isinstance(interest_features, str):
            feature_ids = [int(x)
                           for x in interest_features.split(",") if x.strip()]
        else:
            feature_ids = [int(x) for x in interest_features]

        if isinstance(target_values, str):
            targets = [x.strip()
                       for x in target_values.split(",") if x.strip()]
        else:
            targets = target_values or []

        if isinstance(spf_values, str):
            spfs = [x.strip() for x in spf_values.split(",") if x.strip()]
        else:
            spfs = spf_values or []

        await self._update_project_features_in_db(pid, feature_ids, targets, spfs)

        self.send_response({
            "info": "Project features updated",
            "project_id": pid
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

        self.send_response({
            'info': "This function is a placeholder. It should update project parameters in the database based on the provided arguments.",
            'received_params': params
        })
