import glob
import json
import shutil
from os.path import basename, join, normpath
from types import SimpleNamespace

from asyncpg.exceptions import UniqueViolationError
from handlers.base_handler import BaseHandler
from passlib.hash import bcrypt
from psycopg2 import sql
from services.user_service import get_notifications_data
from services.file_service import (
    get_key_values_from_file, update_file_parameters)
from services.project_service import clone_a_project, set_folder_paths
from services.service_error import ServicesError, raise_error

# JWT utility for generating tokens


class UserHandler(BaseHandler):
    """
    REST HTTP handler for user-related operations, including creation, validation, deletion,
    updating parameters, and retrieving user data.
    """

    def initialize(self, pg, project_paths):
        super().initialize()
        self.pg = pg
        self.project_paths = project_paths

    def validate_args(self, arguments, required_arguments):
        """
        Validates that all required arguments are present in the provided arguments dictionary.

        Args:
            arguments (dict): Dictionary of arguments (e.g., from a Tornado HTTP request).
            required_arguments (list[str]): List of required argument names.

        Returns:
            None

        Raises:
            ServicesError: If any required arguments are missing.
        """
        missing_arguments = [
            arg for arg in required_arguments if arg not in arguments]
        if missing_arguments:
            raise ServicesError(
                f"Missing arguments: {', '.join(missing_arguments)}")

    async def get(self):
        """
        Handles GET requests for various user-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'get':
                await self.get_user()
            elif action == 'list':
                await self.get_users()
            elif action == 'logout':
                await self.logout_user()
            elif action == 'delete':
                await self.delete_user()
            elif action == 'resend_password':
                await self.resend_password()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def post(self):
        """
        Handles POST requests for user-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'create':
                await self.create_user()
            elif action == 'update':
                await self.update_user()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def create_user(self):
        self.validate_args(self.request.arguments, [
                           "user", "password", "fullname", "email"])
        try:
            body = json.loads(self.request.body)
            username = body.get("username")
            email = body.get("email")
            password = body.get("password")
            role = body.get("role", "user")
            fullname = self.get_argument('fullname')

            if not username or not email or not password:
                self.set_status(400)
                self.write(
                    {"message": "Username, email, and password are required"})
                return

            password_hash = bcrypt.hash(password)
            new_user = await self.pg.execute(
                """
                INSERT INTO users (username, email, password_hash, role, report_units, basemap, date_created, show_popup, use_feature_colours)
                VALUES (%s, %s, %s, 'Admin', 'Km2', 'Light', CURRENT_TIMESTAMP, FALSE, FALSE)
                """,
                data=[username, email, password_hash],
                return_format="Dict"
            )
            self.set_status(201)
            self.write({"message": "User created", "user": new_user})

        except UniqueViolationError:
            self.set_status(409)
            self.write({"message": "Username or email already exists"})
        except Exception as e:
            self.set_status(500)
            self.write({"message": "Error creating user", "error": str(e)})

    async def logout_user(self):
        try:
            # Get current user info if possible
            user_id = self.get_secure_cookie("user_id")

            query = "UPDATE bioprotect.users SET refresh_tokens = ARRAY[]::text[] WHERE id = %s"
            await self.pg.execute(query, [int(user_id.decode())])

            # Clear cookies
            self.clear_cookie("jwt")
            self.clear_cookie("user")
            self.clear_cookie("user_id")
            self.clear_cookie("role")

            self.send_response({'info': "User logged out"})
        except Exception as e:
            self.set_status(500)
            self.send_response({'error': "Logout Failed"})

    async def resend_password(self):
        self.send_response({'info': "Not currently implemented"})

    async def get_user(self):
        self.validate_args(self.request.arguments, ["user"])

        query = """
                SELECT id, username, password_hash, role, last_project, show_popup, basemap, use_feature_colours, report_units, refresh_tokens
                FROM bioprotect.users WHERE username = %s
            """
        userData = await self.pg.execute(query, [self.get_current_user()], return_format="Dict")

        notifications = get_notifications_data(self)
        self.send_response({
            'info': "User data received",
            "userData": userData,
            "unauthorisedMethods": [],
            'dismissedNotifications': notifications
        })

    async def get_users(self):
        """Retrieve a list of users from the database."""

        query = """
            SELECT id, username, email, role, date_created
            FROM bioprotect.users
            ORDER BY username;
        """
        try:
            users = await self.pg.execute(query, return_format="Dict")
            return users
        except Exception as e:
            self.send_response({"error": f"Failed to fetch users: {str(e)}"})

    async def delete_user(self):
        self.validate_args(self.request.arguments, ["user"])

        try:
            shutil.rmtree(self.folder_user)
            self.send_response({'info': "User deleted"})
        except Exception as e:
            raise ServicesError(f"Failed to delete user: {e}")

    async def update_user_parameters(self):
        self.validate_args(self.request.arguments, ["user_id"])

        params = {
            key: self.get_argument(key)
            for key in self.request.arguments
            if key not in ["user_id", "callback"]
        }

        update_file_parameters(join(self.folder_user, "user.dat"), params)

        self.send_response({
            'info': ", ".join(params.keys()) + " parameters updated"
        })

    # NOT YET IMPLEMENTED *************************************************
    async def get_user_by_id(self, user_id=None):
        """
        Retrieve a single user by ID or all users if no ID is provided.
        """
        query = """
        SELECT id, username, last_project, show_popup, basemap, role, use_feature_colours, report_units, refresh_tokens
        """
        try:
            user_id = int(user_id)
        except ValueError:
            self.set_status(400)
            self.write(json.dumps({"error": "Invalid user ID"}))
            return

        if user_id:
            query = query + "FROM bioprotect.users WHERE id = %s"
            result = await self.pg.execute(query, data=[user_id], return_format="Dict")
            if not result:
                self.set_status(404)
                self.write({"message": "User not found"})
                return

            response = json.dumps(result[0])
            callback = self.get_argument("callback", None)
            if callback:
                self.write(f"{callback}({response})")
            else:
                self.write(response)
        else:
            users = await self.pg.execute(query, return_format="Array")
            self.write(json.dumps({"users": users}))

    # NOT YET IMPLEMENTED *************************************************

    async def update_user(self):
        """
        Update an existing user by ID (password, username, email, role).
        """
        try:
            body = {k: self.get_argument(k) for k in self.request.arguments}
            print("BODY====== ", body)
            user_id = int(self.get_argument("user_id"))
            updates = []
            params = []

            field_map = {
                "username": "username",
                "email": "email",
                "role": "role",
                "basemap": "basemap",
                "show_popup": "show_popup",
                "use_feature_colours": "use_feature_colours",
                "report_units": "report_units",
                "password": "password_hash",
            }
            for field, col in field_map.items():
                if field in body:
                    value = body[field]
                    if field == "password":
                        value = bcrypt.hash(value)
                    updates.append(f"{col} = %s")
                    params.append(value)

            if not updates:
                self.set_status(400)
                self.write(
                    {"success": False, "message": "No fields to update"})
                return

            params.append(user_id)

            query = f"""
                UPDATE bioprotect.users
                SET {', '.join(updates)}
                WHERE id = %s
            """

            await self.pg.execute(query, data=params)

            self.write({"success": True, "message": "User updated"})

        except Exception as e:
            self.set_status(500)
            self.write({
                "success": False,
                "message": "Error updating user",
                "error": str(e),
            })

            # NOT YET IMPLEMENTED *************************************************
            # async def delete_user_by_id(self, user_id):
            #     try:
            # query = "DELETE FROM bioprotect.users WHERE id = %s"
            #     result= await self.pg.execute(query, int(user_id))
            #     if result == "DELETE 0":
            #     self.set_status(404)
            #     self.write({"message": "User not found"})
            #     else:
            #     self.write({"message": "User deleted"})

            #     except Exception as e:
            #     self.set_status(500)
            #     self.write({"message": "Error deleting user", "error": str(e)})
