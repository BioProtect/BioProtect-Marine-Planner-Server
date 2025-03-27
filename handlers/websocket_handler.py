import datetime
import logging
import traceback
from urllib.parse import urlparse

from classes.folder_path_config import get_folder_path_config
from services.project_service import get_project_data, set_folder_paths
from services.service_error import ServicesError
from tornado.ioloop import PeriodicCallback
from tornado.web import HTTPError
from tornado.websocket import WebSocketClosedError, WebSocketHandler
from classes.postgis_class import get_pg

project_paths = get_folder_path_config()


class SocketHandler(WebSocketHandler):
    """
    Base WebSocket handler for managing WebSocket connections, including authentication,
    authorization, CORS, and keep-alive pings.
    """

    async def initialize(self):
        super().initialize()
        self.pg = await get_pg

    def get_current_user(self):
        """Retrieves the currently authenticated user."""
        user_cookie = self.get_secure_cookie("user")
        return user_cookie.decode("utf-8") if user_cookie else None

    def check_origin(self, origin):
        """Checks CORS access for the WebSocket."""
        if project_paths.DISABLE_SECURITY:
            return True

        parsed_origin = urlparse(origin)
        if origin in project_paths.PERMITTED_DOMAINS or parsed_origin.netloc.find(self.request.host_name) != -1:
            return True
        raise HTTPError(403, f"The origin '{
                        origin}' does not have permission to access the service (CORS error)")

    async def open(self, start_message):
        """Handles WebSocket connection opening."""
        try:
            self.start_time = datetime.datetime.now()
            start_message.update({'status': 'Started'})
            self.send_response(start_message)

            if "user" in self.request.arguments:
                set_folder_paths(self, self.request.arguments,
                                 project_paths.USERS_FOLDER)
                if hasattr(self, 'project_folder'):
                    await get_project_data(self.pg, self)

            if project_paths.DISABLE_SECURITY:
                return

            if not self.current_user:
                raise HTTPError(401, "User not authenticated")

            self._authorize_request()

            self.send_response(
                {"status": "Preprocessing", "info": "Preprocessing..."})

            self.ping_callback = PeriodicCallback(self._send_ping, 30000)
            self.ping_callback.start()
            self.client_sent_final_msg = False

        except HTTPError as e:
            self._handle_connection_error(e)

    def _authorize_request(self):
        """Handles authorization logic for WebSocket requests."""
        method = self.request.path.strip("/").split("/")[-1]

        role = self.get_secure_cookie("role")
        if not role:
            raise HTTPError(403, "Unauthorized: Role not found.")

        role = role.decode("utf-8")
        unauthorized_methods = ROLE_UNAUTHORISED_METHODS.get(role, [])
        if method in unauthorized_methods:
            raise HTTPError(
                403, f"Role '{role}' does not have access to '{method}'")

        requested_user = self.get_argument("user", None)
        if not requested_user:
            return

        if requested_user == "_clumping":
            return

        if requested_user != self.current_user:
            if self.current_user == GUEST_USERNAME:
                raise HTTPError(
                    403, "Guest users cannot access other users' projects.")

            if role != "Admin":
                raise HTTPError(
                    403, f"User '{self.current_user}' cannot access projects of other users.")

    def send_response(self, message):
        """Sends a response to the client with metadata."""
        elapsed_time = f"{
            (datetime.datetime.now() - self.start_time).seconds}s"
        message.update({'elapsedtime': elapsed_time})

        if "user" in self.request.arguments:
            message.update({'user': self.get_argument("user")})

        if hasattr(self, 'pid'):
            message.update({'pid': self.pid})

        try:
            self.write_message(message)
        except WebSocketClosedError:
            self.close_connection()

    def _send_ping(self):
        """Sends a ping to keep the WebSocket connection alive."""
        message = {"status": "WebSocketOpen"}
        if hasattr(self, 'ping_message'):
            message.update({"status": "Preprocessing",
                           "info": self.ping_message})
        self.send_response(message)

    def close(self, close_message=None, clean=True):
        """Closes the WebSocket connection."""
        if self.ping_callback.is_running:
            self.ping_callback.stop()

        if clean:
            close_message = close_message or {}
            close_message.update({'status': 'Finished'})
            self.send_response(close_message)
            if 'error' in close_message:
                logging.warning(close_message['error'])

        super().close(1000)

    def _handle_connection_error(self, error):
        """Handles connection errors by logging and closing the WebSocket."""
        error_message = str(error)
        logging.error(f"WebSocket error: {error_message}")
        self.close({'error': error_message}, clean=False)
