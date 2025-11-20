import contextlib
import json
import traceback
from datetime import datetime
from decimal import Decimal

from classes.folder_path_config import get_folder_path_config
from services.service_error import ServicesError, raise_error
from tornado.web import RequestHandler

proj_paths = get_folder_path_config()


class BaseHandler(RequestHandler):
    """Base class to handle all HTTP requests. Handles authentication, authorisation, exception handling, writing headers and sending responses. All REST request handlers derive from this class.

    Attributes:
        user: A string with the name of the user making the request (if the request.arguments contains a user key).
        folder_user: A string with the path to the users folder (if the request.arguments contains a user key).
        project: A string with the name of the project (if the request.arguments contains a project key).
        project_folder: A string with the path to the project folder (if the request.arguments contains a project key).
        input_folder: A string with the path to the projects input folder (if the request.arguments contains a project key).
        output_folder: A string with the path to the projects output folder (if the request.arguments contains a project key).
    """

    def initialize(self):
        self.proj_paths = proj_paths

    def prepare(self):
        print(f"Incoming {self.request.method} request to {self.request.uri}")

    # def set_default_headers(self):
    #     """Writes CORS headers in the response to prevent CORS errors in the client"""
    #     if proj_paths.DISABLE_SECURITY:
    #         # self.set_header("Access-Control-Allow-Origin", "http://localhost:4500")
    #         self.set_header("Access-Control-Allow-Origin",
    #                         ["http://vmudai1.datascienceinstitute.ie", "http://localhost:4500"])
    #         self.set_header("Access-Control-Allow-Methods",
    #                         "GET, POST, OPTIONS")
    #         self.set_header("Access-Control-Allow-Headers",
    #                         "Content-Type, Authorization")
    #         self.set_header("Access-Control-Allow-Credentials", "true")

    def set_default_headers(self):
        """Writes CORS headers in the response to prevent CORS errors in the client"""
        allowed_origins = [
            "vmudai1.datascienceinstitute.ie",
            "localhost",
            "127.0.0.1"
        ]

        origin = self.request.headers.get("Origin", "")
        if any(all_orig in origin for all_orig in allowed_origins):
            self.set_header("Access-Control-Allow-Origin", origin)
            self.set_header("Access-Control-Allow-Credentials", "true")

        self.set_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.set_header("Access-Control-Allow-Headers",
                        "Content-Type, Authorization")

    def options(self, *args, **kwargs):
        # Respond to preflight OPTIONS request
        self.set_status(204)  # No Content
        self.finish()

    def get_current_user(self):
        """Gets the current user.
        Args:
            None
        Returns:
            string: The name of the currently authenticated user.
        """
        if self.get_secure_cookie("user"):
            return self.get_secure_cookie("user").decode("utf-8")

    def validate_args(self, arguments, required_keys):
        """Validates that all required arguments are present."""
        missing = [key for key in required_keys if key not in arguments]
        if missing:
            raise ServicesError(
                f"Missing required arguments: {', '.join(missing)}")

    def send_response(self, response):
        """Used by all descendent classes to write the response data and send it.

        Args:
            response (dict): The response data to write as a dict.
        Returns:
            None
        """
        # Convert datetime objects to string
        def json_serial(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, Decimal):
                return float(obj)
            raise TypeError(f"Type {type(obj)} not serializable")

        self.set_header('Content-Type', 'application/json')
        # make sure content exists no matter what
        content = ''

        try:
            content = json.dumps(response, default=json_serial)

        except (UnicodeDecodeError) as e:
            if 'log' in response:
                response.update({
                    "log": f"Server warning: Unable to encode the Marxan log. <br/>{repr(e)}",
                    "warning": "Unable to encode the Marxan log"
                })
            content = json.dumps(response, default=json_serial)

        finally:
            callback = self.get_argument("callback", None)
            if callback:
                print('callback: ', callback)
                content = f"{callback}({content})"
                self.write(f"{callback}({content})")
            else:
                self.write(content)

    def write_error(self, status_code, **kwargs):
        """
        Handles uncaught exceptions in descendant classes by sending the stack trace to the client.

        Args:
            status_code (int): HTTP status code of the error.
            **kwargs: Additional arguments passed by Tornado, including exception info.
        Returns:
            None
        """
        # If no exception info is provided, return immediately
        if "exc_info" not in kwargs:
            self.set_status(500)
            self.write({"error": "An unknown error occurred."})
            self.finish()
            return

        # Extract exception details
        exc_info = kwargs["exc_info"]
        trace = "".join(traceback.format_exception(*exc_info))
        last_line = traceback.format_exception(
            *exc_info)[-1].split(":", 1)[-1].strip()

        # Set CORS headers if needed
        if not proj_paths.DISABLE_SECURITY:
            with contextlib.suppress(Exception):
                _checkCORS(self)

        # Respond with an HTTP 200 status code and the error details
        self.set_status(200)
        self.send_response({
            "error": last_line,
            "trace": trace
        })
        self.finish()
