import json
import logging

from tornado.websocket import WebSocketHandler, WebSocketClosedError
from tornado.web import HTTPError
from classes.folder_path_config import get_folder_path_config

project_paths = get_folder_path_config()

# All currently connected notification clients
_clients = set()


def broadcast(event_type, data=None):
    """Broadcast an event to all connected notification clients."""
    message = json.dumps({"event": event_type, **(data or {})})
    closed = []
    for client in _clients:
        try:
            client.write_message(message)
        except WebSocketClosedError:
            closed.append(client)
    for c in closed:
        _clients.discard(c)


class NotificationHandler(WebSocketHandler):
    """Lightweight WebSocket for pushing server-side events to the frontend."""

    def check_origin(self, origin):
        if project_paths.DISABLE_SECURITY:
            return True
        from urllib.parse import urlparse
        parsed_origin = urlparse(origin)
        if origin in project_paths.PERMITTED_DOMAINS or parsed_origin.netloc.find(self.request.host_name) != -1:
            return True
        return False

    def open(self):
        _clients.add(self)
        logging.info(f"Notification client connected ({len(_clients)} total)")

    def on_message(self, message):
        pass  # clients don't send anything meaningful

    def on_close(self):
        _clients.discard(self)
        logging.info(f"Notification client disconnected ({len(_clients)} total)")
