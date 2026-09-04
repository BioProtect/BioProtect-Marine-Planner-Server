import tornado
from server.handlers.base_handler import BaseHandler
from services.service_error import raise_error


class MethodNotFound(BaseHandler):
    """REST HTTP handler. Called when the REST service method does not match any defined handlers.

    Args:
        None

    Returns:
        None
    """

    def prepare(self):
        """Override method to raise an exception in the REST handler if the method is not found.
        """
        print('Method not found')

        error_message = (
            f"The method is not supported or the parameters are incorrect")

        if 'Upgrade' in self.request.headers:
            # WebSocket unsupported method: raise a 501 error (not implemented)
            raise tornado.web.HTTPError(501, error_message)
        else:
            # Regular GET/POST unsupported method
            raise_error(self, error_message)
