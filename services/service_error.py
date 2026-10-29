import logging


class ServicesError(Exception):
    """Custom exception class for raising exceptions in this module.
    """

    def __init__(self, *args, **kwargs):
        super(ServicesError, self)


def raise_error(obj, msg):
    """Generic function to send an error response and close the connection. Used in all BaseHandler descendent classes.

    Args:
        obj (BaseHandler): The request handler instance
        msg (string): The error message to send
    Returns:
        None
    """
    # send a response with the error message
    if hasattr(obj, "send_response"):
        obj.send_response({"error": msg})
        obj.finish()
    # log the error
    logging.warning(msg)


