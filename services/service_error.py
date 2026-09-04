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


def validate_arguments(arguments, argumentList):
    """Checks that all of the arguments in argumentList are in the arguments dictionary.

    Args:
        # tornado.httputil.HTTPServerRequest.arguments
        arguments (dict): See https://www.tornadoweb.org/en/stable/httputil.html
        argumentList (string[]): The list of arguments that must be present.
    Returns:
        None
    Raises:
        raise_error: If any of the required arguments are not present.
    """

    for argument in argumentList:
        if argument not in list(arguments.keys()):
            raise raise_error(f"Missing input argument:${argument}")


class ExtendableObject(object):
    """Custom class for allowing objects to be extended with new attributes.
    """
    pass
