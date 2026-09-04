import logging
import platform
import subprocess
import shlex
from tornado.process import Subprocess
from tornado.iostream import StreamClosedError
from tornado import gen
from subprocess import CalledProcessError

from services.service_error import ServicesError

logger = logging.getLogger(__name__)

# async def run_command(cmd, suppressOutput=False):
#     """Runs a command in a separate process asynchronously.

#     Args:
#         cmd (string): The command to run.
#         suppressOutput (bool): Optional. If True, suppresses the output to stdout. Default value is False.
#     Returns:
#         int: Returns 0 if successful, otherwise 1.
#     Raises:
#         ServicesError: If there's an error running the command.
#     """
#     if platform.system() != "Windows":
#         try:
#             # Run the command as an asynchronous subprocess
#             if suppressOutput:
#                 process = Subprocess(
#                     shlex.split(cmd), stdout=subprocess.DEVNULL
#                 )
#             else:
#                 process = Subprocess(
#                     shlex.split(cmd), stdout=subprocess.PIPE
#                 )
#             result = await process.wait_for_exit()
#         except subprocess.CalledProcessError as e:
#             raise ServicesError(f"Error running command: {cmd}\n{str(e)}")
#         except StreamClosedError as e:
#             raise ServicesError(f"Stream closed unexpectedly: {str(e)}")
#     else:
#         # For Windows, run the command using Python's subprocess module
#         try:
#             resultBytes = subprocess.check_output(
#                 cmd, shell=True, stderr=subprocess.STDOUT
#             )
#             result = 0 if resultBytes.decode("utf-8") == '' else -1
#         except subprocess.CalledProcessError as e:
#             raise ServicesError(f"Error running command: {cmd}\n{e.output.decode('utf-8')}")

#     return result


@gen.coroutine
def run_command(cmd, suppressOutput=False):
    """Runs a command in a separate process. This is a utility method for running synchronous code in Tornado in a separate process (and thereby running it asynchronously).

    Args:
        cmd (string): The command to run.  
        suppressOutput (bool): Optional. If True, suppresses the output to stdout. Default value is False.  
    Returns:
        int: Returns 0 if successful otherwise 1.
    """
    if platform.system() != "Windows":
        try:
            logging.debug(cmd)
            # run the import as an asyncronous subprocess
            if suppressOutput:
                process = Subprocess([*shlex.split(cmd)],
                                     stdout=subprocess.DEVNULL)
            else:
                process = Subprocess([*shlex.split(cmd)])
            result = yield process.wait_for_exit()
        except CalledProcessError as e:
            raise ServicesError(
                "Error running command: " + cmd + "\n" + e.args[0])
    else:
        # run the command using the python subprocess module
        resultBytes = subprocess.check_output(
            cmd, shell=True, stderr=subprocess.STDOUT)
        result = 0 if (resultBytes.decode("utf-8") == '') else -1
    return result
