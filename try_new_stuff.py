#!/home/ubuntu/miniconda2/envs/python36/bin/python3.6
#
# Copyright (c) 2020 Andrew Cottam.
#
# This file is part of marxan-server
# (see https://github.com/marxanweb/marxan-server).
#
# License: European Union Public Licence V. 1.2, see https://opensource.org/licenses/EUPL-1.2
#

"""Core module for handling all marxan-server REST API requests.

This module is run with the Tornado Web Server to handle requests to the Marxan
software and to return JSON data for those requests.

This module defines the following:

 - Global variables (constants) that are used in the module.
 - Private module functions (prefixed with an underscore) that are the internal
 implementation.
 - Request handler classes. HTTPRequest handlers
 (MarxanRESTHandler descendents) and WebSocket handlers for long-running processes
 (MarxanWebSocketHandler descendents).
 - Utiliy classes, e.g. for interacting with PostGIS.

"""

import asyncio
import ctypes
import datetime
import fnmatch
import glob
import io
import json
import logging
import os
import platform
import re
import select
import shlex
import shutil
import signal
import subprocess
import sys
import time
import traceback
import urllib
import uuid
import webbrowser
import xml.etree.ElementTree as ET
import zipfile
from collections import OrderedDict
from datetime import timedelta, timezone
from subprocess import PIPE, CalledProcessError, Popen
from threading import Thread
from urllib import request
from urllib.parse import urlparse

import aiohttp
import aiopg
import colorama
import numpy
import pandas
import psutil
import psycopg2
from psycopg2.sql import SQL, Identifier
import rasterio
import requests
import tornado.options
from colorama import Back, Fore, Style
from google.cloud import logging as googlelogger
from mapbox import Uploader, errors
from osgeo import ogr
from psycopg2 import sql
from psycopg2.extensions import AsIs, register_adapter
from rasterio.io import MemoryFile
from sqlalchemy import create_engine, exc
from tornado import concurrent, gen, httpclient, queues
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError
from tornado.log import LogFormatter
from tornado.platform.asyncio import AnyThreadEventLoopPolicy
from tornado.process import Subprocess
from tornado.web import HTTPError, StaticFileHandler
from tornado.websocket import WebSocketClosedError


conn = psycopg2.connect(
    'postgresql://postgres:oxen4chit@localhost/marxanserver')
cur = conn.cursor()


def shape_to_hex():
    data = cur.execute(
        """
            SELECT marxan.hex_grid(
                case_study.area,case_study.xmin,case_study.ymin,case_study.xmax,case_study.ymax FROM
                (SELECT (area, ST_XMin(geom), ST_YMin(geom), ST_XMax(geom), ST_YMax(geom)) AS case_study from impact.case_studies WHERE gid=12))             
        """
    )
    res = data.fetchone()
    print('res: ', res)

    return


shape_to_hex()
