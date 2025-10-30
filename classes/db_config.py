from dotenv import dotenv_values
from sqlalchemy import create_engine
from platform import system
from os import path, sep
import sys


class DBConfig:
    def __init__(self):
        # Load environment and server configuration
        self.db_config = dotenv_values('.env.local')
        # print("Loaded .env.local →", repr(self.db_config))

        self.set_executables()
        self.load_server_config()
        self.engine = self.create_db_engine()

    def load_server_config(self):
        """Loads the server configuration from the file."""
        self.SERVER_NAME = self.db_config.get(
            'server_name', 'default_server_name')
        self.SERVER_DESCRIPTION = self.db_config.get(
            'server_description', 'default_description')
        self.SERVER_PORT = self.db_config.get(
            'server_port', '5000')
        self.DATABASE_NAME = self.db_config.get("db_name")
        self.DATABASE_HOST = self.db_config.get("db_host")
        self.DATABASE_USER = self.db_config.get("db_user")
        self.DATABASE_PASSWORD = self.db_config.get("db_pass")
        # default port for PostgreSQL
        self.PORT = self.db_config.get('PORT', '5432')
        self.COOKIE_SECRET = self.db_config.get('cookie_secret')

        # Check for missing database configuration and raise an error if any are missing
        if not all([self.DATABASE_NAME,
                    self.DATABASE_HOST,
                    self.DATABASE_USER,
                    self.DATABASE_PASSWORD]):
            raise ValueError(
                "Missing required database configuration (db_name, db_host, db_user, or db_pass).")

        self.CONNECTION_STRING = self.build_connection_string()

    def build_connection_string(self):
        """Builds the PostgreSQL connection string."""
        # return f"host={self.DATABASE_HOST} dbname={self.DATABASE_NAME} user={self.DATABASE_USER} password={self.DATABASE_PASSWORD}"
        return f"postgres://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}@{self.DATABASE_HOST}:{self.PORT}/{self.DATABASE_NAME}"

    def psql_str(self):
        return f" |  psql -h {self.DATABASE_HOST} -p {self.PORT} -U {self.DATABASE_USER} -d {self.DATABASE_NAME}"

    def create_db_engine(self):
        """Creates a SQLAlchemy engine for the PostgreSQL database."""
        return create_engine(
            f'postgresql+psycopg2://{self.DATABASE_USER}:{self.DATABASE_PASSWORD}@{self.DATABASE_HOST}:{self.PORT}/{self.DATABASE_NAME}',
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=1800
        )

    def set_executables(self):
        """Returns the paths to ogr2ogr and Marxan executables, and a stop command based on the platform."""
        if system() == "Windows":
            ogr2ogr_executable = "ogr2ogr.exe"
            OGR2OGR_PATH = path.dirname(
                sys.executable) + sep + "library" + sep + "bin" + sep
            marxan_executable = "Marxan.exe"
            stop_cmd = "Press CTRL+C or CTRL+Fn+Pause to stop the server\n"
        else:
            ogr2ogr_executable = "ogr2ogr"
            OGR2OGR_PATH = "/usr/bin/"
            marxan_executable = "MarOpt_v243_Linux64"
            stop_cmd = "Press CTRL+C to stop the server\n"

        OGR2OGR_EXECUTABLE = OGR2OGR_PATH + ogr2ogr_executable

        self.OGR2OGR_EXECUTABLE = OGR2OGR_EXECUTABLE
        self.MARXAN_EXECUTABLE = marxan_executable
        self.STOP_CMD = stop_cmd


db_config = None


def get_db_config():
    global db_config
    if db_config is None:
        db_config = DBConfig()
    return db_config
