"""Main entrypoint for the Ambuda application.

For a high-level overview of the application and how to operate it, see:

https://ambuda.readthedocs.io/en/latest/
"""

from dotenv import load_dotenv


load_dotenv(".env")


def create_app(config_env: str):
    from ambuda.app import create_app

    return create_app(config_env)
