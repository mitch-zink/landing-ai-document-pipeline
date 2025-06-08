from prefect import get_run_logger
from prefect_snowflake import SnowflakeCredentials, SnowflakeConnector
import base64
from typing import Optional

import snowflake.connector


def get_snowflake_connector(
    warehouse: str = "PREFECT_WH",
    database: Optional[str] = "ai",
    schema: Optional[str] = "AGENTIC_DOC_EXTRACTION",
) -> SnowflakeConnector:
    """
    Returns a SnowflakeConnector object for use with Prefect and DBT.
    """
    # Get the Prefect logger
    get_run_logger()

    # Load the Snowflake credentials from the Prefect block
    credentials = SnowflakeCredentials.load("snowflake-credentials-key-auth")

    # Create the SnowflakeConnector with the credentials
    connector = SnowflakeConnector(
        warehouse=warehouse,
        database=database,
        schema=schema,
        role=credentials.role,
        credentials=credentials,
    )

    return connector


def get_snowflake_connection(
    warehouse: str = "PREFECT_WH",
    database: Optional[str] = "ai",
    schema: Optional[str] = "AGENTIC_DOC_EXTRACTION",
) -> snowflake.connector.SnowflakeConnection:
    """
    Establishes a connection to Snowflake and returns the connection object.
    """
    logger = get_run_logger()

    connector = get_snowflake_connector(warehouse, database, schema)
    credentials = connector.credentials

    # Retrieve the base64-encoded private key
    private_key_base64 = credentials.private_key.get_secret_value()

    # Decode the private key from base64 to bytes
    private_key_bytes = base64.b64decode(private_key_base64)

    # Create a Snowflake connection
    try:
        conn = snowflake.connector.connect(
            user=credentials.user,
            account=credentials.account,
            role=credentials.role,
            warehouse=connector.warehouse,
            database=connector.database,
            schema=connector.schema,
            private_key=private_key_bytes,
            autocommit=True,
        )
        logger.info(
            f"✅ Successfully connected to Snowflake | Database: {database} | "
            f"Schema: {schema} | Warehouse: {warehouse}"
        )
        return conn
    except Exception as e:
        logger.error(f"💩 Failed to connect to Snowflake: {e}")
        raise
