"""Tests for Snowflake connection utilities."""

import base64
from unittest.mock import Mock, patch
import pytest
from flows.get_snowflake_connection import get_snowflake_connector, get_snowflake_connection


class TestSnowflakeConnection:
    """Test Snowflake connection utilities."""

    @patch("flows.get_snowflake_connection.SnowflakeCredentials")
    @patch("flows.get_snowflake_connection.SnowflakeConnector")
    def test_get_snowflake_connector_success(self, mock_connector_class, mock_credentials_class, mocker):
        """Test successful Snowflake connector creation."""
        # Mock credentials
        mock_credentials = Mock()
        mock_credentials.role = "test_role"
        mock_credentials_class.load.return_value = mock_credentials
        
        # Mock connector
        mock_connector = Mock()
        mock_connector_class.return_value = mock_connector
        
        mocker.patch("flows.get_snowflake_connection.get_run_logger")
        
        result = get_snowflake_connector()
        
        # Verify credentials were loaded
        mock_credentials_class.load.assert_called_once_with("snowflake-credentials-key-auth")
        
        # Verify connector was created with correct parameters
        mock_connector_class.assert_called_once_with(
            warehouse="PREFECT_WH",
            database="ai",
            schema="AGENTIC_DOC_EXTRACTION",
            role="test_role",
            credentials=mock_credentials
        )
        
        assert result == mock_connector

    @patch("flows.get_snowflake_connection.get_snowflake_connector")
    @patch("flows.get_snowflake_connection.snowflake.connector.connect")
    @patch("flows.get_snowflake_connection.base64.b64decode")
    def test_get_snowflake_connection_success(self, mock_b64decode, mock_connect, mock_get_connector, mocker):
        """Test successful Snowflake connection establishment."""
        # Mock connector and credentials
        mock_credentials = Mock()
        mock_credentials.user = "test_user"
        mock_credentials.account = "test_account"
        mock_credentials.role = "test_role"
        mock_credentials.private_key.get_secret_value.return_value = "base64_encoded_key"
        
        mock_connector = Mock()
        mock_connector.credentials = mock_credentials
        mock_connector.warehouse = "PREFECT_WH"
        mock_connector.database = "ai"
        mock_connector.schema = "AGENTIC_DOC_EXTRACTION"
        
        mock_get_connector.return_value = mock_connector
        
        # Mock base64 decoding
        mock_b64decode.return_value = b"decoded_private_key"
        
        # Mock Snowflake connection
        mock_connection = Mock()
        mock_connect.return_value = mock_connection
        
        mock_logger = mocker.patch("flows.get_snowflake_connection.get_run_logger")
        
        result = get_snowflake_connection()
        
        # Verify connector was retrieved
        mock_get_connector.assert_called_once_with("PREFECT_WH", "ai", "AGENTIC_DOC_EXTRACTION")
        
        # Verify private key was decoded
        mock_b64decode.assert_called_once_with("base64_encoded_key")
        
        # Verify connection was established
        mock_connect.assert_called_once_with(
            user="test_user",
            account="test_account",
            role="test_role",
            warehouse="PREFECT_WH",
            database="ai",
            schema="AGENTIC_DOC_EXTRACTION",
            private_key=b"decoded_private_key",
            autocommit=True
        )
        
        assert result == mock_connection

    @patch("flows.get_snowflake_connection.get_snowflake_connector")
    @patch("flows.get_snowflake_connection.snowflake.connector.connect")
    def test_get_snowflake_connection_failure(self, mock_connect, mock_get_connector, mocker):
        """Test Snowflake connection failure handling."""
        # Mock connector
        mock_credentials = Mock()
        mock_credentials.private_key.get_secret_value.return_value = "base64_key"
        mock_connector = Mock()
        mock_connector.credentials = mock_credentials
        mock_get_connector.return_value = mock_connector
        
        # Mock connection failure
        mock_connect.side_effect = Exception("Connection failed")
        
        mock_logger = mocker.patch("flows.get_snowflake_connection.get_run_logger")
        
        with pytest.raises(Exception, match="Connection failed"):
            get_snowflake_connection()
        
        # Verify error was logged
        mock_logger.return_value.error.assert_called_once()

    def test_get_snowflake_connector_custom_parameters(self, mocker):
        """Test Snowflake connector creation with custom parameters."""
        with patch("flows.get_snowflake_connection.SnowflakeCredentials") as mock_creds, \
             patch("flows.get_snowflake_connection.SnowflakeConnector") as mock_conn_class:
            
            mock_credentials = Mock()
            mock_creds.load.return_value = mock_credentials
            mock_connector = Mock()
            mock_conn_class.return_value = mock_connector
            
            mocker.patch("flows.get_snowflake_connection.get_run_logger")
            
            result = get_snowflake_connector(
                warehouse="CUSTOM_WH",
                database="custom_db",
                schema="custom_schema"
            )
            
            mock_conn_class.assert_called_once_with(
                warehouse="CUSTOM_WH",
                database="custom_db", 
                schema="custom_schema",
                role=mock_credentials.role,
                credentials=mock_credentials
            )