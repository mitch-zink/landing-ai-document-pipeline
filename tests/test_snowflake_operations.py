"""Tests for Snowflake operations in the document processing pipeline."""

import json
from unittest.mock import Mock, patch, call
import pytest
from flows.s3_to_snowflake import setup_snowflake_infrastructure, load_to_snowflake, _exec_sql


class TestSnowflakeOperations:
    """Test Snowflake-related operations."""

    @patch("flows.s3_to_snowflake.get_snowflake_connection")
    def test_setup_snowflake_infrastructure_success(self, mock_get_connection, mocker):
        """Test successful Snowflake infrastructure setup."""
        # Mock connection and cursor
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_connection.return_value.__enter__.return_value = mock_connection
        
        # Mock context query
        mock_cursor.fetchall.return_value = [("ai", "AGENTIC_DOC_EXTRACTION", "PREFECT_WH")]
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        setup_snowflake_infrastructure()
        
        # Verify connection was established
        mock_get_connection.assert_called_once_with(warehouse="PREFECT_WH")
        
        # Verify SQL commands were executed (at least warehouse, database, schema creation)
        assert mock_cursor.execute.call_count >= 6

    @patch("flows.s3_to_snowflake.get_snowflake_connection")
    def test_load_to_snowflake_success(self, mock_get_connection, mocker):
        """Test successful data loading to Snowflake."""
        # Mock connection and cursor
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_connection.return_value.__enter__.return_value = mock_connection
        
        mock_cursor.fetchall.return_value = []
        mock_cursor.rowcount = 1
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        test_content = {
            "data": {
                "markdown": "# Test",
                "chunks": [],
                "extracted_schema": {"file_name": "test.pdf"}
            }
        }
        
        load_to_snowflake("folder/test.pdf", test_content)
        
        # Verify connection was established with correct parameters
        mock_get_connection.assert_called_once_with(
            warehouse="PREFECT_WH", 
            database="ai", 
            schema="AGENTIC_DOC_EXTRACTION"
        )
        
        # Verify MERGE SQL was executed
        merge_calls = [call for call in mock_cursor.execute.call_args_list 
                      if 'MERGE' in str(call)]
        assert len(merge_calls) >= 1

    @patch("flows.s3_to_snowflake.get_snowflake_connection")
    def test_load_to_snowflake_fallback_to_simple_table(self, mock_get_connection, mocker):
        """Test fallback to simple table name when qualified name fails."""
        mock_cursor = Mock()
        mock_connection = Mock()
        mock_connection.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_connection.return_value.__enter__.return_value = mock_connection
        
        # First MERGE fails, second succeeds
        mock_cursor.execute.side_effect = [
            None,  # USE DATABASE
            None,  # USE SCHEMA  
            Exception("Qualified table error"),  # First MERGE fails
            None,  # Second MERGE succeeds
        ]
        mock_cursor.fetchall.return_value = []
        mock_cursor.rowcount = 1
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        test_content = {"data": {"markdown": "test"}}
        
        load_to_snowflake("test.pdf", test_content)
        
        # Verify both MERGE attempts were made
        assert mock_cursor.execute.call_count == 4

    def test_exec_sql_success(self, mocker):
        """Test successful SQL execution."""
        mock_cursor = Mock()
        mock_cursor.fetchall.return_value = [("result",)]
        mock_cursor.rowcount = 1
        
        mock_logger = mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        _exec_sql(mock_cursor, "SELECT 1", "Test query")
        
        mock_cursor.execute.assert_called_once_with("SELECT 1")
        mock_cursor.fetchall.assert_called_once()

    def test_exec_sql_failure(self, mocker):
        """Test SQL execution failure handling."""
        mock_cursor = Mock()
        mock_cursor.execute.side_effect = Exception("SQL error")
        
        mock_logger = mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        # Should not raise, just log warning
        _exec_sql(mock_cursor, "INVALID SQL", "Test query")
        
        mock_cursor.execute.assert_called_once_with("INVALID SQL")