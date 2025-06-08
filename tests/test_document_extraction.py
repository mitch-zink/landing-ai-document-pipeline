"""Tests for document extraction functionality."""

import json
import tempfile
from unittest.mock import Mock, patch, mock_open
import pytest
from flows.s3_to_snowflake import extract_document_content


class TestDocumentExtraction:
    """Test document extraction using Landing AI."""

    @patch("flows.s3_to_snowflake.Secret")
    @patch("flows.s3_to_snowflake.tempfile.NamedTemporaryFile")
    @patch("flows.s3_to_snowflake.os.unlink")
    @patch("flows.s3_to_snowflake.parse")
    def test_extract_document_content_success(self, mock_parse, mock_unlink, mock_temp_file, mock_secret, mocker):
        """Test successful document extraction."""
        # Mock Secret
        mock_secret_obj = Mock()
        mock_secret_obj.get.return_value = "test-api-key"
        mock_secret.load.return_value = mock_secret_obj
        
        # Mock temporary file
        mock_temp = Mock()
        mock_temp.name = "/tmp/test_file.pdf"
        mock_temp_file.return_value.__enter__.return_value = mock_temp
        
        # Mock Landing AI parse result
        mock_chunk = Mock()
        mock_chunk.text = "Test document content"
        mock_chunk.chunk_type.value = "text"
        mock_chunk.chunk_id = "chunk_1"
        mock_chunk.grounding = []
        
        mock_result = Mock()
        mock_result.markdown = "# Test Document\nTest content"
        mock_result.chunks = [mock_chunk]
        
        mock_parse.return_value = [mock_result]
        
        # Mock logger
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        test_content = b"fake pdf content"
        file_key = "test.pdf"
        
        result = extract_document_content(test_content, file_key)
        
        # Verify the result structure
        assert "data" in result
        assert "markdown" in result["data"]
        assert "chunks" in result["data"]
        assert "extracted_schema" in result["data"]
        assert "extraction_metadata" in result["data"]
        
        # Verify content
        assert result["data"]["markdown"] == "# Test Document\nTest content"
        assert len(result["data"]["chunks"]) == 1
        assert result["data"]["chunks"][0]["text"] == "Test document content"
        assert result["data"]["extracted_schema"]["file_name"] == file_key
        
        # Verify API key was set
        mock_temp.write.assert_called_once_with(test_content)
        mock_parse.assert_called_once_with("/tmp/test_file.pdf")
        mock_unlink.assert_called_once_with("/tmp/test_file.pdf")

    @patch("flows.s3_to_snowflake.Secret")
    def test_extract_document_content_missing_api_key(self, mock_secret, mocker):
        """Test extraction fails when API key is missing."""
        mock_secret_obj = Mock()
        mock_secret_obj.get.return_value = None
        mock_secret.load.return_value = mock_secret_obj
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        with pytest.raises(ValueError, match="Landing AI API key is required"):
            extract_document_content(b"content", "test.pdf")

    @patch("flows.s3_to_snowflake.Secret")
    @patch("flows.s3_to_snowflake.tempfile.NamedTemporaryFile")
    @patch("flows.s3_to_snowflake.os.unlink")
    @patch("flows.s3_to_snowflake.parse")
    def test_extract_document_content_empty_results(self, mock_parse, mock_unlink, mock_temp_file, mock_secret, mocker):
        """Test extraction when Landing AI returns empty results."""
        # Mock Secret
        mock_secret_obj = Mock()
        mock_secret_obj.get.return_value = "test-api-key"
        mock_secret.load.return_value = mock_secret_obj
        
        # Mock temporary file
        mock_temp = Mock()
        mock_temp.name = "/tmp/test_file.pdf"
        mock_temp_file.return_value.__enter__.return_value = mock_temp
        
        # Mock empty results
        mock_parse.return_value = []
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        with pytest.raises(ValueError, match="Empty results"):
            extract_document_content(b"content", "test.pdf")