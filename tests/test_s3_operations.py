"""Tests for S3 operations in the document processing pipeline."""

import pytest
from unittest.mock import Mock, patch
from flows.s3_to_snowflake import list_s3_files, get_s3_file


class TestS3Operations:
    """Test S3-related operations."""

    def test_list_s3_files_with_objects(self, mocker):
        """Test listing S3 files when objects exist."""
        # Mock S3Bucket
        mock_bucket = Mock()
        mock_bucket.list_objects.return_value = [
            {"Key": "doc1.pdf"},
            {"Key": "doc2.jpg"},
            {"Key": "folder/doc3.png"}
        ]
        
        # Mock logger
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        result = list_s3_files(mock_bucket)
        
        assert result == ["doc1.pdf", "doc2.jpg", "folder/doc3.png"]
        mock_bucket.list_objects.assert_called_once()

    def test_list_s3_files_empty_bucket(self, mocker):
        """Test listing S3 files when bucket is empty."""
        mock_bucket = Mock()
        mock_bucket.list_objects.return_value = None
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        result = list_s3_files(mock_bucket)
        
        assert result == []
        mock_bucket.list_objects.assert_called_once()

    def test_get_s3_file(self, mocker):
        """Test downloading file from S3."""
        mock_bucket = Mock()
        test_content = b"test file content"
        mock_bucket.read_path.return_value = test_content
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        result = get_s3_file(mock_bucket, "test-file.pdf")
        
        assert result == test_content
        mock_bucket.read_path.assert_called_once_with("test-file.pdf")