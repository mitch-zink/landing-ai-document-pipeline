"""Integration tests for the complete document processing workflow."""

from unittest.mock import Mock, patch
import pytest
from flows.s3_to_snowflake import s3_to_snowflake_flow, process_document


class TestWorkflowIntegration:
    """Test complete workflow integration."""

    @patch("flows.s3_to_snowflake.process_document")
    @patch("flows.s3_to_snowflake.list_s3_files")
    @patch("flows.s3_to_snowflake.setup_snowflake_infrastructure")
    @patch("flows.s3_to_snowflake.create_s3_bucket_if_not_exists")
    @patch("flows.s3_to_snowflake.S3Bucket")
    @patch("flows.s3_to_snowflake.Secret")
    def test_s3_to_snowflake_flow_success(
        self, 
        mock_secret, 
        mock_s3_bucket, 
        mock_create_bucket, 
        mock_setup_snowflake, 
        mock_list_files, 
        mock_process_doc,
        mocker
    ):
        """Test successful end-to-end workflow."""
        # Mock Secret for bucket name
        mock_secret_obj = Mock()
        mock_secret_obj.get.return_value = "test-bucket"
        mock_secret.load.return_value = mock_secret_obj
        
        # Mock S3 bucket and files
        mock_bucket_instance = Mock()
        mock_s3_bucket.load.return_value = mock_bucket_instance
        mock_list_files.return_value = ["doc1.pdf", "doc2.jpg"]
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        # Run the flow
        result = s3_to_snowflake_flow()
        
        # Verify infrastructure setup
        mock_create_bucket.assert_called_once_with("test-bucket")
        mock_setup_snowflake.assert_called_once()
        
        # Verify file processing
        mock_list_files.assert_called_once_with(bucket=mock_bucket_instance)
        assert mock_process_doc.call_count == 2
        mock_process_doc.assert_any_call(mock_bucket_instance, "doc1.pdf")
        mock_process_doc.assert_any_call(mock_bucket_instance, "doc2.jpg")

    @patch("flows.s3_to_snowflake.list_s3_files")
    @patch("flows.s3_to_snowflake.setup_snowflake_infrastructure")
    @patch("flows.s3_to_snowflake.create_s3_bucket_if_not_exists")
    @patch("flows.s3_to_snowflake.S3Bucket")
    @patch("flows.s3_to_snowflake.Secret")
    def test_s3_to_snowflake_flow_no_files(
        self, 
        mock_secret, 
        mock_s3_bucket, 
        mock_create_bucket, 
        mock_setup_snowflake, 
        mock_list_files,
        mocker
    ):
        """Test workflow when no files are found."""
        # Mock Secret for bucket name
        mock_secret_obj = Mock()
        mock_secret_obj.get.return_value = "test-bucket"
        mock_secret.load.return_value = mock_secret_obj
        
        # Mock empty file list
        mock_bucket_instance = Mock()
        mock_s3_bucket.load.return_value = mock_bucket_instance
        mock_list_files.return_value = []
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        # Run the flow
        result = s3_to_snowflake_flow()
        
        # Verify infrastructure setup still happened
        mock_create_bucket.assert_called_once_with("test-bucket")
        mock_setup_snowflake.assert_called_once()
        
        # Verify no file processing occurred
        mock_list_files.assert_called_once_with(bucket=mock_bucket_instance)

    @patch("flows.s3_to_snowflake.Secret")
    def test_s3_to_snowflake_flow_missing_bucket_name(self, mock_secret, mocker):
        """Test workflow fails when bucket name is missing."""
        mock_secret_obj = Mock()
        mock_secret_obj.get.return_value = None
        mock_secret.load.return_value = mock_secret_obj
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        with pytest.raises(ValueError, match="S3 bucket name is required"):
            s3_to_snowflake_flow()

    @patch("flows.s3_to_snowflake.load_to_snowflake")
    @patch("flows.s3_to_snowflake.extract_document_content")
    @patch("flows.s3_to_snowflake.get_s3_file")
    def test_process_document_success(
        self, 
        mock_get_file, 
        mock_extract, 
        mock_load,
        mocker
    ):
        """Test successful document processing."""
        mock_bucket = Mock()
        test_content = b"fake file content"
        test_extracted = {"data": {"markdown": "test"}}
        
        mock_get_file.return_value = test_content
        mock_extract.return_value = test_extracted
        
        mocker.patch("flows.s3_to_snowflake.get_run_logger")
        
        process_document(mock_bucket, "test.pdf")
        
        mock_get_file.assert_called_once_with(bucket=mock_bucket, file_key="test.pdf")
        mock_extract.assert_called_once_with(test_content, "test.pdf")
        mock_load.assert_called_once_with(file_key="test.pdf", extracted_content=test_extracted)