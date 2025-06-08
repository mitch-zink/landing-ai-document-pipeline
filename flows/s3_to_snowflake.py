"""Agentic Document Processing Pipeline using Landing AI's agentic-doc package"""

DEPLOY_MODE = False  # Set to False to run directly, True to deploy

from prefect import flow, task, serve, get_run_logger
from prefect_aws import S3Bucket
from prefect.blocks.system import Secret
from prefect.task_runners import ConcurrentTaskRunner
from datetime import timedelta
from typing import Dict, Any, List
import json, requests, base64, boto3
from botocore.exceptions import ClientError
from get_snowflake_connection import get_snowflake_connection

@task
def list_s3_files(bucket: S3Bucket) -> List[str]:
    """List all files in S3 bucket."""
    objects = bucket.list_objects()
    get_run_logger().info(f"S3 list_objects() returned: {objects}")
    # Extract just the keys from the object metadata
    files = [obj['Key'] for obj in objects] if objects else []
    get_run_logger().info(f"Found {len(files)} files in S3 bucket: {files}")
    return files

@task
def get_s3_file(bucket: S3Bucket, file_key: str) -> bytes:
    """Download file from S3."""
    content = bucket.read_path(file_key)
    get_run_logger().info(f"Downloaded {file_key}, size: {len(content)} bytes")
    return content

@task
def extract_document_content(file_content: bytes, file_key: str) -> Dict[str, Any]:
    """Extract document content using Landing AI official library."""
    import os
    import tempfile
    
    # Set the API key as environment variable (required by Landing AI library)
    api_key = Secret.load("landing-ai-api-key").get()
    if not api_key:
        raise ValueError("Landing AI API key is required")
    
    # Landing AI library expects VISION_AGENT_API_KEY environment variable
    os.environ['VISION_AGENT_API_KEY'] = api_key
    
    # Import after setting environment variable
    from agentic_doc.parse import parse
    get_run_logger().info(f"Making Landing AI API call for {file_key} using official library")
    
    try:
        # Write file content to temporary file (required by the library)
        with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{file_key}") as temp_file:
            temp_file.write(file_content)
            temp_file_path = temp_file.name
        
        # Use Landing AI official library
        results = parse(temp_file_path)
        
        # Clean up temp file
        os.unlink(temp_file_path)
        
        if results and len(results) > 0:
            result = results[0]
            get_run_logger().info(f"Landing AI extraction successful for {file_key}")
            
            # Convert chunks to JSON-serializable format
            serializable_chunks = []
            if hasattr(result, 'chunks') and result.chunks:
                for chunk in result.chunks:
                    chunk_dict = {
                        "text": chunk.text if hasattr(chunk, 'text') else str(chunk),
                        "chunk_type": chunk.chunk_type.value if hasattr(chunk, 'chunk_type') and hasattr(chunk.chunk_type, 'value') else "text",
                        "chunk_id": chunk.chunk_id if hasattr(chunk, 'chunk_id') else None
                    }
                    # Handle grounding information
                    if hasattr(chunk, 'grounding') and chunk.grounding:
                        chunk_dict["grounding"] = []
                        for grounding in chunk.grounding:
                            grounding_dict = {
                                "page": grounding.page if hasattr(grounding, 'page') else 0
                            }
                            if hasattr(grounding, 'box'):
                                grounding_dict["box"] = {
                                    "l": grounding.box.l if hasattr(grounding.box, 'l') else 0,
                                    "t": grounding.box.t if hasattr(grounding.box, 't') else 0,
                                    "r": grounding.box.r if hasattr(grounding.box, 'r') else 0,
                                    "b": grounding.box.b if hasattr(grounding.box, 'b') else 0
                                }
                            chunk_dict["grounding"].append(grounding_dict)
                    serializable_chunks.append(chunk_dict)
            
            # Convert to our expected format
            formatted_result = {
                "data": {
                    "markdown": result.markdown if hasattr(result, 'markdown') else str(result),
                    "chunks": serializable_chunks,
                    "extracted_schema": {"file_name": file_key, "file_size": len(file_content)},
                    "extraction_metadata": {"timestamp": "2025-06-06", "library": "agentic-doc"}
                }
            }
            get_run_logger().info(f"Landing AI result formatted: {formatted_result}")
            return formatted_result
        else:
            get_run_logger().warning("Landing AI returned empty results, using mock response")
            raise ValueError("Empty results")
            
    except Exception as e:
        get_run_logger().error(f"Landing AI library failed: {str(e)}")
        raise e

@task
def create_s3_bucket_if_not_exists(bucket_name: str) -> None:
    """Create S3 bucket if it doesn't exist."""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=Secret.load("aws-access-key-id").get(),
        aws_secret_access_key=Secret.load("aws-secret-access-key").get(),
        region_name='us-east-1'
    )
    
    try:
        head_response = s3_client.head_bucket(Bucket=bucket_name)
        get_run_logger().info(f"S3 head_bucket response: {head_response}")
        get_run_logger().info(f"S3 bucket '{bucket_name}' exists")
    except ClientError as e:
        get_run_logger().info(f"S3 head_bucket error: {e.response}")
        if e.response['Error']['Code'] == '404':
            create_response = s3_client.create_bucket(Bucket=bucket_name)
            get_run_logger().info(f"S3 create_bucket response: {create_response}")
            get_run_logger().info(f"Created S3 bucket '{bucket_name}'")
        else:
            raise

def _exec_sql(cur, sql: str, desc: str) -> None:
    """Execute SQL with logging and error handling."""
    try:
        get_run_logger().info(f"Executing SQL: {sql}")
        cur.execute(sql)
        result = cur.fetchall()
        get_run_logger().info(f"{desc} - SQL result: {result}")
        get_run_logger().info(f"{desc} - Row count: {cur.rowcount}")
    except Exception as e:
        get_run_logger().warning(f"Could not {desc.lower()}: {str(e)}")

@task
def setup_snowflake_infrastructure() -> None:
    """Create Snowflake infrastructure."""
    with get_snowflake_connection(warehouse="PREFECT_WH") as conn:
        with conn.cursor() as cur:
            # Try to create infrastructure with fallbacks
            _exec_sql(cur, "CREATE WAREHOUSE IF NOT EXISTS PREFECT_WH WITH WAREHOUSE_SIZE = 'X-SMALL'", "Create warehouse")
            _exec_sql(cur, "CREATE DATABASE IF NOT EXISTS ai", "Create database")
            _exec_sql(cur, "USE DATABASE ai", "Use database")
            _exec_sql(cur, "CREATE SCHEMA IF NOT EXISTS AGENTIC_DOC_EXTRACTION", "Create schema")
            _exec_sql(cur, "USE SCHEMA AGENTIC_DOC_EXTRACTION", "Use schema")
            
            # Show context
            try:
                cur.execute("SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()")
                context = cur.fetchall()[0]
                get_run_logger().info(f"Context - DB: {context[0]}, Schema: {context[1]}, WH: {context[2]}")
            except:
                pass
            
            # Create table with fallback
            table_sql = """CREATE TABLE IF NOT EXISTS ai.AGENTIC_DOC_EXTRACTION.DOCS (
                ID NUMBER AUTOINCREMENT PRIMARY KEY, FILE_NAME VARCHAR(255),
                FILE_PATH VARCHAR(500) UNIQUE, PROCESS_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                EXTRACTED_CONTENT VARIANT)"""
            try:
                _exec_sql(cur, table_sql, "Create qualified table")
            except:
                _exec_sql(cur, table_sql.replace("ai.AGENTIC_DOC_EXTRACTION.", ""), "Create simple table")

@task
def load_to_snowflake(file_key: str, extracted_content: Dict[str, Any]) -> None:
    """Load document content to Snowflake."""
    with get_snowflake_connection(warehouse="PREFECT_WH", database="ai", schema="AGENTIC_DOC_EXTRACTION") as conn:
        with conn.cursor() as cur:
            _exec_sql(cur, "USE DATABASE ai", "Use database")
            _exec_sql(cur, "USE SCHEMA AGENTIC_DOC_EXTRACTION", "Use schema")
            
            merge_sql = """MERGE INTO ai.AGENTIC_DOC_EXTRACTION.DOCS AS target
                USING (SELECT %s AS file_name, %s AS file_path, %s AS extracted_content) AS source
                ON target.FILE_PATH = source.file_path
                WHEN NOT MATCHED THEN INSERT (FILE_NAME, FILE_PATH, EXTRACTED_CONTENT)
                VALUES (source.file_name, source.file_path, PARSE_JSON(source.extracted_content))"""
            
            params = (file_key.split('/')[-1], file_key, json.dumps(extracted_content))
            try:
                get_run_logger().info(f"Executing MERGE SQL: {merge_sql}")
                get_run_logger().info(f"MERGE parameters: {params}")
                cur.execute(merge_sql, params)
                result = cur.fetchall()
                get_run_logger().info(f"MERGE result: {result}")
                get_run_logger().info(f"MERGE row count: {cur.rowcount}")
                get_run_logger().info(f"Loaded {file_key} to Snowflake")
            except Exception as e:
                get_run_logger().warning(f"Qualified MERGE failed: {str(e)}")
                # Fallback to simple table name
                simple_sql = merge_sql.replace("ai.AGENTIC_DOC_EXTRACTION.", "")
                get_run_logger().info(f"Trying simple MERGE: {simple_sql}")
                cur.execute(simple_sql, params)
                result = cur.fetchall()
                get_run_logger().info(f"Simple MERGE result: {result}")
                get_run_logger().info(f"Simple MERGE row count: {cur.rowcount}")
                get_run_logger().info(f"Loaded {file_key} to Snowflake (simple)")

@task
def process_document(aws_credentials: S3Bucket, file_key: str) -> None:
    """Process document: download, extract, load to Snowflake."""
    file_content = get_s3_file(bucket=aws_credentials, file_key=file_key)
    extracted_content = extract_document_content(file_content, file_key)
    load_to_snowflake(file_key=file_key, extracted_content=extracted_content)
    get_run_logger().info(f"Processed document: {file_key}")

@flow(
    name="Agentic Document Processing",
    description="Process documents from S3 using Landing AI's agentic-doc package and store in Snowflake"
)
def s3_to_snowflake_flow():
    """Main flow: S3 → Landing AI → Snowflake."""
    s3_bucket_name = Secret.load("s3-bucket-name").get()
    if not s3_bucket_name:
        raise ValueError("S3 bucket name is required")
    
    get_run_logger().info(f"Processing bucket: {s3_bucket_name}")
    
    # Setup infrastructure and get files
    create_s3_bucket_if_not_exists(s3_bucket_name)
    setup_snowflake_infrastructure()
    aws_credentials = S3Bucket.load("default")
    files = list_s3_files(bucket=aws_credentials)
    
    if not files:
        get_run_logger().info("No files found")
        return
    
    # Process files sequentially
    get_run_logger().info(f"Processing {len(files)} files sequentially")
    for file_key in files:
        process_document(aws_credentials, file_key)

if __name__ == "__main__":
    if DEPLOY_MODE:
        # Create and serve deployment with hourly schedule
        deployment = s3_to_snowflake_flow.to_deployment(
            name="s3-to-snowflake-extraction",
            description="Extract documents from S3 using Landing AI and load to Snowflake",
            tags=["s3", "snowflake", "document-extraction", "landing-ai", "agentic"],
            version="1.0.0",
            interval=timedelta(hours=1)
        )
        
        print("🚀 Serving deployment: S3 to Snowflake Document Extraction")
        print("📊 Monitor at: http://localhost:4200")
        print("▶️  To run: prefect deployment run 'S3 to Snowflake Document Extraction/s3-to-snowflake-extraction'")
        
        serve(deployment)
    else:
        # Run flow directly
        print("🔄 Running flow directly...")
        result = s3_to_snowflake_flow()
        print(f"✅ Flow completed: {result}")