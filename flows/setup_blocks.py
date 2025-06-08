#!/usr/bin/env python3
"""
Setup script to create Prefect blocks for the document extraction pipeline.
"""

import sys
from prefect_aws.credentials import AwsCredentials
from prefect_aws.s3 import S3Bucket
from prefect_snowflake.database import SnowflakeConnector
from prefect_snowflake.credentials import SnowflakeCredentials
from prefect.blocks.system import Secret

def create_aws_credentials(access_key_id: str, secret_access_key: str, bucket_name: str) -> bool:
    """Create AWS credentials and S3 bucket blocks."""
    try:
        # Create AWS credentials block
        aws_credentials_block = AwsCredentials(
            aws_access_key_id=access_key_id,
            aws_secret_access_key=secret_access_key
        )
        aws_credentials_block.save("aws-credentials", overwrite=True)
        print("✓ AWS credentials block created successfully")
        
        # Create S3 bucket block using the credentials
        s3_bucket_block = S3Bucket(
            bucket_name=bucket_name,
            credentials=aws_credentials_block
        )
        s3_bucket_block.save("default", overwrite=True)
        print("✓ S3 bucket block created successfully")
        
        # Also create individual secret blocks for AWS credentials
        Secret(value=access_key_id).save("aws-access-key-id", overwrite=True)
        Secret(value=secret_access_key).save("aws-secret-access-key", overwrite=True)
        print("✓ AWS credential secrets created successfully")
        return True
    except Exception as e:
        print(f"✗ Error creating AWS blocks: {str(e)}")
        return False

def create_snowflake_connector(
    user: str,
    private_key_path: str,
    account: str,
    database: str = "ai",
    schema: str = "AGENTIC_DOC_EXTRACTION",
    warehouse: str = "PREFECT_WH",
) -> bool:
    """Create Snowflake connector block using private key authentication."""
    try:
        # Read private key file
        with open(private_key_path, 'rb') as key_file:
            private_key = key_file.read()
            
        # Create credentials first
        snowflake_credentials = SnowflakeCredentials(
            user=user,
            private_key=private_key,
            account=account
        )
        snowflake_credentials.save("snowflake-credentials-key-auth", overwrite=True)
        print("✓ Snowflake credentials block created successfully")
            
        # Create connector with credentials
        snowflake_connector_block = SnowflakeConnector(
            credentials=snowflake_credentials,
            database=database,
            schema=schema,
            warehouse=warehouse
        )
        snowflake_connector_block.save("snowflake-credentials-key-auth", overwrite=True)
        print("✓ Snowflake connector block created successfully")
        return True
    except Exception as e:
        print(f"✗ Error creating Snowflake connector block: {str(e)}")
        return False

def create_secret_block(name: str, value: str) -> bool:
    """Create a secret block."""
    try:
        secret_block = Secret(value=value)
        secret_block.save(name, overwrite=True)
        print(f"✓ Secret block '{name}' created successfully")
        return True
    except Exception as e:
        print(f"✗ Error creating secret block '{name}': {str(e)}")
        return False

def main() -> None:
    """Main setup function."""
    if len(sys.argv) != 8:
        print(
            "Usage: python setup_blocks.py <aws_access_key> <aws_secret_key> "
            "<sf_user> <sf_private_key_path> <sf_account> <landing_ai_key> "
            "<s3_bucket_name>"
        )
        sys.exit(1)
    
    aws_access_key = sys.argv[1]
    aws_secret_key = sys.argv[2]
    sf_user = sys.argv[3]
    sf_private_key_path = sys.argv[4]
    sf_account = sys.argv[5]
    landing_ai_key = sys.argv[6]
    s3_bucket_name = sys.argv[7]
    
    print("Creating Prefect blocks...")
    
    success = True
    
    # Create AWS credentials and S3 bucket blocks
    if not create_aws_credentials(aws_access_key, aws_secret_key, s3_bucket_name):
        success = False
    
    # Create Snowflake connector block
    if not create_snowflake_connector(sf_user, sf_private_key_path, sf_account):
        success = False
    
    # Create Landing AI API key secret
    if not create_secret_block("landing-ai-api-key", landing_ai_key):
        success = False
    
    # Create S3 bucket name secret  
    if not create_secret_block("s3-bucket-name", s3_bucket_name):
        success = False
    
    if success:
        print("\n✓ All blocks created successfully!")
        sys.exit(0)
    else:
        print("\n✗ Some blocks failed to create. Check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main()