# Agentic Document Processing Pipeline

Uses Landing AI's agentic AI package to process documents from S3, orchestrated with Prefect, stored in Snowflake.

```text
📄 S3 Documents → 🤖 Agentic AI → 📝 Markdown → ❄️ Snowflake
```

## Quick Start

1. **Set environment variables:**

```bash
export AWS_ACCESS_KEY_ID=""
export AWS_SECRET_ACCESS_KEY=""
export SNOWFLAKE_USER=""
export SNOWFLAKE_ACCOUNT=""
export SNOWFLAKE_PRIVATE_KEY_PATH=""
export LANDING_AI_API_KEY=""
export S3_BUCKET_NAME=""
```

2. **Install and deploy:**

```bash
git clone <repo_url>
cd agentic-document-extraction-s3-to-snowflake
./setup_mac.sh
```

The setup script installs Python 3.12, creates a virtual environment, installs the agentic-doc package, configures Prefect blocks, and starts the pipeline server.

**Important**: Your Snowflake private key file must be in raw format (no headers/footers):

```text
MIIEvQIBADANBgkqhkiG9w0BAQEFAaSCBKcwggSjAgEAAoIBAQC...
```

Not PEM format with headers:

```text
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAaSCBKcwggSjAgEAAoIBAQC...
-----END PRIVATE KEY-----
```

## Usage

```bash
# Run via Prefect deployment
prefect deployment run 'Agentic Document Processing/agentic-doc-pipeline'

# Or run directly
python flows/s3_to_snowflake.py
```

## Snowflake Schema

```sql
CREATE TABLE AI.AGENTIC_DOC_EXTRACTION.DOCS (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    FILE_NAME VARCHAR(255),
    FILE_PATH VARCHAR(500) UNIQUE,
    PROCESS_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXTRACTED_CONTENT VARIANT
);
```

## Requirements

- macOS with Homebrew
- Python 3.12+
- Cloud credentials (AWS, Snowflake, Landing AI)
