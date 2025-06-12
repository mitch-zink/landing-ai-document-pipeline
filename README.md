# Landing AI Document Intelligence Pipeline

Production-ready pipeline for transforming unstructured documents into structured, queryable data using Landing AI's agentic API. Automatically processes documents from S3 and loads the parsed content into Snowflake.

## 🤖 Why Agentic Document AI?

Landing AI's agentic API uses autonomous AI that adapts to any document type without templates:

- **Contextual Understanding**: Comprehends document structure and relationships between elements
- **Semantic Chunking**: Intelligently segments content into meaningful units with spatial coordinates
- **Multi-Modal Processing**: Handles mixed content (text, tables, charts) in a single pass
- **Zero Configuration**: No templates or rules - the AI figures out document structure autonomously

## 🏗️ Architecture

```
S3 Documents → Landing AI Agentic API → Structured JSON → Snowflake
     ↑                                                        ↓
     └──────────── Prefect Orchestration (Hourly) ───────────┘
```

## 🛠️ Prerequisites

- macOS with Homebrew installed
- Python 3.12+ (will be installed by setup script)
- AWS account with S3 access
- Snowflake account with private key authentication configured
- Landing AI API key (get one at [Landing AI](https://landing.ai))

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/ai-document-extraction-pipeline.git
cd ai-document-extraction-pipeline
```

### 2. Set Environment Variables

```bash
export AWS_ACCESS_KEY_ID="your-aws-access-key"
export AWS_SECRET_ACCESS_KEY="your-aws-secret-key"
export SNOWFLAKE_USER="your-snowflake-username"
export SNOWFLAKE_ACCOUNT="your-account.region"
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/private-key.pem"
export LANDING_AI_API_KEY="your-landing-ai-api-key"
export S3_BUCKET_NAME="your-s3-bucket-name"
```

### 3. Run Automated Setup

```bash
./setup_mac.sh
```

The setup script will:

- Install Python 3.12 via Homebrew
- Create and activate a virtual environment
- Install all required dependencies including Landing AI's agentic API
- Configure Prefect for local orchestration
- Create secure credential blocks for AWS, Snowflake, and Landing AI
- Deploy the document processing pipeline with hourly scheduling
- Start the Prefect server and open the monitoring UI

### ⚠️ Important: Snowflake Private Key Format

Your Snowflake private key must be in raw base64 format without PEM headers:

✅ **Correct format:**

```text
MIIEvQIBADANBgkqhkiG9w0BAQEFAaSCBKcwggSjAgEAAoIBAQC...
```

❌ **Incorrect format (PEM with headers):**

```text
-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAaSCBKcwggSjAgEAAoIBAQC...
-----END PRIVATE KEY-----
```

To convert from PEM format:

```bash
openssl rsa -in private_key.pem -out private_key_raw.txt
tail -n +2 private_key_raw.txt | head -n -1 | tr -d '\n' > private_key_clean.txt
```

## 📖 Usage

### Running the Pipeline

Once deployed, the pipeline runs automatically every hour. You can also trigger manual runs:

```bash
# Trigger a manual run via Prefect deployment
prefect deployment run 'Agentic Document Processing/agentic-doc-pipeline'

# Or run the flow directly (for testing/debugging)
python flows/s3_to_snowflake.py
```

### Monitoring

Access the Prefect UI to monitor your flows:

```bash
open http://localhost:4200
```

The UI provides:

- Real-time flow execution status
- Detailed logs for each task
- Flow run history and statistics
- Error tracking and debugging tools

## 🧠 Technical Deep Dive

### How Agentic Processing Works

The agentic-doc API uses transformer-based models that understand documents holistically:

1. **Document Analysis**: Multi-modal AI processes the entire document context
2. **Semantic Segmentation**: Creates meaningful chunks based on content relationships
3. **Spatial Mapping**: Preserves exact location data for each element
4. **Structure Inference**: Automatically detects tables, lists, headers without rules

### API Integration

```python
from agentic_doc.parse import parse

# Single API call handles any document type
results = parse(document_path)

# Returns rich structured data
document = results[0]
markdown = document.markdown  # Full document as markdown
chunks = document.chunks      # Semantic segments with metadata
```

## 📊 Output Schema

Each document is transformed into structured JSON with full content preservation:

```json
{
  "markdown": "# Complete document content as markdown",
  "chunks": [
    {
      "text": "Document segment content",
      "chunk_type": "table|paragraph|list|header",
      "grounding": [
        {
          "page": 1,
          "box": { "l": 100, "t": 200, "r": 300, "b": 400 }
        }
      ]
    }
  ]
}
```

**What You Get:**

- Full document content as clean markdown text
- Document segments with type classification
- Exact spatial coordinates for each element
- Preserved document structure and hierarchy

## 📸 Example Results

![Landing AI Extraction](screenshots/extracted_content.png)
_Document content parsed and structured by Landing AI_

![Snowflake Table Output](screenshots/table_output.png)
_Parsed documents stored in Snowflake for querying_

## 💾 Data Pipeline

### Snowflake Schema

```sql
CREATE TABLE AI.AGENTIC_DOC_EXTRACTION.DOCS (
    ID NUMBER AUTOINCREMENT PRIMARY KEY,
    FILE_NAME VARCHAR(255),
    FILE_PATH VARCHAR(500) UNIQUE,
    PROCESS_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXTRACTED_CONTENT VARIANT  -- Stores full JSON output
);
```

### Query Examples

```sql
-- Get full document content as markdown
SELECT FILE_NAME, EXTRACTED_CONTENT:data:markdown::string as document_content
FROM AI.AGENTIC_DOC_EXTRACTION.DOCS
WHERE PROCESS_DATE >= CURRENT_DATE();

-- Query specific document segments
SELECT
    FILE_NAME,
    chunk.value:text::string as segment_text,
    chunk.value:chunk_type::string as segment_type
FROM AI.AGENTIC_DOC_EXTRACTION.DOCS,
    LATERAL FLATTEN(input => EXTRACTED_CONTENT:data:chunks) chunk
WHERE chunk.value:chunk_type = 'table';
```

## 🔧 Implementation Details

### Core Pipeline Logic

```python
@task
def extract_document_content(file_content: bytes, file_key: str) -> Dict[str, Any]:
    """Parse document content using Landing AI's agentic-doc."""
    from agentic_doc.parse import parse

    # Write to temp file (required by API)
    with tempfile.NamedTemporaryFile(suffix=f"_{file_key}") as temp_file:
        temp_file.write(file_content)
        results = parse(temp_file.name)

    # Transform parsed content to JSON format
    return {
        "data": {
            "markdown": results[0].markdown,
            "chunks": [serialize_chunk(chunk) for chunk in results[0].chunks],
            "metadata": {"file_name": file_key}
        }
    }
```

### Key Dependencies

- **agentic-doc** (>=0.1.0): Landing AI's document intelligence API
- **prefect** (>=3.0.0): Workflow orchestration
- **snowflake-connector-python**: Data warehouse integration

## 🚀 Why This Matters

Traditional document processing requires:

- Custom parsers for each document type
- Constant maintenance as formats change
- Manual intervention for edge cases

Landing AI's agentic approach:

- **One API, Any Document**: No templates or configuration needed
- **Self-Improving**: AI adapts to new document types automatically
- **Production Scale**: Process thousands of documents reliably

[Get your Landing AI API key](https://landing.ai) to start building intelligent document workflows.
