# Superset MCP Plugins

AI Assistant plugin for Apache Superset with Model Context Protocol (MCP) integration and dbt graph support.

## Overview

This plugin extends Apache Superset with an intelligent AI assistant that can interact with your Superset instance, query metadata, and provide insights about your dashboards, charts, and datasets. It leverages LangChain, LangGraph, and the Model Context Protocol (MCP) to provide a seamless conversational interface for data exploration and analysis.

## Features

- **AI-Powered Chat Interface**: Interactive chat UI integrated directly into Apache Superset
- **MCP Integration**: Connect to Superset via Model Context Protocol for powerful tool-based interactions
- **dbt Graph Support**: Query and analyze dbt model lineage and dependencies through graph databases (Neo4j/FalkorDB)
- **Multiple LLM Providers**: Support for OpenAI, Anthropic Claude, and AWS Bedrock
- **Session Management**: Persistent chat sessions with PostgreSQL checkpointing
- **Streaming Responses**: Real-time streaming of AI responses for better user experience
- **Authentication**: Integrated with Apache Superset's authentication system

## Architecture

The plugin consists of several key components:

- **AI Assistant View** (`ai_superset_assistant.py`): Flask-AppBuilder view providing the chat interface
- **LLM Agent** (`app/server/llm.py`): LangGraph-based agent orchestrating tool calls and responses
- **MCP Client**: Connects to Superset MCP server for accessing Superset APIs
- **Graph Database Integration**: Optional dbt lineage visualization via Neo4j or FalkorDB
- **Model Inference**: Pluggable LLM backends (Anthropic, OpenAI, Bedrock)

## Installation

### Using Poetry

```bash
poetry install
```

### Using pip

```bash
pip install superset-chat
```

## Configuration

### Environment Variables

Configure the following environment variables:

```bash
# Database Configuration
SQLALCHEMY_DATABASE_URI=postgresql://user:password@host:port/database

# Superset API Configuration
SUPERSET_API_URL=http://localhost:8088
SUPERSET_USERNAME=admin
SUPERSET_PASSWORD=admin

# MCP Configuration
TRANSPORT_TYPE=stdio  # or 'sse'
mcp_host=mcp_sse_server:8000  # if using SSE transport
MCP_TOKEN=your_token  # if using SSE transport

# LLM Provider (choose one)
# For OpenAI
OPENAI_API_KEY=your_openai_key

# For Anthropic
ANTHROPIC_API_KEY=your_anthropic_key

# For AWS Bedrock
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

# Optional: Graph Database for dbt
GRAPH_DB=neo4j  # or 'falkordb'
GRAPH_HOST=neo4j
GRAPH_USER=neo4j
GRAPH_PASSWORD=password

# Optional: Langfuse Observability
LANGFUSE_HOST=https://cloud.langfuse.com
LANGFUSE_PUBLIC_KEY=your_public_key
LANGFUSE_SECRET_KEY=your_secret_key
```

### Superset Integration

The plugin integrates with Apache Superset using the `FLASK_APP_MUTATOR` configuration option. Add the following to your `superset_config.py`:

```python
import logging

logger = logging.getLogger()

# Flask-AppBuilder Init Hook for custom views
FLASK_APP_MUTATOR = lambda app: init_custom_views(app)

def init_custom_views(app):
    """Initialize custom views after Flask app is created"""
    try:
        from superset_chat.ai_superset_assistant import AISupersetAssistantView

        # Get the appbuilder instance
        appbuilder = app.appbuilder

        # Register the view
        appbuilder.add_view(
            AISupersetAssistantView,
            "AI Superset Assistant",
            icon="fa-robot",
            category="Custom Tools"
        )

        logger.info("✅ AI Superset Assistant plugin registered successfully!")

    except Exception as e:
        logger.error(f"❌ Failed to register AI Superset Assistant plugin: {e}")
        import traceback
        logger.error(traceback.format_exc())
```

**Why `FLASK_APP_MUTATOR`?**

`FLASK_APP_MUTATOR` is the recommended modern approach for registering custom views in Superset. It's called after the Flask application is fully initialized, ensuring all dependencies are available.

**Installation in Docker**

If you're using Docker, add the plugin installation to your Dockerfile:

```dockerfile
FROM apache/superset:4.1.1

USER root

# Install dependencies
RUN pip install psycopg2-binary Pillow

# Install the plugin
RUN pip install superset-chat==0.1.0a11

# Copy your superset_config.py
COPY ./superset_config.py /app/superset_config.py
ENV SUPERSET_CONFIG_PATH /app/superset_config.py
```

For development, you can mount the plugin code as volumes in docker-compose.yaml:

```yaml
services:
  superset:
    volumes:
      - ./superset_chat/app:/app/app
      - ./superset_chat/templates:/app/templates
      - ./superset_chat/ai_superset_assistant.py:/app/ai_superset_assistant.py
```

## Usage

### Quick Start with Docker Compose

A complete Docker Compose setup is provided for quick start:

```bash
# Clone the repository
git clone https://github.com/ponderedw/superset-mcp-plugins.git
cd superset-mcp-plugins

# Create .env file with required variables
cat > .env << EOF
# Admin password for Superset
ADMIN_PASSWORD=superset

# Database configuration
DATABASE_HOST=postgres
DATABASE_NAME=postgres
DATABASE_USER=postgres
DATABASE_PASSWORD=postgres

# Superset API credentials
SUPERSET_API_URL=http://superset:8088
SUPERSET_USERNAME=superset_admin
SUPERSET_PASSWORD=superset

# LLM Provider (add your API key)
ANTHROPIC_API_KEY=your_anthropic_api_key_here
# OR
# OPENAI_API_KEY=your_openai_api_key_here

# MCP Transport
TRANSPORT_TYPE=stdio
EOF

# Start the services
docker-compose up -d

# Check logs to verify plugin installation
docker-compose logs superset | grep "AI Superset Assistant"
```

This will start:
- Apache Superset on port 8088
- PostgreSQL database on port 5432

### Access the AI Assistant

1. Navigate to your Superset instance: http://localhost:8088
2. Log in with credentials:
   - Username: `superset_admin`
   - Password: `superset` (or value from `ADMIN_PASSWORD` env var)
3. Look for **"Custom Tools"** in the top navigation menu
4. Click on **"AI Superset Assistant"** (with robot icon)
5. Start chatting with your AI assistant

### Example Queries

Once in the chat interface, try these queries:

- "Show me all available dashboards"
- "What datasets do we have?"
- "Explain the lineage for the sales_model"
- "Create a chart showing monthly revenue"
- "What are the most popular dashboards?"
- "How many charts are in the analytics dashboard?"
- "What datasources are connected to Superset?"

### API Endpoints

The plugin provides the following REST endpoints:

- **POST** `/ai_superset_assistant/api/new_session` - Create a new chat session
- **POST** `/ai_superset_assistant/api/chat` - Send a message (synchronous response)
- **POST** `/ai_superset_assistant/api/chat_stream` - Send a message (streaming response)
- **POST** `/ai_superset_assistant/api/clear_session` - Clear a chat session

Example API usage:

```bash
# Create a new session
curl -X POST http://localhost:8088/ai_superset_assistant/api/new_session \
  -H "Content-Type: application/json" \
  -b cookies.txt

# Send a message with streaming
curl -X POST http://localhost:8088/ai_superset_assistant/api/chat_stream \
  -H "Content-Type: application/json" \
  -b cookies.txt \
  -d '{"message": "Show me all dashboards", "session_id": "your-session-id"}'
```

### Troubleshooting

**Plugin not appearing in menu:**
- Check Superset logs: `docker-compose logs superset | grep "AI Superset Assistant"`
- Verify the plugin is installed: `docker-compose exec superset pip show superset-chat`
- Ensure `FLASK_APP_MUTATOR` is configured in `superset_config.py`

**Authentication errors:**
- Verify you're logged into Superset
- Check that your user has proper permissions
- The plugin requires authenticated users

**LLM errors:**
- Verify your LLM API key is set correctly in the `.env` file
- Check logs for specific error messages
- Ensure you have internet connectivity for API calls

**Database connection errors:**
- Verify `SQLALCHEMY_DATABASE_URI` is set correctly
- Check that PostgreSQL is running and accessible
- Ensure the database user has proper permissions

## Development

### Project Structure

```
superset-mcp-plugins/
├── superset_chat/
│   ├── ai_superset_assistant.py    # Main Flask view
│   ├── app/
│   │   ├── databases/              # Database connectors
│   │   ├── models/                 # LLM model implementations
│   │   │   └── inference/          # Model-specific inference
│   │   ├── server/                 # LLM agent server
│   │   └── utils/                  # Utility functions
│   └── templates/                  # HTML templates
├── superset/                       # Superset configuration
├── docker-compose.yaml             # Docker setup
└── pyproject.toml                  # Python dependencies
```

### Adding Custom Tools

To add custom tools to the AI agent, modify `app/server/llm.py`:

```python
from langchain.tools import Tool

custom_tool = Tool(
    name="CustomTool",
    func=your_function,
    description="Description of what your tool does",
)

# Add to tools list before creating the agent
tools.append(custom_tool)
```

## Dependencies

Key dependencies include:
- Flask & Flask-AppBuilder
- LangChain, LangGraph, LangSmith
- Model Context Protocol (MCP)
- SQLAlchemy & PostgreSQL
- LLM providers (OpenAI, Anthropic, AWS)
- Optional: Neo4j/FalkorDB for graph operations

See `pyproject.toml` for complete dependency list.

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Authors

Maintained by [Ponder](https://github.com/ponderedw)
