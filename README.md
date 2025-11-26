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

Add the plugin to your Superset configuration (`superset_config.py`):

```python
from superset_chat.ai_superset_assistant import AISupersetAssistantView

# Register the AI Assistant view
CUSTOM_SECURITY_MANAGER = None  # Use your custom security manager if needed

# Add to Flask-AppBuilder
FAB_ADD_SECURITY_VIEWS = True

# Register the custom view
def ADDON_MANAGER_POST_INIT(app):
    appbuilder = app.appbuilder
    appbuilder.add_view(
        AISupersetAssistantView,
        "AI Assistant",
        icon="fa-robot",
        category="AI",
        category_icon="fa-brain",
    )
```

## Usage

### Docker Compose

A complete Docker Compose setup is provided for quick start:

```bash
docker-compose up -d
```

This will start:
- Apache Superset on port 8088
- PostgreSQL database on port 5432

### Access the AI Assistant

1. Navigate to your Superset instance (default: http://localhost:8088)
2. Log in with your credentials
3. Find "AI Assistant" in the navigation menu
4. Start chatting with your AI assistant

### Example Queries

- "Show me all available dashboards"
- "What datasets do we have?"
- "Explain the lineage for the sales_model"
- "Create a chart showing monthly revenue"
- "What are the most popular dashboards?"

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
