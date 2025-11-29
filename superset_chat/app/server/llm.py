from enum import Enum
from typing import AsyncGenerator

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage, BaseMessage, \
    SystemMessage, ToolMessage, AIMessage, AIMessageChunk

from langchain_neo4j import GraphCypherQAChain, Neo4jGraph
from langchain.chains import FalkorDBQAChain
from langchain_community.graphs import FalkorDBGraph

from ..databases.postgres import Database
from ..models import ChatModel
from ..utils.logger import Logger

import os

from langchain_mcp_adapters.client import MultiServerMCPClient
from datetime import datetime
from langchain.tools import Tool

try:
    from langfuse.callback import CallbackHandler
    LANGFUSE_AVAILABLE = True
except ImportError:
    CallbackHandler = None
    LANGFUSE_AVAILABLE = False


PROMPT_MESSAGE = """Be a chatbot and help with your tools. Use Superset MCP to receive charts/dashboards/Superset metadata/ datasets / datasource.
So don't search for "n:Model {name: '{{datasource}}'}", search for Superset tools before."""


class LLMEventType(Enum):
    """Event types for the LLM agent."""

    STORED_MESSAGE = 'stored_message'
    RETRIEVER_START = 'on_retriever_start'
    RETRIEVER_END = 'on_retriever_end'
    CHAT_CHUNK = 'on_chat_model_stream'
    DONE = 'done'


class ChatMessage:
    class Sender(Enum):
        """The sender of the message."""
        SYSTEM = 'system'
        AI = 'ai'
        HUMAN = 'human'
        TOOL = 'tool'

    def __init__(self, type: LLMEventType, sender: Sender,
                 content: str, payload: dict = None):
        self.type = type
        self.sender: str = sender.value
        self.content = content
        self.payload = payload or {}

    @classmethod
    def from_base_message(cls, message: BaseMessage) -> 'ChatMessage':
        message_type_lookup = {
            HumanMessage: cls.Sender.HUMAN,
            SystemMessage: cls.Sender.SYSTEM,
            ToolMessage: cls.Sender.TOOL,
            AIMessage: cls.Sender.AI,
            AIMessageChunk: cls.Sender.AI,
        }

        # Different message types have different structures.
        try:
            content = message.content[0]['text']
        except (KeyError, TypeError, IndexError):
            content = message.content

        return ChatMessage(
            LLMEventType.STORED_MESSAGE,
            sender=message_type_lookup[type(message)],
            content=content,
        )

    @classmethod
    def from_event(cls, event: dict) -> 'ChatMessage':
        """Convert an event from the LLM agent to a `ChatMessage` object."""
        if event['event'] in ('on_tool_start', 'on_tool_end'):
            print(event)
            print('--------------------')
        match event['event']:
            case 'on_chat_model_stream':
                if event['data']['chunk'].content:
                    return cls._handle_on_chat_model_stream(event)
            case 'on_tool_start':
                return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI,
                                   f'''

Start Running Tool:
```
Data: {event['data']['input']}
Function: {event['name']}
```
''')
            case 'on_tool_end':
                return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI,
                                   f'''

Tool Output:
```
{event['data']['output'].content}
```
''')
                                #    '')
            # The conversation is done.
            case 'done':
                return ChatMessage(
                    LLMEventType.DONE,
                    cls.Sender.SYSTEM,
                    'Done',
                )
            # Known events that we ignore.
            case 'on_chat_model_start' | 'on_chain_start' | 'on_chain_end' \
                | 'on_chat_model_stream' | 'on_chat_model_end' | \
                    'on_chain_stream' | 'on_tool_start' | 'on_tool_end':
                Logger().get_logger().debug('Ignoring message', event['event'])
                return ''
            # Unknown events.
            case _:
                raise ValueError('Unknown event', event)

    @classmethod
    def _handle_on_chat_model_stream(cls, event: dict) -> 'ChatMessage':
        content = event['data']['chunk'].content
        content_type = ''
        if not isinstance(content, str):
            content_type = content[0]['type']
            content = content[0].get('text')

        # If the message is a tool call, just print a debug message.
        if content_type in ('tool_use', 'tool_call'):
            Logger().get_logger().debug('Stream.tool_calls:',
                                        event['data']['chunk'].tool_calls,
                                        flush=True)
            return ''
        else:
            return ChatMessage(LLMEventType.CHAT_CHUNK, cls.Sender.AI, content
                               if content is not None else '')

    def to_dict(self) -> dict:
        """Returns a dictionary representation of the message."""
        return {
            'sender': self.sender,
            'content': self.content,
            'payload': self.payload,
        }


class LLMAgent:

    def __init__(self, tools, md_uri=None):
        self._agent = None
        self._llm = None
        self.retriever_tool_name = 'Internal_Company_Info_Retriever'
        self._checkpointer_ctx = None
        self.tools = tools
        self.md_uri = md_uri

    async def __aenter__(self) -> 'LLMAgent':

        tools = self.tools
        self._llm = ChatModel()

        # Checkpointer for the agent.
        self._checkpointer_ctx = AsyncPostgresSaver\
            .from_conn_string(
                Database(self.md_uri).get_connection_string())
        checkpointer = await self._checkpointer_ctx.__aenter__()

        # Create the agent itself.
        self._agent = create_react_agent(
            self._llm,
            tools,
            checkpointer=checkpointer,
            # state_modifier=SystemMessage(PROMPT_MESSAGE),
            prompt=PROMPT_MESSAGE
        )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the agent and the checkpointer."""
        await self._checkpointer_ctx.__aexit__(exc_type, exc_val, exc_tb)
        self._llm = None
        self._agent = None
        self._checkpointer_ctx = None

    async def astream_events(self, message: str,
                             chat_session: dict) -> AsyncGenerator[ChatMessage,
                                                                   None]:
        async for event in self._agent.astream_events(
            {"messages": [HumanMessage(content=message)]},
            config=chat_session,
            version='v2',
        ):
            message = ChatMessage.from_event(event)
            if message:
                yield message

        # Let the client know that the conversation is done.
        # yield ChatMessage.from_event({'event': 'done'})


def get_user_chat_config(session_id: str, username: str = None) -> dict:
    chat_config = {'configurable': {'thread_id': session_id},
                   "recursion_limit": 100}
    if LANGFUSE_AVAILABLE and os.environ.get('LANGFUSE_HOST'):
        langfuse_handler = CallbackHandler(
                user_id=username if username else session_id,
                session_id=f"{session_id}",
                public_key=os.environ.get('LANGFUSE_PUBLIC_KEY'),
                secret_key=os.environ.get('LANGFUSE_SECRET_KEY'),
                host=os.environ.get('LANGFUSE_HOST')
            )
        chat_config['callbacks'] = [langfuse_handler]
    return chat_config


async def get_stream_agent_responce(session_id, message,
                                    md_uri: str = None,
                                    username: str = None):
    user_config = get_user_chat_config(session_id, username)
    mcp_host = os.environ.get('mcp_host', 'mcp_sse_server:8000')
    TRANSPORT_TYPE = os.environ.get('TRANSPORT_TYPE', 'stdio')
    if TRANSPORT_TYPE == 'stdio':
        mcps = {
                "SupersetMCP":
                {
                    'command': "python",
                    'args': ["-m", "superset_mcp_server.mcp_server"],
                    "transport": "stdio",
                    'env': {k: v for k, v in {
                        'SUPERSET_API_URL': os.getenv(
                            'SUPERSET_API_URL'),
                        'SUPERSET_USERNAME': os.getenv(
                            'SUPERSET_USERNAME'),
                        'SUPERSET_PASSWORD': os.getenv(
                            'SUPERSET_PASSWORD'),
                    }.items() if v is not None}
                }
            }
    elif TRANSPORT_TYPE == 'sse':
        mcps = {
                    "SupersetMCP": {
                        "url": f"http://{mcp_host}/sse",
                        "transport": "sse",
                        "headers": {"Authorization": f"""Bearer {
                            os.environ.get('MCP_TOKEN')}"""}
                    }
                }
    datetime_tool = Tool(
        name="Datetime",
        func=lambda x: datetime.now().isoformat(),
        description="Returns the current datetime",
    )
    client = MultiServerMCPClient(mcps)
    mcp_tools = await client.get_tools()
    tools = mcp_tools + [datetime_tool]

    dbt_prompt = '''Don't use this tool to receieve charts/dashboards/Superset/datasets/datasource metadata. This graph doesn't know anything about them. Find real database name from Superset MCP.
    The database schema includes:
- Node Types (labels): "Macro", "Model" (not dataset!!!!, dataset must be discovered in Superset API first of all and search by object name), "Operation", "Seed", "Snapshot", "Source" (source's name is "schema"."name" unlike all the other nodes where it's just "name"), "Test"
- Model attributes:
    access:	e.g. protected
    alias:	The actual object name in the database, which typically matches the model name but may differ in some cases.
    checksum	
    database
    description	
    enabled: true/false
    language: sql (can be python for some adapters)
    materialized: table, view, etc.
    meta
    name:	model name (not always dataset/datasource name from Superset!!! So always retrieve Dataset/datasource metadata before it from SupersetMCP)
    original_file_path	models/{{path}}/{{name}}.sql
    owner
    package_name
    path	{{path}}/{{name}}.sql
    relation_name	"{{database}}"."{{schema}}"."{{alias}}"
    resource_type	model
    schema	
    table_type	
    tags	[]
    unique_id	model.{{package_name}}.{{name}}
- Relationships:
    DEPENDS_ON: This relationship connects nodes with the "Model", "Snapshot" label to nodes with the "Model"/"Snapshot"/"Seed"/"Source" label
    REFERENCES: This relationship connects nodes with the "Model", "Snapshot" label to nodes with the "Model"/"Snapshot"/"Seed" label (not "Source")
    TESTS: This relationship connects nodes with the "Test" label to nodes with the "Model"/"Snapshot"/"Source"/"Seed" label
    USES_MACRO: This relationship connects nodes with the "Model"/"Snapshot" label to nodes with the "Macro" label
Use this retriever to answer questions about model lineage, dependencies, testing coverage, and data flow in our dbt project.
    '''
    if os.environ.get('GRAPH_DB') == 'falkordb':
        graph = FalkorDBGraph(host=os.environ.get('GRAPH_HOST', 'falkordb'),
                              port=6379, database="dbt_graph",
                              username=os.environ.get('GRAPH_USER'),
                              password=os.environ.get('GRAPH_PASSWORD'
                                                      ),)
        chain = FalkorDBQAChain.from_llm(ChatModel(), graph=graph, verbose=True, allow_dangerous_requests=True)
        retriever_tool = chain.as_tool(name="Falkor_Knowledge_Graph_Retriever_DBT",
                                            description=f"Query and retrieve dbt data from your Falkor graph database using Cypher syntax\n{dbt_prompt}")
        tools.append(retriever_tool)
    elif os.environ.get('GRAPH_DB') == 'neo4j':
        graph = Neo4jGraph(url=f"bolt://{os.environ.get('GRAPH_HOST', 'neo4j')}:7687",
                           username=os.environ.get('GRAPH_USER'),
                           password=os.environ.get('GRAPH_PASSWORD'),
                           enhanced_schema=True)
        chain = GraphCypherQAChain.from_llm(
            ChatModel(),
            graph=graph,
            verbose=True,
            allow_dangerous_requests=True,
        )
        retriever_tool = chain.as_tool(name="Neo4J_Knowledge_Graph_Retriever_DBT",
                                       description=f"Query your Neo4j graph database using Cypher to retrieve nodes, relationships, and insights.\n{dbt_prompt}")
        tools.append(retriever_tool)

    async def stream_agent_response():
        async with LLMAgent(tools=tools, md_uri=md_uri) as llm_agent:
            async for chat_msg in llm_agent.astream_events(
                 message, user_config):
                yield chat_msg.content
    return stream_agent_response
