# Metadata enrichment agent
#

import contextlib
import io
import json
import os
import pathlib
import typing as t
import uuid

from google.adk.agents.llm_agent import LlmAgent
from google.adk.cli.utils import logs
from google.adk.code_executors.unsafe_local_code_executor import UnsafeLocalCodeExecutor
from google.adk.runners import InMemoryRunner
from google.adk.sessions.session import Session
from google.adk.skills import load_skill_from_dir
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from google.adk.tools.mcp_tool.mcp_session_manager import StreamableHTTPConnectionParams
from google.adk.tools.mcp_tool.mcp_toolset import McpToolset
from google.adk.tools.skill_toolset import SkillToolset
from google.genai import types
from mcp import StdioServerParameters

with contextlib.redirect_stdout(io.StringIO()):
  # Logs will be in /tmp/agents_log/
  logs.log_to_tmp_folder()


APP_NAME = 'agent'
MODEL_NAME = 'gemini-2.5-flash'
SYSTEM_PROMPT = """
You are a data documentation agent. Your goal is find information about a BigQuery
table and extract relevant details, to generate readme-style documentation that can
improve discovery and data understanding.

Workflow:
1. Notify that you are enriching the metadata for the table.

2. Find the additional organizational information for the table using the specified
   sources and avaiable tools. Summarize the information you find to produce useful
   documentation for the table, what it contains, how it can be used, any tips or
   guidelines for using it.
   
   Be thorough and comprehensive. Include all the documentation that can be useful
   to users of the table, based on the information you find, along with citations
   to the sources.
   
   Generate definitive and easy to read statements. Avoid ambiguous statements like
   may, could, likely, etc.
   
   Generate markdown documentation in the following format:

<generated documentation>

<list of citations>

3. Do not directly output the markdown content to the user. Instead, use the
   `update_table` tool to update the table with the generated documentation.
   Pass the table name, and the generated documentation.

Here are the specific instructions on what information sources to use for generating
documentation.

{user_instructions}

Only use the above information to generate documentation. If you do not find any
useful information, just produce an empty documentation. Do not guess or 
fabricate information.
"""


def _load_instructions(instructions_path: pathlib.Path) -> str:
  if instructions_path.exists():
    return instructions_path.read_text(encoding='utf-8')
  return ''


def _load_mcp_toolsets(mcp_config_path: pathlib.Path) -> list[McpToolset]:
  if not mcp_config_path.exists():
    return []

  try:
    with open(mcp_config_path, 'r') as f:
      config = json.load(f)
  except json.JSONDecodeError:
    return []

  toolsets = []
  mcp_servers = config.get('mcpServers', {})
  for name, server_config in mcp_servers.items():
    if 'command' in server_config:
      command = os.path.expandvars(server_config['command'])
      args = [os.path.expandvars(a) for a in server_config.get('args', [])]
      env = server_config.get('env')
      timeout = server_config.get('timeout', 30.0)

      toolsets.append(
          McpToolset(
              connection_params=StdioConnectionParams(
                  server_params=StdioServerParameters(
                      command=command, args=args, env=env
                  ),
                  timeout=timeout,
              )
          )
      )
    elif 'httpUrl' in server_config:
      url = server_config['httpUrl']
      timeout = server_config.get('timeout', 30.0)

      toolsets.append(
          McpToolset(
              connection_params=StreamableHTTPConnectionParams(
                  url=url,
                  timeout=timeout,
              )
          )
      )
  return toolsets


def _load_skills(skills_dir: pathlib.Path) -> SkillToolset | None:
  if not skills_dir.exists() or not skills_dir.is_dir():
    return []

  skills = []
  for skill_path in skills_dir.iterdir():
    if skill_path.is_dir():
      try:
        skills.append(load_skill_from_dir(skill_path))
      except Exception as e:
        pass

  if not skills:
    return []

  # WARNING: UnsafeLocalCodeExecutor has security concerns and should NOT
  # be used in production environments.
  return SkillToolset(skills=skills, code_executor=UnsafeLocalCodeExecutor())


def create_agent(tools: list[t.Any], config_dir: pathlib.Path) -> InMemoryRunner:
  instructions = SYSTEM_PROMPT.format(
      user_instructions=_load_instructions(config_dir / 'instructions.md')
  )

  all_tools = tools.copy()
  mcp_toolsets = _load_mcp_toolsets(config_dir / 'mcp.json')
  if mcp_toolsets:
    all_tools.extend(mcp_toolsets)
  skills_toolset = _load_skills(config_dir / 'skills')
  if skills_toolset:
    all_tools.append(skills_toolset)

  agent = LlmAgent(
      model=MODEL_NAME,
      name=APP_NAME,
      description='Enrichment Agent',
      instruction=instructions,
      tools=all_tools,
  )
  return InMemoryRunner(agent=agent, app_name=APP_NAME)


async def run_agent(runner: InMemoryRunner, prompt: str):
  user = str(uuid.uuid4())
  session = await runner.session_service.create_session(
      app_name=APP_NAME, user_id=user
  )

  async for event in runner.run_async(
      user_id=user,
      session_id=session.id,
      new_message=types.Content(
          role='user', 
          parts=[types.Part.from_text(text=prompt)]
      )
  ):
    if event.content and event.content.parts:
      for part in event.content.parts:
        if part.text:
          print(f'{event.author} >\n{part.text}')
        elif part.function_call:
          print(f'{event.author} >\nTool Invoke: {part.function_call.name}\n{part.function_call.args}')
        elif part.function_response:
          print(f'{event.author} >\nTool Result: {part.function_response.name}\n{part.function_response.response}')
