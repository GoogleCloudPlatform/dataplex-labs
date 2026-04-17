# Metadata enrichment agent
#

import argparse
import asyncio
import pathlib
import sys

import enrichment.agent as agent
from enrichment import catalog


async def main():
  parser = argparse.ArgumentParser(description='Metadata enrichment agent')
  parser.add_argument(
    '--dir',
    required=True,
    help='Directory containing the metadata entries',
  )
  parser.add_argument(
    '--config-dir',
    required=True,
    help='Directory containing instructions.md, mcp.json, skills/',
  )
  args = parser.parse_args()

  metadata_dir = pathlib.Path(args.dir).resolve()
  if not metadata_dir.exists() or not metadata_dir.is_dir():
    print(f'Error: {metadata_dir} does not exist or is not a directory.')
    sys.exit(1)

  config_dir = pathlib.Path(args.config_dir).resolve()
  if not config_dir.exists() or not config_dir.is_dir():
    print(f'Error: {config_dir} does not exist or is not a directory.')
    sys.exit(1)


  def update_table(table_name: str, content: str) -> str:
    '''Updates the documentation content for a table.

    Args:
      table_name: The BigQuery table name in the format 'project_id.dataset_id.table_id'.
      content: The generated documentation content.

    Returns:
      A success message or an error message.
    '''
    try:
      catalog.update_entry(metadata_dir, table_name, content)
      return f'Successfully updated {table_name}'
    except Exception as e:
      return f'Failed to update {table_name}: {e}'


  runner = agent.create_agent([update_table], config_dir)

  tables = catalog.list_entries(metadata_dir)
  for table in tables:
    prompt = f'Enrich the metadata for table {table}'

    await agent.run_agent(runner, prompt)


if __name__ == '__main__':
  asyncio.run(main())
