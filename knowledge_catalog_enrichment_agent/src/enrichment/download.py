# Metadata download tool
#

import argparse
import pathlib

from enrichment import catalog


def main():
  parser = argparse.ArgumentParser(description='Download metadata from the Catalog')
  parser.add_argument(
    '--dataset',
    required=True,
    help='The dataset to download metadata for.',
  )
  parser.add_argument(
    '--dir',
    required=True,
    help='The directory to save the metadata into.',
  )
  args = parser.parse_args()

  dir = pathlib.Path(args.dir)
  dir.mkdir(parents=True, exist_ok=True)

  catalog.download_entries(dir, args.dataset)


if __name__ == '__main__':
  main()
