"""
main.py — Thin entry point for backward compatibility.

For new usage, install the package and use the `crawler` CLI:
  pip install -e .
  crawler --help

Or run directly:
  python -m crawler.main --help
"""

from crawler.main import cli

if __name__ == "__main__":
    cli()
