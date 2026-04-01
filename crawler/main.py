"""
main.py — Click CLI for the self-healing crawler.

Commands:
  crawler demo               Full 4-run demonstration (v1 → v2 redesign)
  crawler crawl <url>        Crawl a live URL
  crawler local <path>       Crawl a local HTML file
  crawler stats              Show cache + DB statistics
"""

from __future__ import annotations


import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Optional

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box

console = Console()


def _configure_logging(log_level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("playwright").setLevel(logging.WARNING)
    logging.getLogger("anthropic").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)


def _print_result(result, label: str = "") -> None:
    d = result.data
    t = Table(box=box.SIMPLE, show_header=False, padding=(0, 1))
    t.add_column("Field", style="bold cyan", min_width=14)
    t.add_column("Value")

    t.add_row("Method", result.method.upper())
    t.add_row("Cache Hit", "[green]YES[/]" if result.cache_hit else "[red]NO[/]")
    t.add_row("Drift", "[yellow]DETECTED[/]" if result.drift_detected else "[green]NONE[/]")
    t.add_row("Confidence", f"{result.confidence:.0%}")
    t.add_row("LLM Tokens", str(result.llm_tokens_used))
    t.add_row("DOM Hash", f"{result.raw_html_hash}...")
    t.add_row("", "")
    t.add_row("Title", d.title or "(null)")
    t.add_row("Price", f"{d.currency or ''}{d.price or '(null)'}")
    t.add_row("In Stock", str(d.in_stock))
    t.add_row("Rating", str(d.rating))
    t.add_row("Reviews", str(d.review_count))
    t.add_row("Brand", d.brand or "(null)")
    t.add_row("SKU", d.sku or "(null)")
    t.add_row("Description", ((d.description or "(null)")[:80] + "..."))

    if result.selectors_used:
        t.add_row("", "")
        s = result.selectors_used
        for field in ("title", "price", "in_stock", "rating", "brand"):
            val = getattr(s, field, None)
            if val:
                t.add_row(f"sel:{field}", val)

    console.print(Panel(t, title=f"[bold]{label or 'EXTRACTION RESULT'}[/]"))


@click.group()
@click.option("--log-level", default="INFO", help="Logging level (DEBUG/INFO/WARNING)")
def cli(log_level: str) -> None:
    """Self-healing web crawler with LLM-powered selector regeneration."""
    _configure_logging(log_level)


@cli.command()
@click.option("--schema", default=None, help="Schema name override (CRAWLER_SCHEMA_NAME)")
def demo(schema: Optional[str]) -> None:
    """Run the full 4-step self-healing demo using mock product pages."""
    asyncio.run(_run_demo(schema))


@cli.command()
@click.argument("url")
@click.option("--schema", default=None, help="Schema name override")
def crawl(url: str, schema: Optional[str]) -> None:
    """Crawl a live URL and print extracted product data."""
    asyncio.run(_run_url(url, schema))


@cli.command("local")
@click.argument("path", type=click.Path(exists=True))
@click.option("--schema", default=None, help="Schema name override")
def local_file(path: str, schema: Optional[str]) -> None:
    """Crawl a local HTML file and print extracted product data."""
    asyncio.run(_run_local(path, schema))


@cli.command()
def stats() -> None:
    """Show cache and database statistics."""
    asyncio.run(_show_stats())


# ── Async implementations ─────────────────────────────────────────────────────

async def _run_demo(schema: Optional[str]) -> None:
    from crawler.orchestrator import CrawlerOrchestrator

    use_llm = bool(os.environ.get("ANTHROPIC_API_KEY"))
    if not use_llm:
        console.print(
            "[yellow]ANTHROPIC_API_KEY not set — LLM calls will be skipped.[/]"
        )

    # Clean up stale state from previous runs
    for _f in ("/tmp/demo_selector_cache.json", "/tmp/demo_crawler_data.db"):
        if Path(_f).exists():
            Path(_f).unlink()

    mock_dir = Path(__file__).parent.parent
    v1_path = str(mock_dir / "product_v1.html")
    v2_path = str(mock_dir / "product_v2.html")
    url = "https://mockshop.com/product/techgear-pro-x500"

    async with CrawlerOrchestrator(
        cache_path="/tmp/demo_selector_cache.json",
        db_path="/tmp/demo_crawler_data.db",
        schema_name=schema,
        use_llm=use_llm,
    ) as crawler:
        console.rule("[bold]RUN 1: First visit — no cache, LLM extraction")
        r1 = await crawler.crawl(url=url, local_html_path=v1_path)
        _print_result(r1, "RUN 1 — LLM extraction, selectors cached")

        console.rule("[bold]RUN 2: Same page — expecting CACHE HIT")
        r2 = await crawler.crawl(url=url, local_html_path=v1_path)
        _print_result(r2, "RUN 2 — Cache hit, fast extraction")
        if r2.cache_hit:
            console.print("[green]Cache hit confirmed — zero LLM tokens used![/]")
        else:
            console.print(f"[yellow]Expected cache hit but got: {r2.method}[/]")

        console.rule("[bold]RUN 3: SITE REDESIGN — all IDs removed, structure changed")
        r3 = await crawler.crawl(url=url, local_html_path=v2_path)
        _print_result(r3, "RUN 3 — Drift detected, LLM re-extracts")
        if r3.drift_detected:
            console.print("[green]Drift detected — old selectors invalidated, LLM re-extracted.[/]")

        console.rule("[bold]RUN 4: Revisit v2 — new selectors should be cached")
        r4 = await crawler.crawl(url=url, local_html_path=v2_path)
        _print_result(r4, "RUN 4 — Cache hit with new selectors")

        stats_data = await crawler.get_stats()
        db = stats_data["db"]
        t = Table(title="Final Statistics", box=box.SIMPLE)
        t.add_column("Metric", style="bold cyan")
        t.add_column("Value")
        t.add_row("Total extractions", str(db["total_extractions"]))
        t.add_row("LLM runs", str(db["llm_runs"]))
        t.add_row("Cache hits", str(db["cache_hits"]))
        t.add_row("Cache hit rate", f"{db['cache_hit_rate']:.0%}")
        t.add_row("Total LLM tokens", str(db["total_llm_tokens"]))
        t.add_row("Drift events", str(db["drift_events"]))
        console.print(t)
        console.print("[bold green]Demo complete. Self-healing pipeline validated.[/]")


async def _run_url(url: str, schema: Optional[str]) -> None:
    from crawler.orchestrator import CrawlerOrchestrator

    async with CrawlerOrchestrator(schema_name=schema) as crawler:
        console.print(f"Crawling: [bold]{url}[/]")
        result = await crawler.crawl(url=url)
        _print_result(result)


async def _run_local(path: str, schema: Optional[str]) -> None:
    from crawler.orchestrator import CrawlerOrchestrator

    url = f"https://local-file/{Path(path).name}"
    async with CrawlerOrchestrator(schema_name=schema) as crawler:
        console.print(f"Loading local file: [bold]{path}[/]")
        result = await crawler.crawl(url=url, local_html_path=path)
        _print_result(result)


async def _show_stats() -> None:
    from crawler.selector_cache import SelectorCache
    from crawler.db import CrawlerDB

    cache = SelectorCache()
    db = CrawlerDB()
    console.print_json(json.dumps({"cache": cache.stats(), "db": await db.summary_stats()}, indent=2))


if __name__ == "__main__":
    cli()
