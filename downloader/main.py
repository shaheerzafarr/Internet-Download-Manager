"""
Simple CLI Download Manager for Windows.
Supports segmented (multi-connection) and single-stream downloads.
"""

import argparse
import asyncio
import re
import signal
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

import aiohttp
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

console = Console()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize_filename(name: str) -> str:
    """Remove or replace characters illegal in Windows filenames."""
    # Strip leading/trailing whitespace and dots
    name = name.strip().strip(".")
    # Replace illegal chars
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    # Reserved Windows names
    reserved = {
        "CON", "PRN", "AUX", "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }
    stem = Path(name).stem.upper()
    if stem in reserved:
        name = f"_{name}"
    return name or "download"


def extract_filename(url: str, headers: dict) -> str:
    """Derive a filename from Content-Disposition or the URL path."""
    cd = headers.get("Content-Disposition", "")
    if cd:
        # Try filename*= (RFC 5987) first, then filename=
        match = re.search(r"filename\*\s*=\s*(?:UTF-8''|utf-8'')(.+)", cd, re.IGNORECASE)
        if match:
            return sanitize_filename(unquote(match.group(1).strip().strip('"')))
        match = re.search(r'filename\s*=\s*"?([^";]+)"?', cd, re.IGNORECASE)
        if match:
            return sanitize_filename(match.group(1).strip())
    # Fall back to last URL path segment
    path = urlsplit(url).path
    name = unquote(path.rsplit("/", 1)[-1]) if "/" in path else ""
    return sanitize_filename(name) if name else "download"


# ---------------------------------------------------------------------------
# Probe
# ---------------------------------------------------------------------------

async def probe_url(session: aiohttp.ClientSession, url: str):
    """
    Issue a HEAD (then a Range-test GET) to discover:
      - final_url, filename, file_size, supports_range
    """
    async with session.head(url, allow_redirects=True) as resp:
        resp.raise_for_status()
        final_url = str(resp.url)
        headers = resp.headers
        filename = extract_filename(final_url, headers)
        content_length = resp.headers.get("Content-Length")
        file_size = int(content_length) if content_length else None

    # Test Range support
    supports_range = False
    if file_size and file_size > 0:
        range_headers = {"Range": "bytes=0-0"}
        async with session.get(final_url, headers=range_headers, allow_redirects=True) as resp:
            if resp.status == 206:
                supports_range = True

    return final_url, filename, file_size, supports_range


# ---------------------------------------------------------------------------
# Download workers
# ---------------------------------------------------------------------------

CHUNK_READ_SIZE = 1024 * 1024  # 1 MiB per network read
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


async def download_segment(
    session: aiohttp.ClientSession,
    url: str,
    part_path: Path,
    start: int,
    end: int,
    progress: Progress,
    task_id,
):
    """Download bytes [start, end] and write them at the correct offset."""
    written = 0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {"Range": f"bytes={start + written}-{end}"}
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                if resp.status not in (200, 206):
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message=f"Unexpected status {resp.status}",
                    )
                # Each worker opens its own file handle for thread-safe seeking
                with open(part_path, "r+b") as fh:
                    fh.seek(start + written)
                    async for chunk in resp.content.iter_chunked(CHUNK_READ_SIZE):
                        fh.write(chunk)
                        n = len(chunk)
                        written += n
                        progress.advance(task_id, n)
            return  # success
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            if attempt < MAX_RETRIES:
                console.print(
                    f"[yellow]Segment {start}-{end}: retry {attempt}/{MAX_RETRIES} "
                    f"after error: {exc}[/yellow]"
                )
                await asyncio.sleep(RETRY_DELAY * attempt)
            else:
                raise


async def download_single(
    session: aiohttp.ClientSession,
    url: str,
    part_path: Path,
    file_size: int | None,
    progress: Progress,
    task_id,
):
    """Single-stream download (no Range support or unknown size)."""
    written = 0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {}
            if written > 0:
                headers["Range"] = f"bytes={written}-"
            async with session.get(url, headers=headers, allow_redirects=True) as resp:
                if resp.status not in (200, 206):
                    raise aiohttp.ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message=f"Unexpected status {resp.status}",
                    )
                with open(part_path, "r+b" if written > 0 else "wb") as fh:
                    if written > 0:
                        fh.seek(written)
                    async for chunk in resp.content.iter_chunked(CHUNK_READ_SIZE):
                        fh.write(chunk)
                        n = len(chunk)
                        written += n
                        progress.advance(task_id, n)
            return
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
            if attempt < MAX_RETRIES:
                console.print(
                    f"[yellow]Retry {attempt}/{MAX_RETRIES} after error: {exc}[/yellow]"
                )
                await asyncio.sleep(RETRY_DELAY * attempt)
            else:
                raise


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

async def run(url: str, connections: int, dest_dir: Path):
    timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_read=60)
    connector = aiohttp.TCPConnector(limit=connections + 2)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # --- Probe ---
        console.print(f"[cyan]Probing:[/cyan] {url}")
        final_url, filename, file_size, supports_range = await probe_url(session, url)

        dest_dir.mkdir(parents=True, exist_ok=True)
        final_path = dest_dir / filename
        part_path = dest_dir / (filename + ".part")

        console.print(f"[cyan]File:[/cyan]  {filename}")
        if file_size is not None:
            size_mb = file_size / (1024 * 1024)
            console.print(f"[cyan]Size:[/cyan]  {size_mb:.2f} MiB")
        else:
            console.print("[cyan]Size:[/cyan]  unknown")
        console.print(f"[cyan]Range:[/cyan] {'yes' if supports_range else 'no'}")

        # Don't overwrite completed files
        if final_path.exists():
            console.print(f"[green]File already exists:[/green] {final_path}")
            return

        # Decide mode
        use_segments = supports_range and file_size and file_size > 0 and connections > 1
        actual_connections = connections if use_segments else 1
        console.print(f"[cyan]Connections:[/cyan] {actual_connections}\n")

        # --- Progress bar ---
        progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]{task.description}"),
            BarColumn(bar_width=40),
            "[progress.percentage]{task.percentage:>3.1f}%",
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        )

        task_id = progress.add_task(
            filename,
            total=file_size if file_size else None,
        )

        cancelled = False

        def on_sigint(sig, frame):
            nonlocal cancelled
            cancelled = True

        original_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, on_sigint)

        try:
            with progress:
                if use_segments:
                    # Preallocate file
                    with open(part_path, "wb") as fh:
                        fh.seek(file_size - 1)
                        fh.write(b"\0")

                    # Calculate ranges
                    chunk_size = file_size // actual_connections
                    ranges = []
                    for i in range(actual_connections):
                        start = i * chunk_size
                        end = (start + chunk_size - 1) if i < actual_connections - 1 else file_size - 1
                        ranges.append((start, end))

                    # Launch workers
                    tasks = [
                        asyncio.create_task(
                            download_segment(session, final_url, part_path, s, e, progress, task_id)
                        )
                        for s, e in ranges
                    ]

                    # Wait with cancellation check
                    while not all(t.done() for t in tasks):
                        if cancelled:
                            for t in tasks:
                                t.cancel()
                            break
                        await asyncio.sleep(0.2)

                    # Collect results / exceptions
                    for t in tasks:
                        if not t.cancelled():
                            exc = t.exception() if t.done() else None
                            if exc:
                                raise exc

                else:
                    # Single-stream download
                    dl_task = asyncio.create_task(
                        download_single(session, final_url, part_path, file_size, progress, task_id)
                    )
                    while not dl_task.done():
                        if cancelled:
                            dl_task.cancel()
                            break
                        await asyncio.sleep(0.2)

                    if not dl_task.cancelled() and dl_task.done():
                        exc = dl_task.exception()
                        if exc:
                            raise exc

        except asyncio.CancelledError:
            pass
        finally:
            signal.signal(signal.SIGINT, original_handler)

        if cancelled:
            console.print(
                f"\n[yellow]Download interrupted. Partial file preserved:[/yellow] {part_path}"
            )
            return

        # --- Verify & rename ---
        if file_size is not None:
            actual_size = part_path.stat().st_size
            if actual_size != file_size:
                console.print(
                    f"[red]Size mismatch! Expected {file_size}, got {actual_size}. "
                    f"Keeping .part file.[/red]"
                )
                return

        part_path.rename(final_path)
        console.print(f"\n[bold green]Download complete:[/bold green] {final_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Simple CLI Download Manager",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("url", help="URL to download")
    parser.add_argument(
        "-c", "--connections",
        type=int,
        default=8,
        help="Number of concurrent connections (default: 8)",
    )
    parser.add_argument(
        "-d", "--dest",
        type=str,
        default=".",
        help='Destination directory (default: current directory)',
    )

    args = parser.parse_args()

    if args.connections < 1:
        console.print("[red]Connections must be >= 1[/red]")
        sys.exit(1)

    dest = Path(args.dest).resolve()

    try:
        asyncio.run(run(args.url, args.connections, dest))
    except Exception as exc:
        console.print(f"[red]Error:[/red] {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
