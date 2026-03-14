"""Console alerts when a device goes offline."""
from rich.console import Console
from rich.panel import Panel

# force_terminal=True so ANSI colors are emitted in Docker (non-TTY); terminal or log viewer can then render them
_console = Console(force_terminal=True)


def alert_device_offline(device_id: str, last_seen_ago_seconds: float, total_offline_count: int) -> None:
    """Emit a visible console alert for a device that just went offline."""
    _console.print(
        Panel(
            f"[bold red]Device [yellow]{device_id}[/yellow] is OFFLINE[/bold red]\n"
            f"Last seen: [dim]{last_seen_ago_seconds:.0f}s ago[/dim]\n"
            f"Total offline count: [bold]{total_offline_count}[/bold]",
            title="[bold]⚠ Device Watchdog Alert[/bold]",
            border_style="red",
        )
    )
