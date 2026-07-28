import asyncio
from collections.abc import Awaitable, Callable


async def poll_until(check: Callable[[], Awaitable[bool]], attempts: int = 100, interval: float = 0.05) -> None:
    """Poll a real-time async predicate until it's true.

    Used instead of a fixed sleep whenever the test needs the workflow to
    have actually processed a signal and moved to a new state before the
    test sends the next one -- avoids races against the worker's poll loop.
    """
    for _ in range(attempts):
        if await check():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition was never satisfied within the polling window")
