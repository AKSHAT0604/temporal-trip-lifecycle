"""A minimal saga compensation stack.

As each side-effecting activity succeeds, the workflow pushes a zero-argument
coroutine that undoes it. On any failure path, the stack unwinds in reverse
(LIFO) order, which is what makes "release the driver before voiding the
payment hold" fall out naturally from the order things were acquired in,
rather than needing to be hand-coded per failure path.
"""

from collections.abc import Awaitable, Callable


class CompensationStack:
    def __init__(self) -> None:
        self._stack: list[tuple[str, Callable[[], Awaitable[None]]]] = []

    def push(self, name: str, compensate: Callable[[], Awaitable[None]]) -> None:
        self._stack.append((name, compensate))

    async def unwind_last(self) -> None:
        if not self._stack:
            return
        _name, compensate = self._stack.pop()
        await compensate()

    async def unwind(self) -> None:
        while self._stack:
            _name, compensate = self._stack.pop()
            await compensate()
