"""Single-task runner for orchestration engine streams.

The engine holds anyio cancel scopes across `yield`s (the per-step timeout at
`engine._execute_loop`). anyio binds a cancel scope to the task that entered
it, so the engine's generator must be advanced -- and closed -- from ONE task,
whatever the HTTP layer does with the events. A consumer that pulls each event
in a fresh task raises "Attempted to exit cancel scope in a different task"
(issue #356).

Every orchestration entry point goes through here, so the launch path can no
longer change how the engine is driven.
"""
import asyncio
from typing import AsyncGenerator

# Live in-process runs, keyed by run_id. The cancel endpoint cancels the task
# to interrupt an in-progress await; the run-status endpoint consults it to
# report a run that is still inside its first step (no checkpoint yet).
_active_tasks: dict[str, asyncio.Task] = {}

# Queue marker meaning "the engine generator is exhausted".
SENTINEL = object()


def spawn_engine_run(agen, run_id: str) -> tuple[asyncio.Task, asyncio.Queue]:
    """Advance `agen` in one dedicated task, returning (task, queue).

    The task is detached: it runs to completion even if nobody drains the
    queue, so a client disconnect never kills a run. Only the cancel endpoint
    stops it.
    """
    queue: asyncio.Queue = asyncio.Queue()

    async def _pump():
        try:
            async for event in agen:
                await queue.put(event)
                # Yield so the SSE consumer can dequeue and flush this event
                # to the HTTP response before the next one is enqueued.
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            await queue.put({"type": "orchestration_error", "error": "Cancelled"})
        except FileNotFoundError:
            await queue.put({"type": "orchestration_error", "error": "Run not found"})
        except Exception as e:
            await queue.put({"type": "orchestration_error", "error": str(e)})
        finally:
            _active_tasks.pop(run_id, None)
            print(f"DEBUG SSE QUEUE: sentinel sent for '{run_id}', stream closing", flush=True)
            await queue.put(SENTINEL)

    task = asyncio.create_task(_pump())
    _active_tasks[run_id] = task
    return task, queue


async def stream_engine_events(agen, run_id: str) -> AsyncGenerator[dict, None]:
    """Yield the engine's event dicts while a dedicated task advances it.

    Deliberately does NOT cancel that task on close: the run is detached, so
    abandoning this generator (client disconnect, GC finalization) must leave
    it running -- the run still checkpoints and stays cancellable and
    resumable.
    """
    _task, queue = spawn_engine_run(agen, run_id)
    while True:
        event = await queue.get()
        if event is SENTINEL:
            return
        yield event
