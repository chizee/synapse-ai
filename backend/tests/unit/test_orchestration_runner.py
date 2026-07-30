"""
The shared engine runner (core.orchestration.runner).

The contract it exists to enforce: an engine stream is advanced by exactly one
task, so anyio cancel scopes the engine holds across yields are entered and
exited in the same task (issue #356). It also owns the _active_tasks registry
and keeps runs detached from their SSE consumer.
"""
import asyncio

import anyio


class TestSingleTaskOwnership:
    async def test_source_is_advanced_by_one_task(self):
        from core.orchestration.runner import stream_engine_events

        tasks = []

        async def _src():
            for i in range(4):
                tasks.append(asyncio.current_task())
                yield {"type": "step_complete", "i": i}

        events = [ev async for ev in stream_engine_events(_src(), "run_one_task")]
        assert len(events) == 4
        assert len(set(id(t) for t in tasks)) == 1, "source was advanced by more than one task"

    async def test_cancel_scope_held_across_yield_survives(self):
        """The #356 shape: the engine wraps its yields in anyio.fail_after."""
        from core.orchestration.runner import stream_engine_events

        async def _src():
            with anyio.fail_after(30):
                yield {"type": "step_start"}
                yield {"type": "final"}
            yield {"type": "orchestration_complete"}

        events = [ev async for ev in stream_engine_events(_src(), "run_scope")]
        assert [e["type"] for e in events] == [
            "step_start", "final", "orchestration_complete",
        ]


class TestActiveTaskRegistry:
    async def test_registered_while_running_and_popped_when_done(self):
        from core.orchestration.runner import _active_tasks, stream_engine_events

        gate = asyncio.Event()

        async def _src():
            yield {"type": "step_start"}
            await gate.wait()
            yield {"type": "orchestration_complete"}

        stream = stream_engine_events(_src(), "run_registry")
        assert await stream.__anext__() == {"type": "step_start"}
        assert "run_registry" in _active_tasks

        gate.set()
        assert await stream.__anext__() == {"type": "orchestration_complete"}
        with_stop = [ev async for ev in stream]
        assert with_stop == []
        assert "run_registry" not in _active_tasks

    async def test_run_survives_consumer_going_away(self):
        """Runs are detached: abandoning the stream must not kill the engine."""
        from core.orchestration.runner import _active_tasks, stream_engine_events

        finished = asyncio.Event()

        async def _src():
            yield {"type": "step_start"}
            await asyncio.sleep(0.01)
            yield {"type": "orchestration_complete"}
            finished.set()

        stream = stream_engine_events(_src(), "run_detached")
        assert await stream.__anext__() == {"type": "step_start"}
        task = _active_tasks["run_detached"]

        await stream.aclose()  # client disconnected
        await asyncio.wait_for(finished.wait(), timeout=1.0)
        assert not task.cancelled()
        assert "run_detached" not in _active_tasks


class TestErrorMapping:
    async def test_missing_run_becomes_orchestration_error(self):
        from core.orchestration.runner import stream_engine_events

        async def _src():
            raise FileNotFoundError("checkpoint gone")
            yield  # pragma: no cover — makes _src an async generator

        events = [ev async for ev in stream_engine_events(_src(), "run_missing")]
        assert events == [{"type": "orchestration_error", "error": "Run not found"}]

    async def test_unexpected_error_becomes_orchestration_error(self):
        from core.orchestration.runner import stream_engine_events

        async def _src():
            yield {"type": "step_start"}
            raise ValueError("boom")

        events = [ev async for ev in stream_engine_events(_src(), "run_boom")]
        assert events[0] == {"type": "step_start"}
        assert events[1] == {"type": "orchestration_error", "error": "boom"}

    async def test_cancelled_run_reports_cancelled(self):
        from core.orchestration.runner import _active_tasks, spawn_engine_run, SENTINEL

        async def _src():
            yield {"type": "step_start"}
            await asyncio.sleep(30)
            yield {"type": "never"}  # pragma: no cover

        task, queue = spawn_engine_run(_src(), "run_cancel")
        assert await queue.get() == {"type": "step_start"}
        task.cancel()
        assert await queue.get() == {"type": "orchestration_error", "error": "Cancelled"}
        assert await queue.get() is SENTINEL
        assert "run_cancel" not in _active_tasks
