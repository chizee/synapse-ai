"""
Extra engine paths: run_agent_step orchestration parameters and additional step
executor branches (merge strategies, if/else eval error, switch output_key,
extract_json multiple objects, print empty)."""
import types

import pytest

from _fakes import seed as S


def _server():
    return types.SimpleNamespace(agent_sessions={}, memory_store=None, tool_router={})


async def _run(orch_dict, fake_llm=None, script=None, initial_state=None):
    from core.models_orchestration import Orchestration
    from core.orchestration.engine import OrchestrationEngine
    if fake_llm is not None and script is not None:
        fake_llm.script(script)
    engine = OrchestrationEngine(Orchestration.model_validate(orch_dict), _server())
    return [ev async for ev in engine.run("go", run_id=f"run_{orch_dict['id']}", initial_state=initial_state)]


class TestRunAgentStepParams:
    async def test_orchestration_context_params(self, fake_llm):
        agent = S.make_agent(tools=["all"], skip_default_tools=True)
        fake_llm.set_default("done")
        from core.react_engine import run_agent_step
        events = [ev async for ev in run_agent_step(
            message="task", agent_id=agent["id"], session_id="s1", server_module=_server(),
            agent_override=agent, tools_override=[],
            system_prompt_extra="ORCHESTRATION AWARENESS BLOCK",
            system_prompt_prefix="ITERATION 2 BANNER",
            model_override="claude-y", source="orchestration", run_id="r1",
            allowed_tools_override=["all"], max_turns=1)]
        assert any(e.get("type") == "final" for e in events)
        # model_override took effect on the LLM call.
        assert fake_llm.last_call.get("current_model") == "claude-y"
        assert fake_llm.last_call.get("source") == "orchestration"


class TestMergeStrategies:
    @pytest.mark.parametrize("strategy", ["list", "concat", "dict"])
    async def test_merge_strategies(self, strategy):
        orch = S.make_orchestration(
            id=f"merge_{strategy}",
            entry_step_id="par",
            steps=[
                {"id": "par", "name": "Fan", "type": "parallel",
                 "parallel_branches": [["b1"], ["b2"]], "next_step_id": "mrg"},
                {"id": "b1", "name": "B1", "type": "print", "print_content": "x", "output_key": "r1", "next_step_id": None},
                {"id": "b2", "name": "B2", "type": "print", "print_content": "y", "output_key": "r2", "next_step_id": None},
                {"id": "mrg", "name": "M", "type": "merge", "merge_strategy": strategy,
                 "input_keys": ["r1", "r2"], "output_key": "m", "next_step_id": None},
            ],
        )
        events = await _run(orch)
        assert "orchestration_complete" in [e.get("type") for e in events]


class TestStepEdgeCases:
    async def test_if_else_eval_error_treated_false(self):
        orch = S.make_orchestration(
            entry_step_id="c",
            steps=[
                {"id": "c", "name": "C", "type": "if_else",
                 "if_condition": "state.missing.deep.attr == 1",  # will raise -> False
                 "if_true_step_id": "t", "if_false_step_id": "f", "next_step_id": None},
                {"id": "t", "name": "T", "type": "print", "print_content": "t", "output_key": "o", "next_step_id": None},
                {"id": "f", "name": "F", "type": "print", "print_content": "f", "output_key": "o", "next_step_id": None},
            ],
        )
        events = await _run(orch)
        decisions = [e for e in events if e.get("type") == "if_decision"]
        assert decisions and decisions[0]["result"] == "false"

    async def test_switch_with_output_key(self):
        orch = S.make_orchestration(
            entry_step_id="s",
            steps=[
                {"id": "s", "name": "S", "type": "switch", "switch_expression": "state.k",
                 "switch_cases": {"v": "a"}, "switch_default_step_id": "a",
                 "output_key": "chosen", "next_step_id": None},
                {"id": "a", "name": "A", "type": "print", "print_content": "a", "output_key": "o", "next_step_id": None},
            ],
        )
        events = await _run(orch, initial_state={"k": "v"})
        assert any(e.get("type") == "switch_decision" for e in events)

    async def test_extract_json_multiple_objects(self):
        orch = S.make_orchestration(
            entry_step_id="ex",
            steps=[{"id": "ex", "name": "Ex", "type": "extract_json",
                    "input_keys": ["blob"], "output_key": "parsed", "next_step_id": None}],
        )
        events = await _run(orch, initial_state={"blob": '{"a":1}\nand\n{"b":2}'})
        step_final = [e for e in events if e.get("type") == "final" and e.get("orch_step_id") == "ex"]
        assert step_final  # extracted a list of two objects

    async def test_print_empty_content_warns(self):
        orch = S.make_orchestration(
            entry_step_id="p",
            steps=[{"id": "p", "name": "P", "type": "print", "print_content": "   ",
                    "output_key": "o", "next_step_id": None}],
        )
        events = await _run(orch)
        assert any(e.get("type") == "step_warning" for e in events)


class TestInitialCheckpoint:
    async def test_run_file_exists_before_first_step_completes(self, tmp_path, monkeypatch):
        """The run must be visible to /runs listings from second zero — the
        checkpoint is written at run start, not first at the step boundary."""
        import json as _json
        import core.orchestration.state as state_mod
        from core.models_orchestration import Orchestration
        from core.orchestration.engine import OrchestrationEngine

        monkeypatch.setattr(state_mod, "RUNS_DIR", tmp_path / "runs")
        orch = S.make_orchestration(
            id="orch_initial_ckpt",
            entry_step_id="p",
            steps=[{"id": "p", "name": "P", "type": "print", "print_content": "x",
                    "output_key": "o", "next_step_id": None}],
        )
        engine = OrchestrationEngine(Orchestration.model_validate(orch), _server())
        agen = engine.run("go", run_id="run_early")

        # Consume ONLY orchestration_start — no step has run yet.
        first = await agen.__anext__()
        assert first["type"] == "orchestration_start"
        ckpt = _json.loads((tmp_path / "runs" / "run_early.json").read_text(encoding="utf-8"))
        assert ckpt["status"] == "running"
        assert ckpt["current_step_id"] == "p"
        assert ckpt["step_history"] == []

        # Finish the run — the same file transitions to completed.
        async for _ in agen:
            pass
        ckpt = _json.loads((tmp_path / "runs" / "run_early.json").read_text(encoding="utf-8"))
        assert ckpt["status"] == "completed"
