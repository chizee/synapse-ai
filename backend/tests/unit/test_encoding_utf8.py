"""Regression tests for #352: Synapse must read/write its own files as UTF-8,
never the platform default (cp1252 on Windows).

The reported crash was an orchestration checkpoint write:
    UnicodeEncodeError: 'charmap' codec can't encode characters ...
      state.py, in checkpoint: f.write(self.run.model_dump_json(indent=2))
because os.fdopen(fd, "w") used the platform-default encoding. These tests
assert non-ASCII content round-trips and is persisted as UTF-8 bytes.
"""
import json

NON_ASCII = "café résumé — €uro ✓ Société 日本語"


class TestOrchestrationCheckpoint:
    def test_checkpoint_roundtrips_non_ascii(self, tmp_path, monkeypatch):
        from core.orchestration import state as state_mod
        from core.models_orchestration import OrchestrationRun

        monkeypatch.setattr(state_mod, "RUNS_DIR", tmp_path)
        run = OrchestrationRun(run_id="utf8-run", orchestration_id="o1",
                               shared_state={"contrat": NON_ASCII})

        # Before the fix this raises UnicodeEncodeError on a cp1252 platform.
        state_mod.SharedState(run).checkpoint()

        raw = (tmp_path / "utf8-run.json").read_bytes()
        decoded = raw.decode("utf-8")          # file must be valid UTF-8
        assert NON_ASCII in decoded            # raw non-ASCII (model_dump_json), not \u-escaped
        assert "\\u00e9" not in decoded

        restored = state_mod.SharedState.restore("utf8-run")
        assert restored.run.shared_state["contrat"] == NON_ASCII

    def test_list_runs_reads_non_ascii(self, tmp_path, monkeypatch):
        from core.orchestration import state as state_mod
        from core.models_orchestration import OrchestrationRun

        monkeypatch.setattr(state_mod, "RUNS_DIR", tmp_path)
        run = OrchestrationRun(run_id="r1", orchestration_id=NON_ASCII)
        state_mod.SharedState(run).checkpoint()

        runs = state_mod.SharedState.list_runs()
        assert any(r["orchestration_id"] == NON_ASCII for r in runs)


class TestJsonStore:
    def test_roundtrips_non_ascii_as_utf8(self, tmp_path):
        from core.json_store import JsonStore

        store = JsonStore(str(tmp_path / "data.json"))
        store.save([{"name": NON_ASCII}])

        raw = (tmp_path / "data.json").read_bytes()
        # json.dump escapes to ASCII, but the file must still decode as UTF-8
        # and the value must survive a load round-trip.
        raw.decode("utf-8")
        assert JsonStore(str(tmp_path / "data.json")).load() == [{"name": NON_ASCII}]
