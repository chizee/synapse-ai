"""
Boot every native MCP tool server and list its tools (runs in the gate).

This is the test that would have caught the mcp 2.0.0 breakage: a fresh install
resolved an unpinned `mcp`, got the 2.0 rewrite, and *every* tool server died at
import because the lowlevel `Server` no longer exposes `@server.list_tools()`.
The backend swallowed each failure as a warning, started anyway with an empty
`tool_router`, and the UI showed no native tools.

Nothing in the rest of the suite spawns a tool server or opens a `ClientSession`,
so the suite stayed green while the product was completely broken. This closes
that hole by doing the real thing: spawn the server as a subprocess, complete the
stdio handshake, and ask it for its tools — mirroring
`core.server.initialize_agents` exactly.

Because it exercises both halves of the protocol, it catches both classes of SDK
drift at once:

  * server side — the `@app.list_tools()` / `@app.call_tool()` decorators
  * client side — `ClientSession(read_timeout_seconds=...)`, whose type changed
    from `timedelta` to `float` in mcp 2.0 (`unsupported operand type(s) for +:
    'float' and 'datetime.timedelta'`)

Parametrized over the tool registry so a failure names the offending tool.
"""
from __future__ import annotations

import os
import sys
from datetime import timedelta

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from core.tools_registry import ALL_NATIVE_TOOLS

# Generous enough for the heavy importers (code_search pulls cocoindex,
# web_scraper pulls crawl4ai) on a cold CI runner, short enough that a hung
# server fails the job instead of stalling it.
_BOOT_TIMEOUT = timedelta(seconds=90)

_BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _boot_and_list(script_path: str):
    """Spawn one tool server over stdio and return its ListToolsResult.

    Mirrors core.server.initialize_agents: same interpreter, same PYTHONPATH
    injection (tool scripts import `core.*` and `services.*`), same transport.
    """
    env = os.environ.copy()
    env["PYTHONPATH"] = _BACKEND_ROOT + os.pathsep + env.get("PYTHONPATH", "")

    params = StdioServerParameters(command=sys.executable, args=[script_path], env=env)

    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write, read_timeout_seconds=_BOOT_TIMEOUT) as session:
            await session.initialize()
            return await session.list_tools()


@pytest.mark.parametrize("tool_name,script_path", sorted(ALL_NATIVE_TOOLS.items()))
async def test_native_tool_server_boots_and_advertises_tools(tool_name: str, script_path: str):
    """Every server in ALL_NATIVE_TOOLS must start and advertise >=1 tool.

    A failure here means the tool is invisible to agents at runtime — the
    backend logs `Failed to connect agent '<name>'` and carries on with a
    silently degraded tool_router.
    """
    assert os.path.isfile(script_path), f"{tool_name}: script missing at {script_path}"

    result = await _boot_and_list(script_path)

    assert result.tools, f"{tool_name}: server started but advertised no tools"
    for tool in result.tools:
        assert tool.name, f"{tool_name}: advertised a tool with no name"
        assert tool.inputSchema is not None, f"{tool_name}: tool {tool.name!r} has no inputSchema"


# Collisions that already exist and are not addressed here.
#
# `read_file_by_lines` is advertised by both code_search.py and file_reader.py
# with genuinely different behaviour — code_search's takes a `repo_id` and
# searches active repositories, file_reader's is the lightweight vault/S3 variant
# with no index. Since `core.server.tool_router` is keyed by bare tool name and
# file_reader connects last, file_reader's version wins and code_search's
# repo-aware one is unreachable through the router.
#
# Tracked separately — fixing it is a routing/namespacing decision (see how
# core/routes/tools.py namespaces *external* servers as `f"{name}__{tool.name}"`),
# not part of the dependency-pinning work. Listed here so the test guards against
# *new* collisions without hiding this one.
_KNOWN_TOOL_NAME_COLLISIONS = {"read_file_by_lines"}


async def test_no_new_tool_name_collisions_across_servers():
    """`core.server.tool_router` is keyed by bare tool name, so a collision
    between two servers silently shadows one of them — last connection wins."""
    seen: dict[str, str] = {}
    collisions: list[str] = []

    for tool_name, script_path in sorted(ALL_NATIVE_TOOLS.items()):
        result = await _boot_and_list(script_path)
        for tool in result.tools:
            if tool.name in seen:
                if tool.name not in _KNOWN_TOOL_NAME_COLLISIONS:
                    collisions.append(f"{tool.name!r} in both {seen[tool.name]!r} and {tool_name!r}")
            else:
                seen[tool.name] = tool_name

    assert not collisions, (
        "New tool name collisions would shadow entries in tool_router: " + "; ".join(collisions)
    )
