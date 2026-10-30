"""Installer for DataFlow-Evolve plugin.

Usage:
  # Install to default locations (~/. dirs)
  python install.py --target claude
  python install.py --target codex
  python install.py --target both

  # Install into one specific Claude Code install. Pass the CLAUDE_CONFIG_DIR
  # that install's launcher exports (e.g. run-claude.sh exports $WORKSPACE/.claude).
  python install.py --target claude --claude-config-dir /path/to/.claude \
      --claude-bin /path/to/bin/claude

  # Install into one specific Codex install (its CODEX_HOME)
  python install.py --target codex --codex-home /path/to/.codex

  # Copy skills, then print the registration command instead of running it
  python install.py --target claude --print-config

Credentials:
  Registration never embeds an API key, so changing keys never needs a
  reinstall. Each workspace stores only env-var NAMES plus an optional
  credentials_file path; key values are resolved at call time from that file
  (re-read on every call) and then from the MCP server's process env. To switch
  keys between experiments, edit the credentials file or point the workspace at
  another one with rsi_set_credential_env.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent
MCP_ENTRY = REPO_ROOT / "rsi_mcp" / "server.py"
SERVER_NAME = "dataflow-evolve"


# ── skill installer (shared) ──────────────────────────────────────────────────

def _install_skills(src_dir: Path, skills_dir: Path, provider: str) -> None:
    """Copy all skills from src_dir (the plugin's bundled skills/) into target."""
    skills_dir.mkdir(parents=True, exist_ok=True)

    for skill_dir in sorted(src_dir.iterdir()):
        if not skill_dir.is_dir():
            continue
        dst = skills_dir / skill_dir.name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(skill_dir, dst)
        print(f"[{provider}] skill: {skill_dir.name} -> {dst}")


# ── Claude Code ────────────────────────────────────────────────────────────────

def _claude_add_cmd(claude_bin: str, python_exe: str) -> list[str]:
    return [claude_bin, "mcp", "add", "-s", "user", SERVER_NAME,
            "--", python_exe, str(MCP_ENTRY)]


def _print_claude_config(claude_bin: str, python_exe: str,
                         config_dir: Path | None) -> None:
    prefix = f"CLAUDE_CONFIG_DIR={config_dir} " if config_dir else ""
    print("\n--- Run this to register the MCP server manually ---")
    print(prefix + " ".join(_claude_add_cmd(claude_bin, python_exe)))
    print("---")


def install_claude(python_exe: str, config_dir: Path | None = None,
                   claude_bin: str = "claude", print_config: bool = False) -> None:
    """Install skills and register the MCP server for one Claude Code install.

    A Claude Code install is identified by its CLAUDE_CONFIG_DIR (default ~/.claude).
    `claude mcp add -s user` writes into $CLAUDE_CONFIG_DIR/.claude.json, so
    running it with CLAUDE_CONFIG_DIR set registers the server for that install
    only and leaves every other install untouched.
    """
    home = config_dir or (Path.home() / ".claude")
    home.mkdir(parents=True, exist_ok=True)

    _install_skills(REPO_ROOT / "skills" / "claude", home / "skills", "claude")

    if print_config:
        _print_claude_config(claude_bin, python_exe, config_dir)
        return

    env = dict(os.environ)
    if config_dir is not None:
        env["CLAUDE_CONFIG_DIR"] = str(config_dir)
    # Re-registering must replace a previous entry, so remove it first; a
    # missing entry makes remove fail, which is expected.
    subprocess.run([claude_bin, "mcp", "remove", "-s", "user", SERVER_NAME],
                   env=env, capture_output=True)
    try:
        subprocess.run(_claude_add_cmd(claude_bin, python_exe), env=env,
                       check=True, capture_output=True, text=True)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        detail = getattr(exc, "stderr", "") or str(exc)
        print(f"[claude] `claude mcp add` failed: {detail.strip()}")
        _print_claude_config(claude_bin, python_exe, config_dir)
        return
    target = config_dir if config_dir is not None else "default (~/.claude.json)"
    print(f"[claude] MCP server {SERVER_NAME} registered for CLAUDE_CONFIG_DIR={target}")


# ── Codex ─────────────────────────────────────────────────────────────────────

def _print_codex_config(python_exe: str) -> None:
    snippet = (
        f"\n[mcp_servers.{SERVER_NAME}]\n"
        f'command = "{python_exe}"\n'
        f'args = ["{MCP_ENTRY}"]\n'
    )
    print("\n--- Manual config snippet (append to ~/.codex/config.toml) ---")
    print(snippet)
    print("---")


def install_codex(python_exe: str, codex_home: Path | None = None,
                  print_config: bool = False) -> None:
    home = codex_home or (Path.home() / ".codex")
    home.mkdir(parents=True, exist_ok=True)

    _install_skills(REPO_ROOT / "skills" / "codex", home / "skills", "codex")

    if print_config:
        _print_codex_config(python_exe)
        return

    config = home / "config.toml"
    marker = f"[mcp_servers.{SERVER_NAME}]"
    existing = config.read_text(encoding="utf-8") if config.exists() else ""
    if marker in existing:
        print(f"[codex] MCP server already in {config}")
    else:
        entry = (
            f"\n[mcp_servers.{SERVER_NAME}]\n"
            f'command = "{python_exe}"\n'
            f'args = ["{MCP_ENTRY}"]\n'
        )
        with config.open("a", encoding="utf-8") as fh:
            fh.write(entry)
        print(f"[codex] MCP server appended to {config}")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Install DataFlow-Evolve plugin",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--target", choices=["claude", "codex", "both"], default="both")
    parser.add_argument("--python", default=sys.executable,
                        help="Python executable to use for the MCP server process")
    parser.add_argument("--claude-config-dir", "--claude-home", dest="claude_config_dir",
                        help="CLAUDE_CONFIG_DIR of the target Claude Code install "
                             "(the value its launcher exports, e.g. .../xqf/.claude). "
                             "Defaults to ~/.claude.")
    parser.add_argument("--claude-bin", default="claude",
                        help="claude executable to use for registration.")
    parser.add_argument("--codex-home",
                        help="CODEX_HOME of the target Codex install. Defaults to ~/.codex.")
    parser.add_argument("--print-config", action="store_true",
                        help="Copy skills, then print the registration command/snippet "
                             "instead of registering.")
    args = parser.parse_args()

    claude_config_dir = Path(args.claude_config_dir).resolve() if args.claude_config_dir else None
    codex_home = Path(args.codex_home).resolve() if args.codex_home else None

    if args.target in ("claude", "both"):
        install_claude(args.python, config_dir=claude_config_dir,
                       claude_bin=args.claude_bin, print_config=args.print_config)
    if args.target in ("codex", "both"):
        install_codex(args.python, codex_home=codex_home,
                      print_config=args.print_config)

    if not args.print_config:
        print("\nInstallation complete.")
        print("Verify with /mcp in your Claude Code or Codex session.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
