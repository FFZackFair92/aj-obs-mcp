#!/usr/bin/env python3
"""Finto eseguibile di OBS per provare open_obs: legge la config di obs-websocket come farebbe OBS e apre il finto server."""
import asyncio, json, os, sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
import fake_obs

cfg = json.loads((Path(os.environ["HOME"]) / ".config/obs-studio/plugin_config/obs-websocket/config.json").read_text())
if not cfg.get("server_enabled"):
    sys.exit(0)

async def main():
    _, server = await fake_obs.serve(cfg["server_port"], cfg.get("server_password") if cfg.get("auth_required") else "")
    async with server:
        await asyncio.sleep(20)
asyncio.run(main())
