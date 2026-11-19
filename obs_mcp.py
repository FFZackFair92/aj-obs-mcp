# /// script
# requires-python = ">=3.10"
# dependencies = ["mcp[cli]>=1.10,<3"]
# ///
"""AJ OBS MCP — controls OBS Studio through its built-in obs-websocket v5 server.

No plugin to install: OBS 28+ ships obs-websocket. Enable it in
Tools > WebSocket Server Settings. The password is read automatically from
OBS's own config file when OBS_PASSWORD is not set.

Environment (all optional):
  OBS_URL        ws://127.0.0.1:4455
  OBS_PASSWORD   websocket password (otherwise read from OBS's config)
  AJ_OBS_EXE     path of obs64.exe / OBS.app binary, for open_obs
  AJ_OBS_LOOKS   file where saved looks are kept (~/.aj-mcp/obs-looks.json)
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import os
import platform
import struct
import subprocess
import sys
import time
import uuid
from collections import deque
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:  # mcp 1.x
    from mcp.server.fastmcp import FastMCP, Image
    from mcp.server.fastmcp.exceptions import ToolError
except Exception:  # mcp 2.x renamed FastMCP to MCPServer
    from mcp.server.mcpserver import MCPServer as FastMCP  # type: ignore
    from mcp.server.mcpserver import Image  # type: ignore
    from mcp.server.mcpserver.exceptions import ToolError  # type: ignore
from mcp.types import ToolAnnotations

VERSION = "1.2.0"
RO = ToolAnnotations(readOnlyHint=True)
DESTRUCTIVE = ToolAnnotations(destructiveHint=True)

# ---------------------------------------------------------------------------
# Minimal WebSocket (RFC 6455) — stdlib only, so the only dependency is mcp.
# ---------------------------------------------------------------------------


class WSClosed(Exception):
    pass


async def ws_write_frame(writer: asyncio.StreamWriter, opcode: int, payload: bytes, mask: bool) -> None:
    head = bytearray([0x80 | opcode])
    n = len(payload)
    mbit = 0x80 if mask else 0
    if n < 126:
        head.append(mbit | n)
    elif n < 65536:
        head.append(mbit | 126)
        head += struct.pack("!H", n)
    else:
        head.append(mbit | 127)
        head += struct.pack("!Q", n)
    if mask:
        key = os.urandom(4)
        head += key
        payload = bytes(b ^ key[i % 4] for i, b in enumerate(payload)) if n < 4096 else _xor_fast(payload, key)
    writer.write(bytes(head) + payload)
    await writer.drain()


def _xor_fast(data: bytes, key: bytes) -> bytes:
    k = int.from_bytes((key * (len(data) // 4 + 1))[: len(data)], "big")
    return (int.from_bytes(data, "big") ^ k).to_bytes(len(data), "big")


async def ws_read_frame(reader: asyncio.StreamReader) -> tuple[int, bool, bytes]:
    b1, b2 = await reader.readexactly(2)
    fin = bool(b1 & 0x80)
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    n = b2 & 0x7F
    if n == 126:
        (n,) = struct.unpack("!H", await reader.readexactly(2))
    elif n == 127:
        (n,) = struct.unpack("!Q", await reader.readexactly(8))
    key = await reader.readexactly(4) if masked else b""
    data = await reader.readexactly(n) if n else b""
    if masked:
        data = _xor_fast(data, key)
    return opcode, fin, data


class WSClient:
    def __init__(self) -> None:
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self._send_lock = asyncio.Lock()

    async def connect(self, url: str, timeout: float = 5.0) -> None:
        u = urlparse(url)
        if u.scheme not in ("ws", "wss"):
            raise ValueError(f"OBS_URL deve iniziare con ws:// (ricevuto {url})")
        host = u.hostname or "127.0.0.1"
        port = u.port or 4455
        ssl = u.scheme == "wss" or None
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_connection(host, port, ssl=ssl, limit=2**27), timeout
        )
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {u.path or '/'} HTTP/1.1\r\nHost: {host}:{port}\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n"
            "Sec-WebSocket-Protocol: obswebsocket.json\r\n\r\n"
        )
        self.writer.write(req.encode())
        await self.writer.drain()
        raw = await asyncio.wait_for(self.reader.readuntil(b"\r\n\r\n"), timeout)
        lines = raw.decode(errors="replace").split("\r\n")
        if " 101 " not in lines[0] + " ":
            raise ConnectionError(f"handshake WebSocket rifiutato: {lines[0]}")
        accept = base64.b64encode(
            hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
        ).decode()
        headers = {ln.split(":", 1)[0].strip().lower(): ln.split(":", 1)[1].strip() for ln in lines[1:] if ":" in ln}
        if headers.get("sec-websocket-accept") != accept:
            raise ConnectionError("handshake WebSocket non valido")

    async def send(self, text: str) -> None:
        if not self.writer:
            raise WSClosed("non connesso")
        async with self._send_lock:
            await ws_write_frame(self.writer, 1, text.encode(), mask=True)

    async def recv(self) -> str:
        assert self.reader
        buf = b""
        while True:
            try:
                op, fin, data = await ws_read_frame(self.reader)
            except (asyncio.IncompleteReadError, ConnectionError) as e:
                raise WSClosed(str(e)) from e
            if op == 8:
                code = struct.unpack("!H", data[:2])[0] if len(data) >= 2 else 1000
                raise WSClosed(f"{code} {data[2:].decode(errors='replace')}".strip())
            if op == 9:
                async with self._send_lock:
                    await ws_write_frame(self.writer, 10, data, mask=True)
                continue
            if op == 10:
                continue
            buf += data
            if fin:
                return buf.decode()

    async def close(self) -> None:
        if self.writer:
            try:
                await ws_write_frame(self.writer, 8, struct.pack("!H", 1000), mask=True)
                self.writer.close()
            except Exception:
                pass
        self.writer = None


# ---------------------------------------------------------------------------
# obs-websocket v5 client
# ---------------------------------------------------------------------------

EV_ALL = 0x7FF  # General..Ui (low-volume)
EV_VOLUME_METERS = 1 << 16
CLOSE_REASONS = {
    4009: "password sbagliata (OBS_PASSWORD)",
    4008: "OBS chiede una password ma non l'ho trovata: imposta OBS_PASSWORD",
    4010: "versione RPC non supportata",
}


class OBSError(ToolError):  # ToolError: the message reaches the model instead of a generic error
    pass


def obs_config_candidates() -> list[Path]:
    home = Path.home()
    out = []
    if os.environ.get("APPDATA"):
        out.append(Path(os.environ["APPDATA"]) / "obs-studio")
    out += [
        home / "AppData/Roaming/obs-studio",
        home / "Library/Application Support/obs-studio",
        home / ".config/obs-studio",
        home / ".var/app/com.obsproject.Studio/config/obs-studio",
    ]
    return out


def read_obs_ws_config() -> dict:
    for base in obs_config_candidates():
        f = base / "plugin_config/obs-websocket/config.json"
        try:
            return json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
    return {}


class OBS:
    def __init__(self) -> None:
        cfg = read_obs_ws_config()
        port = cfg.get("server_port", 4455)
        self.url = os.environ.get("OBS_URL", f"ws://127.0.0.1:{port}")
        pw = os.environ.get("OBS_PASSWORD")
        if pw is None and cfg.get("auth_required", True):
            pw = cfg.get("server_password")
        self.password = pw or ""
        self.ws: WSClient | None = None
        self.pending: dict[str, asyncio.Future] = {}
        self.events: deque = deque(maxlen=500)
        self.event_seq = 0
        self.new_event = asyncio.Event()
        self.meters: dict[str, list] = {}
        self.meter_users = 0
        self.info: dict = {}
        self._lock = asyncio.Lock()
        self._reader_task: asyncio.Task | None = None
        self._identified: asyncio.Future | None = None
        self.subs = EV_ALL

    @property
    def is_local(self) -> bool:
        return (urlparse(self.url).hostname or "") in ("127.0.0.1", "localhost", "::1")

    async def ensure(self) -> None:
        if self.ws and self._reader_task and not self._reader_task.done():
            return
        async with self._lock:
            if self.ws and self._reader_task and not self._reader_task.done():
                return
            ws = WSClient()
            try:
                await ws.connect(self.url)
            except Exception as e:
                raise OBSError(
                    f"OBS non risponde su {self.url} ({e.__class__.__name__}: {e}). Apri OBS e attiva "
                    "Strumenti > Impostazioni server WebSocket > Abilita server WebSocket (oppure usa open_obs)."
                ) from None
            hello = json.loads(await asyncio.wait_for(ws.recv(), 5))
            d = hello.get("d", {})
            ident: dict[str, Any] = {"rpcVersion": 1, "eventSubscriptions": self.subs}
            if "authentication" in d:
                if not self.password:
                    await ws.close()
                    raise OBSError(CLOSE_REASONS[4008])
                a = d["authentication"]
                secret = base64.b64encode(hashlib.sha256((self.password + a["salt"]).encode()).digest()).decode()
                ident["authentication"] = base64.b64encode(
                    hashlib.sha256((secret + a["challenge"]).encode()).digest()
                ).decode()
            await ws.send(json.dumps({"op": 1, "d": ident}))
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), 5))
            except WSClosed as e:
                code = int(str(e).split()[0]) if str(e)[:4].isdigit() else 0
                raise OBSError(f"OBS ha rifiutato la connessione: {CLOSE_REASONS.get(code, e)}") from None
            if msg.get("op") != 2:
                raise OBSError(f"risposta inattesa da OBS: {msg}")
            self.info = {"obsWebSocketVersion": d.get("obsWebSocketVersion"), "rpcVersion": msg["d"].get("negotiatedRpcVersion")}
            self.ws = ws
            self._reader_task = asyncio.create_task(self._read_loop(ws))

    async def _read_loop(self, ws: WSClient) -> None:
        try:
            while True:
                msg = json.loads(await ws.recv())
                op, d = msg.get("op"), msg.get("d", {})
                if op in (7, 9):
                    fut = self.pending.pop(d.get("requestId"), None)
                    if fut and not fut.done():
                        fut.set_result(d)
                elif op == 5:
                    et = d.get("eventType")
                    if et == "InputVolumeMeters":
                        for inp in d.get("eventData", {}).get("inputs", []):
                            self.meters.setdefault(inp.get("inputName"), []).append(inp.get("inputLevelsMul", []))
                        continue
                    self.event_seq += 1
                    self.events.append({"seq": self.event_seq, "time": time.strftime("%H:%M:%S"), "type": et, "data": d.get("eventData")})
                    self.new_event.set()
        except Exception as e:
            err = OBSError(f"connessione con OBS chiusa ({e})")
            for fut in self.pending.values():
                if not fut.done():
                    fut.set_exception(err)
            self.pending.clear()
            self.ws = None

    async def _send(self, op: int, d: dict, rid: str, timeout: float) -> dict:
        await self.ensure()
        fut = asyncio.get_running_loop().create_future()
        self.pending[rid] = fut
        await self.ws.send(json.dumps({"op": op, "d": d}))
        try:
            return await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            self.pending.pop(rid, None)
            raise OBSError(f"OBS non ha risposto entro {timeout:.0f}s (una finestra di dialogo aperta in OBS può bloccarlo)") from None

    async def call(self, rtype: str, data: dict | None = None, timeout: float = 15, keep_none: bool = False) -> dict:
        rid = uuid.uuid4().hex
        d: dict[str, Any] = {"requestType": rtype, "requestId": rid}
        if data:
            d["requestData"] = dict(data) if keep_none else {k: v for k, v in data.items() if v is not None}
        r = await self._send(6, d, rid, timeout)
        st = r.get("requestStatus", {})
        if not st.get("result"):
            raise OBSError(f"{rtype}: {st.get('comment') or 'errore'} (codice {st.get('code')})")
        return r.get("responseData") or {}

    async def try_call(self, rtype: str, data: dict | None = None) -> dict | None:
        try:
            return await self.call(rtype, data)
        except OBSError:
            return None

    async def batch(self, requests: list[dict], halt_on_failure: bool = False, execution_type: int = 0, timeout: float = 60) -> list[dict]:
        rid = uuid.uuid4().hex
        reqs = []
        for i, r in enumerate(requests):
            item = {"requestType": r["requestType"], "requestId": str(i)}
            if r.get("requestData"):
                item["requestData"] = r["requestData"]
            reqs.append(item)
        d = {"requestId": rid, "haltOnFailure": halt_on_failure, "executionType": execution_type, "requests": reqs}
        r = await self._send(8, d, rid, timeout)
        return r.get("results", [])

    async def set_subs(self, subs: int) -> None:
        await self.ensure()
        if subs == self.subs:
            return
        self.subs = subs
        await self.ws.send(json.dumps({"op": 3, "d": {"eventSubscriptions": subs}}))

    async def meters_start(self) -> None:
        self.meter_users += 1
        if self.meter_users == 1:
            self.meters.clear()
            await self.set_subs(self.subs | EV_VOLUME_METERS)

    async def meters_stop(self) -> None:
        self.meter_users = max(0, self.meter_users - 1)
        if self.meter_users == 0:
            await self.set_subs(self.subs & ~EV_VOLUME_METERS)


obs = OBS()
mcp = FastMCP(
    "aj-obs",
    instructions=(
        "Controls OBS Studio (scenes, sources, audio, filters, stream, recording, replay, virtual camera, "
        "screenshots) over obs-websocket v5. Start with obs_status. Anything without a dedicated tool can be "
        "done with obs_request (any obs-websocket request) or obs_batch. Actions that interrupt a live show "
        "or delete things need confirm=true: ask the user first. Use screenshot to check your work visually."
    ),
)

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

SECRET_KEYS = {"key", "password", "bearer_token", "server_password", "stream_key"}


def mask_secrets(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: ("•••• (nascosta)" if k in SECRET_KEYS and isinstance(v, str) and v else mask_secrets(v)) for k, v in obj.items()}
    if isinstance(obj, list):
        return [mask_secrets(v) for v in obj]
    return obj


def need_confirm(what: str) -> dict:
    return {"done": False, "needs_confirm": True, "message": f"Non eseguito: {what}. Chiedi all'utente e richiama con confirm=true."}


def mul_to_db(m: float) -> float:
    return round(20 * math.log10(m), 1) if m and m > 0 else -100.0


def hex_to_abgr(v: Any) -> Any:
    if isinstance(v, str) and v.startswith("#") and len(v) in (7, 9):
        r, g, b = int(v[1:3], 16), int(v[3:5], 16), int(v[5:7], 16)
        a = int(v[7:9], 16) if len(v) == 9 else 255
        return (a << 24) | (b << 16) | (g << 8) | r
    return v


async def current_scene() -> str:
    return (await obs.call("GetCurrentProgramScene"))["currentProgramSceneName"]


async def scene_or_current(scene: str | None) -> str:
    return scene or await current_scene()


async def item_id(scene: str, source: str | int) -> int:
    if isinstance(source, int) or (isinstance(source, str) and source.isdigit()):
        return int(source)
    try:
        return (await obs.call("GetSceneItemId", {"sceneName": scene, "sourceName": source}))["sceneItemId"]
    except OBSError:
        items = (await obs.call("GetSceneItemList", {"sceneName": scene}))["sceneItems"]
        names = ", ".join(i["sourceName"] for i in items) or "(scena vuota)"
        raise OBSError(f"'{source}' non è nella scena '{scene}'. Elementi presenti: {names}") from None


async def canvas() -> tuple[int, int]:
    v = await obs.call("GetVideoSettings")
    return v["baseWidth"], v["baseHeight"]


_kinds_cache: list[str] | None = None


async def input_kinds() -> list[str]:
    global _kinds_cache
    if _kinds_cache is None:
        _kinds_cache = (await obs.call("GetInputKindList", {"unversioned": False}))["inputKinds"]
    return _kinds_cache


INPUT_ALIASES: dict[str, list[str]] = {
    "text": ["text_gdiplus_v3", "text_gdiplus_v2", "text_gdiplus", "text_ft2_source_v2", "text_ft2_source"],
    "image": ["image_source"],
    "media": ["ffmpeg_source"],
    "video": ["ffmpeg_source"],
    "browser": ["browser_source"],
    "color": ["color_source_v3", "color_source_v2", "color_source"],
    "display": ["monitor_capture", "screen_capture", "display_capture", "xshm_input_v2", "xshm_input", "pipewire-desktop-capture-source"],
    "window": ["window_capture", "screen_capture", "xcomposite_input", "pipewire-window-capture-source"],
    "game": ["game_capture"],
    "camera": ["dshow_input", "macos-avcapture", "av_capture_input_v2", "av_capture_input", "v4l2_input"],
    "mic": ["wasapi_input_capture", "coreaudio_input_capture", "pulse_input_capture", "alsa_input_capture"],
    "desktop_audio": ["wasapi_output_capture", "coreaudio_output_capture", "pulse_output_capture"],
    "app_audio": ["wasapi_process_output_capture"],
    "spout": ["spout_capture"],
    "syphon": ["syphon-input"],
    "ndi": ["ndi_source"],
    "slideshow": ["slideshow_v2", "slideshow"],
    "vlc": ["vlc_source"],
}
INPUT_ALIASES.update({"testo": INPUT_ALIASES["text"], "immagine": INPUT_ALIASES["image"], "webcam": INPUT_ALIASES["camera"],
                      "colore": INPUT_ALIASES["color"], "schermo": INPUT_ALIASES["display"], "finestra": INPUT_ALIASES["window"],
                      "microfono": INPUT_ALIASES["mic"], "web": INPUT_ALIASES["browser"]})

FILTER_ALIASES: dict[str, list[str]] = {
    "chroma_key": ["chroma_key_filter_v2", "chroma_key_filter"],
    "color_key": ["color_key_filter_v2", "color_key_filter"],
    "luma_key": ["luma_key_filter_v2", "luma_key_filter"],
    "color_correction": ["color_filter_v2", "color_filter"],
    "sharpen": ["sharpness_filter_v2", "sharpness_filter"],
    "mask": ["mask_filter_v2", "mask_filter"],
    "scroll": ["scroll_filter"],
    "crop": ["crop_filter"],
    "scale": ["scale_filter"],
    "video_delay": ["gpu_delay"],
    "lut": ["clut_filter"],
    "noise_suppress": ["noise_suppress_filter_v2", "noise_suppress_filter"],
    "noise_gate": ["noise_gate_filter"],
    "compressor": ["compressor_filter"],
    "limiter": ["limiter_filter"],
    "expander": ["expander_filter"],
    "upward_compressor": ["upward_compressor_filter"],
    "gain": ["gain_filter"],
    "eq3": ["basic_eq_filter"],
    "audio_delay": ["async_delay_filter"],
    "invert_polarity": ["invert_polarity_filter"],
    "vst": ["vst_filter"],
}


async def resolve_input_kind(kind: str) -> str:
    kinds = await input_kinds()
    if kind in kinds:
        return kind
    cands = INPUT_ALIASES.get(kind.lower())
    if not cands:
        raise OBSError(f"tipo di sorgente '{kind}' sconosciuto. Alias: {', '.join(sorted(INPUT_ALIASES))}; o un tipo reale da list_input_kinds")
    for c in cands:
        if c in kinds:
            return c
    raise OBSError(f"nessun tipo per '{kind}' disponibile in questo OBS (cercati {cands}); plugin mancante?")


async def resolve_filter_kind(kind: str) -> str:
    avail = (await obs.try_call("GetSourceFilterKindList") or {}).get("sourceFilterKinds")
    if avail and kind in avail:
        return kind
    cands = FILTER_ALIASES.get(kind.lower())
    if not cands:
        if avail is None:
            return kind
        raise OBSError(f"filtro '{kind}' sconosciuto. Alias: {', '.join(sorted(FILTER_ALIASES))}")
    if avail is None:
        return cands[0]
    for c in cands:
        if c in avail:
            return c
    raise OBSError(f"filtro '{kind}' non disponibile in questo OBS (cercati {cands})")


def norm_settings(kind: str, settings: dict | None) -> dict:
    s = dict(settings or {})
    if kind == "ffmpeg_source" and "file" in s:
        s["local_file"] = s.pop("file")
        s.setdefault("is_local_file", True)
    if kind == "ffmpeg_source" and "url" in s:
        s["input"] = s.pop("url")
        s["is_local_file"] = False
    if kind.startswith("text_ft2") and "color" in s:
        c = hex_to_abgr(s.pop("color"))
        s["color1"] = s["color2"] = c
    for k, v in list(s.items()):
        if "color" in k.lower():
            s[k] = hex_to_abgr(v)
    return s


PRESETS = {
    "full": (0, 0, 1, 1), "fill": (0, 0, 1, 1), "fit": (0, 0, 1, 1), "center": (0.25, 0.25, 0.5, 0.5),
    "left_half": (0, 0, 0.5, 1), "right_half": (0.5, 0, 0.5, 1), "top_half": (0, 0, 1, 0.5), "bottom_half": (0, 0.5, 1, 0.5),
    "top_left": (0, 0, 0.5, 0.5), "top_right": (0.5, 0, 0.5, 0.5), "bottom_left": (0, 0.5, 0.5, 0.5), "bottom_right": (0.5, 0.5, 0.5, 0.5),
    "pip_top_left": (0.02, 0.03, 0.28, 0.28), "pip_top_right": (0.70, 0.03, 0.28, 0.28),
    "pip_bottom_left": (0.02, 0.69, 0.28, 0.28), "pip_bottom_right": (0.70, 0.69, 0.28, 0.28),
    "lower_third": (0.05, 0.75, 0.6, 0.15), "banner_top": (0, 0, 1, 0.12), "banner_bottom": (0, 0.88, 1, 0.12),
    "left_third": (0, 0, 1 / 3, 1), "middle_third": (1 / 3, 0, 1 / 3, 1), "right_third": (2 / 3, 0, 1 / 3, 1),
}


async def layout_transform(preset: str | None, box: list[float] | None, mode: str) -> dict:
    W, H = await canvas()
    if box:
        if len(box) != 4:
            raise OBSError("box = [x, y, larghezza, altezza]")
        x, y, w, h = box
        if all(0 <= v <= 1 for v in box):  # fractions of the canvas
            x, y, w, h = x * W, y * H, w * W, h * H
    else:
        if preset not in PRESETS:
            raise OBSError(f"preset sconosciuto '{preset}'. Disponibili: {', '.join(PRESETS)}")
        fx, fy, fw, fh = PRESETS[preset]
        x, y, w, h = fx * W, fy * H, fw * W, fh * H
        if preset == "fill":
            mode = "stretch"
    bt = {"fit": "OBS_BOUNDS_SCALE_INNER", "cover": "OBS_BOUNDS_SCALE_OUTER", "stretch": "OBS_BOUNDS_STRETCH"}.get(mode)
    if not bt:
        raise OBSError("mode: fit | cover | stretch")
    return {"positionX": float(x), "positionY": float(y), "alignment": 5, "rotation": 0.0,
            "boundsType": bt, "boundsAlignment": 0, "boundsWidth": max(1.0, float(w)), "boundsHeight": max(1.0, float(h))}


# ---------------------------------------------------------------------------
# General
# ---------------------------------------------------------------------------


@mcp.tool(annotations=RO)
async def obs_status() -> dict:
    """Overview of OBS: version, current program/preview scene, studio mode, stream/record/replay/virtual-cam
    state, profile, scene collection, canvas, fps and performance. Call this first."""
    res = await obs.batch([{"requestType": t} for t in (
        "GetVersion", "GetSceneList", "GetStudioModeEnabled", "GetStreamStatus", "GetRecordStatus",
        "GetReplayBufferStatus", "GetVirtualCamStatus", "GetProfileList", "GetSceneCollectionList",
        "GetVideoSettings", "GetStats", "GetCurrentSceneTransition")])
    r = {x["requestType"]: (x.get("responseData") or {}) if x.get("requestStatus", {}).get("result") else None for x in res}
    v, sl, st, rec, vs = r["GetVersion"] or {}, r["GetSceneList"] or {}, r["GetStats"] or {}, r["GetRecordStatus"] or {}, r["GetVideoSettings"] or {}
    stream = r["GetStreamStatus"] or {}
    return {
        "obs": v.get("obsVersion"), "obs_websocket": v.get("obsWebSocketVersion"), "platform": v.get("platformDescription"),
        "url": obs.url,
        "program_scene": sl.get("currentProgramSceneName"), "preview_scene": sl.get("currentPreviewSceneName"),
        "studio_mode": (r["GetStudioModeEnabled"] or {}).get("studioModeEnabled"),
        "scenes": len(sl.get("scenes", [])),
        "transition": (r["GetCurrentSceneTransition"] or {}).get("transitionName"),
        "streaming": {"active": stream.get("outputActive"), "timecode": stream.get("outputTimecode"),
                      "reconnecting": stream.get("outputReconnecting"), "congestion": stream.get("outputCongestion"),
                      "skipped_frames": stream.get("outputSkippedFrames")},
        "recording": {"active": rec.get("outputActive"), "paused": rec.get("outputPaused"), "timecode": rec.get("outputTimecode")},
        "replay_buffer": (r["GetReplayBufferStatus"] or {}).get("outputActive"),
        "virtual_cam": (r["GetVirtualCamStatus"] or {}).get("outputActive"),
        "profile": (r["GetProfileList"] or {}).get("currentProfileName"),
        "scene_collection": (r["GetSceneCollectionList"] or {}).get("currentSceneCollectionName"),
        "canvas": f"{vs.get('baseWidth')}x{vs.get('baseHeight')}", "output": f"{vs.get('outputWidth')}x{vs.get('outputHeight')}",
        "fps_target": round(vs["fpsNumerator"] / vs["fpsDenominator"], 2) if vs.get("fpsDenominator") else None,
        "fps_now": round(st.get("activeFps", 0), 2), "cpu_percent": round(st.get("cpuUsage", 0), 1),
        "render_ms": round(st.get("averageFrameRenderTime", 0), 2),
        "disk_free_gb": round(st.get("availableDiskSpace", 0) / 1024, 1),
        "render_skipped": st.get("renderSkippedFrames"), "encoder_skipped": st.get("outputSkippedFrames"),
    }


DANGEROUS_REQUESTS = {
    "StopStream", "ToggleStream", "StopRecord", "ToggleRecord", "RemoveScene", "RemoveInput", "RemoveSceneItem",
    "RemoveSourceFilter", "SetStreamServiceSettings", "SetCurrentSceneCollection", "SetCurrentProfile", "RemoveProfile",
    "SetVideoSettings", "StopOutput", "ToggleOutput", "SetProfileParameter", "CreateSceneCollection", "CreateProfile",
}


@mcp.tool()
async def obs_request(request_type: str, request_data: dict | None = None, confirm: bool = False) -> dict:
    """Send ANY obs-websocket v5 request (e.g. 'GetInputPropertiesListPropertyItems', 'SetSceneSceneTransitionOverride',
    'GetSourceFilterDefaultSettings', 'CallVendorRequest', 'GetPersistentData'). Use when no dedicated tool exists.
    Requests that stop the show or delete things need confirm=true. Stream keys/passwords are hidden in replies."""
    if request_type in DANGEROUS_REQUESTS and not confirm:
        return need_confirm(f"{request_type} può interrompere la diretta o cancellare qualcosa")
    return mask_secrets(await obs.call(request_type, request_data, timeout=30))


@mcp.tool()
async def obs_batch(requests: list[dict], halt_on_failure: bool = False, execution_type: str = "serial_realtime", confirm: bool = False) -> dict:
    """Run many obs-websocket requests in one round trip. requests = [{"requestType": "...", "requestData": {...}}, ...].
    Include {"requestType": "Sleep", "requestData": {"sleepMillis": 500}} for timed sequences (serial_realtime) or
    {"sleepFrames": 30} with execution_type='serial_frame' for frame-accurate cues. execution_type: serial_realtime |
    serial_frame | parallel. Dangerous requests inside need confirm=true."""
    bad = [r.get("requestType") for r in requests if r.get("requestType") in DANGEROUS_REQUESTS]
    if bad and not confirm:
        return need_confirm(f"il lotto contiene {sorted(set(bad))}")
    et = {"serial_realtime": 0, "serial_frame": 1, "parallel": 2}.get(execution_type)
    if et is None:
        raise OBSError("execution_type: serial_realtime | serial_frame | parallel")
    res = await obs.batch(requests, halt_on_failure, et, timeout=120)
    out = []
    for x in res:
        s = x.get("requestStatus", {})
        out.append({"request": x.get("requestType"), "ok": s.get("result"), **({"error": s.get("comment"), "code": s.get("code")} if not s.get("result") else {}),
                    **({"data": mask_secrets(x["responseData"])} if x.get("responseData") else {})})
    return {"ok": all(o["ok"] for o in out), "count": len(out), "results": out}


@mcp.tool(annotations=RO)
async def obs_events(wait_seconds: float = 0, types: list[str] | None = None, since: int = 0, limit: int = 50) -> dict:
    """What happened in OBS (scene switches, stream started/stopped, sources created, mute changes, media ended, replay
    saved, ...). Returns buffered events with seq > since. With wait_seconds > 0 waits until a matching event arrives
    (e.g. types=['MediaInputPlaybackEnded'] or ['ReplayBufferSaved']). Pass the returned last_seq as since next time."""
    await obs.ensure()
    deadline = time.monotonic() + max(0.0, min(wait_seconds, 300))

    def pick():
        return [e for e in obs.events if e["seq"] > since and (not types or e["type"] in types)]

    found = pick()
    while not found and time.monotonic() < deadline:
        obs.new_event.clear()
        try:
            await asyncio.wait_for(obs.new_event.wait(), deadline - time.monotonic())
        except asyncio.TimeoutError:
            break
        found = pick()
    return {"events": mask_secrets(found[-limit:]), "last_seq": obs.event_seq, "timed_out": bool(wait_seconds and not found)}


@mcp.tool()
async def hotkey(name: str | None = None, key: str | None = None, shift: bool = False, ctrl: bool = False, alt: bool = False, cmd: bool = False) -> dict:
    """Trigger an OBS hotkey by internal name (e.g. 'OBSBasic.Screenshot', 'ReplayBuffer.Save') or by key
    (key='OBS_KEY_F5' + modifiers). Without arguments returns the list of hotkey names."""
    if name:
        await obs.call("TriggerHotkeyByName", {"hotkeyName": name})
        return {"done": True, "hotkey": name}
    if key:
        k = key if key.startswith("OBS_KEY_") else "OBS_KEY_" + key.upper()
        await obs.call("TriggerHotkeyByKeySequence", {"keyId": k, "keyModifiers": {"shift": shift, "control": ctrl, "alt": alt, "command": cmd}})
        return {"done": True, "key": k}
    return {"hotkeys": (await obs.call("GetHotkeyList"))["hotkeys"]}


def default_obs_exe() -> str | None:
    """Find OBS: AJ_OBS_EXE, the Windows registry, the usual install folders, then PATH."""
    if os.environ.get("AJ_OBS_EXE"):
        return os.environ["AJ_OBS_EXE"]
    cands: list[Path] = []
    if sys.platform == "win32":
        try:
            import winreg
            for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
                for key in (r"SOFTWARE\OBS Studio", r"SOFTWARE\WOW6432Node\OBS Studio"):
                    try:
                        with winreg.OpenKey(hive, key) as k:
                            cands.append(Path(winreg.QueryValueEx(k, "")[0]) / "bin/64bit/obs64.exe")
                    except OSError:
                        pass
        except ImportError:
            pass
        for env in ("ProgramFiles", "ProgramFiles(x86)", "ProgramW6432"):
            if os.environ.get(env):
                cands.append(Path(os.environ[env]) / "obs-studio/bin/64bit/obs64.exe")
        steam = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / "Steam/steamapps/common/OBS Studio/bin/64bit/obs64.exe"
        cands.append(steam)
    cands += [Path("/Applications/OBS.app/Contents/MacOS/OBS"), Path.home() / "Applications/OBS.app/Contents/MacOS/OBS",
              Path("/usr/bin/obs"), Path("/usr/local/bin/obs")]
    for c in cands:
        if c.exists():
            return str(c)
    import shutil
    return shutil.which("obs64") or shutil.which("obs")


def obs_process_running() -> bool:
    try:
        if sys.platform == "win32":
            out = subprocess.run(["tasklist", "/FI", "IMAGENAME eq obs64.exe", "/NH"], capture_output=True, text=True, timeout=10).stdout
            return "obs64.exe" in out.lower()
        return subprocess.run(["pgrep", "-x", "obs" if sys.platform != "darwin" else "OBS"], capture_output=True, timeout=10).returncode == 0
    except Exception:
        return False


def enable_ws_in_config() -> dict:
    """Turn on obs-websocket in OBS's own config (only while OBS is closed: OBS reads it at start-up).
    Keeps the existing port and password; creates a password if auth is on and none exists."""
    bases = [b for b in obs_config_candidates() if b.exists()] or obs_config_candidates()[:1]
    f = bases[0] / "plugin_config/obs-websocket/config.json"
    try:
        cfg = json.loads(f.read_text(encoding="utf-8"))
    except Exception:
        cfg = {}
    changed = []
    if not cfg.get("server_enabled"):
        cfg["server_enabled"] = True
        changed.append("server WebSocket attivato")
    cfg.setdefault("server_port", 4455)
    cfg.setdefault("auth_required", True)
    if cfg["auth_required"] and not cfg.get("server_password"):
        cfg["server_password"] = base64.urlsafe_b64encode(os.urandom(12)).decode().rstrip("=")
        changed.append("password creata")
    if changed:
        cfg["first_load"] = False
        cfg.setdefault("alerts_enabled", False)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(cfg, indent=4), encoding="utf-8")
    return {"file": str(f), "changed": changed, "port": cfg["server_port"], "password": cfg.get("server_password") if cfg["auth_required"] else ""}


@mcp.tool()
async def open_obs(scene_collection: str | None = None, profile: str | None = None, scene: str | None = None,
                   minimize_to_tray: bool = False, start_virtualcam: bool = False, start_replay_buffer: bool = False,
                   wait_seconds: float = 45) -> dict:
    """Open OBS by itself and connect: finds obs64.exe (registry, Program Files, Steam, PATH or AJ_OBS_EXE), switches
    on OBS's WebSocket server in its config if it is off, launches OBS (optionally with a scene collection, profile,
    scene, minimized to tray, virtual camera or replay buffer) and waits until it answers.
    Never starts streaming or recording by itself. Use it whenever another tool says OBS is not reachable."""
    try:
        await obs.ensure()
        return {"already_running": True, **obs.info}
    except OBSError:
        pass
    if obs_process_running():
        raise OBSError("OBS è già aperto ma il server WebSocket non risponde: in OBS vai su Strumenti > Impostazioni "
                       "server WebSocket > Abilita, oppure chiudi OBS e richiama open_obs (lo attivo io)")
    exe = default_obs_exe()
    if not exe:
        raise OBSError("OBS non trovato: installalo da obsproject.com o imposta AJ_OBS_EXE con il percorso di obs64.exe")
    ws = {"changed": []}
    if obs.is_local:
        ws = enable_ws_in_config()
        if "OBS_PASSWORD" not in os.environ:
            obs.password = ws["password"] or ""
        if "OBS_URL" not in os.environ:
            obs.url = f"ws://127.0.0.1:{ws['port']}"
    args = [exe, "--disable-shutdown-check"]
    if scene_collection:
        args += ["--collection", scene_collection]
    if profile:
        args += ["--profile", profile]
    if scene:
        args += ["--scene", scene]
    if minimize_to_tray:
        args.append("--minimize-to-tray")
    if start_virtualcam:
        args.append("--startvirtualcam")
    if start_replay_buffer:
        args.append("--startreplaybuffer")
    kw: dict[str, Any] = {"cwd": str(Path(exe).parent), "stdin": subprocess.DEVNULL, "stdout": subprocess.DEVNULL, "stderr": subprocess.DEVNULL}
    if sys.platform == "win32":
        kw["creationflags"] = 0x00000008 | 0x00000200  # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
    else:
        kw["start_new_session"] = True
    subprocess.Popen(args, **kw)
    deadline = time.monotonic() + wait_seconds
    last = ""
    while time.monotonic() < deadline:
        await asyncio.sleep(1.5)
        try:
            await obs.ensure()
            return {"started": True, "exe": exe, **({"websocket": ws["changed"]} if ws["changed"] else {}), **obs.info}
        except OBSError as e:
            last = str(e)
    return {"started": True, "connected": False, "exe": exe, "message": "OBS avviato ma il websocket non risponde ancora: " + last}


# ---------------------------------------------------------------------------
# Scenes
# ---------------------------------------------------------------------------


@mcp.tool(annotations=RO)
async def list_scenes() -> dict:
    """All scenes in display order (top first), with program/preview scene and available transitions."""
    sl = await obs.call("GetSceneList")
    tr = await obs.try_call("GetSceneTransitionList") or {}
    scenes = sorted(sl["scenes"], key=lambda s: -s["sceneIndex"])
    return {"program": sl.get("currentProgramSceneName"), "preview": sl.get("currentPreviewSceneName"),
            "scenes": [s["sceneName"] for s in scenes],
            "transition": tr.get("currentSceneTransitionName"), "transition_ms": tr.get("currentSceneTransitionDuration"),
            "transitions": [t["transitionName"] for t in tr.get("transitions", [])]}


@mcp.tool()
async def set_scene(scene: str, preview: bool = False, transition: str | None = None, duration_ms: int | None = None) -> dict:
    """Switch the program scene (or the preview scene in studio mode with preview=true). Optionally choose the
    transition and its duration first (this also becomes the default transition)."""
    if transition:
        await obs.call("SetCurrentSceneTransition", {"transitionName": transition})
    if duration_ms is not None:
        await obs.call("SetCurrentSceneTransitionDuration", {"transitionDuration": max(50, min(20000, duration_ms))})
    await obs.call("SetCurrentPreviewScene" if preview else "SetCurrentProgramScene", {"sceneName": scene})
    return {"done": True, ("preview" if preview else "program"): scene}


@mcp.tool()
async def create_scene(name: str, switch_to: bool = False) -> dict:
    """Create an empty scene. Use build_scene to create a scene already filled with sources."""
    await obs.call("CreateScene", {"sceneName": name})
    if switch_to:
        await obs.call("SetCurrentProgramScene", {"sceneName": name})
    return {"done": True, "scene": name}


@mcp.tool(annotations=DESTRUCTIVE)
async def remove_scene(name: str, confirm: bool = False) -> dict:
    """Delete a scene (its sources stay in other scenes). Needs confirm=true."""
    if not confirm:
        return need_confirm(f"cancellare la scena '{name}'")
    await obs.call("RemoveScene", {"sceneName": name})
    return {"done": True, "removed": name}


@mcp.tool()
async def rename_scene(name: str, new_name: str) -> dict:
    """Rename a scene."""
    await obs.call("SetSceneName", {"sceneName": name, "newSceneName": new_name})
    return {"done": True, "scene": new_name}


def _item_view(i: dict) -> dict:
    t = i.get("sceneItemTransform", {})
    return {"id": i["sceneItemId"], "source": i["sourceName"], "kind": i.get("inputKind") or ("group" if i.get("isGroup") else i.get("sourceType")),
            "visible": i.get("sceneItemEnabled"), "locked": i.get("sceneItemLocked"), "index": i.get("sceneItemIndex"),
            "blend": (i.get("sceneItemBlendMode") or "OBS_BLEND_NORMAL").replace("OBS_BLEND_", "").lower(),
            "x": round(t.get("positionX", 0), 1), "y": round(t.get("positionY", 0), 1),
            "w": round(t.get("width", 0), 1), "h": round(t.get("height", 0), 1),
            "scale": [round(t.get("scaleX", 1), 3), round(t.get("scaleY", 1), 3)], "rotation": t.get("rotation", 0),
            **({"bounds": [t.get("boundsType", "").replace("OBS_BOUNDS_", "").lower(), t.get("boundsWidth"), t.get("boundsHeight")]} if t.get("boundsType") not in (None, "OBS_BOUNDS_NONE") else {}),
            **({"crop": [t.get("cropLeft"), t.get("cropTop"), t.get("cropRight"), t.get("cropBottom")]} if any(t.get(k) for k in ("cropLeft", "cropTop", "cropRight", "cropBottom")) else {})}


@mcp.tool(annotations=RO)
async def get_scene_items(scene: str | None = None, include_groups: bool = True) -> dict:
    """Sources in a scene (default: program scene), top layer first, with id, kind, visibility, lock, position,
    size, scale, bounds and crop. Group contents are listed under 'children'."""
    scene = await scene_or_current(scene)
    items = (await obs.call("GetSceneItemList", {"sceneName": scene}))["sceneItems"]
    out = []
    for i in sorted(items, key=lambda x: -x["sceneItemIndex"]):
        v = _item_view(i)
        if i.get("isGroup") and include_groups:
            g = await obs.try_call("GetGroupSceneItemList", {"sceneName": i["sourceName"]}) or {}
            v["children"] = [_item_view(c) for c in sorted(g.get("sceneItems", []), key=lambda x: -x["sceneItemIndex"])]
        out.append(v)
    W, H = await canvas()
    return {"scene": scene, "canvas": [W, H], "items": out}


# ---------------------------------------------------------------------------
# Scene items
# ---------------------------------------------------------------------------


@mcp.tool()
async def add_to_scene(source: str, scene: str | None = None, visible: bool = True, layout: str | None = None) -> dict:
    """Add an EXISTING source (or another scene, nested) to a scene. layout = optional preset (see set_item_layout)."""
    scene = await scene_or_current(scene)
    sid = (await obs.call("CreateSceneItem", {"sceneName": scene, "sourceName": source, "sceneItemEnabled": visible}))["sceneItemId"]
    if layout:
        await obs.call("SetSceneItemTransform", {"sceneName": scene, "sceneItemId": sid, "sceneItemTransform": await layout_transform(layout, None, "fit")})
    return {"done": True, "scene": scene, "id": sid}


@mcp.tool()
async def set_item_layout(source: str, scene: str | None = None, preset: str | None = None, box: list[float] | None = None, mode: str = "fit") -> dict:
    """Place a source with a preset or a box, without pixel maths. Presets: full, fill (stretch), center, left_half,
    right_half, top_half, bottom_half, top_left, top_right, bottom_left, bottom_right, pip_top_left, pip_top_right,
    pip_bottom_left, pip_bottom_right, lower_third, banner_top, banner_bottom, left_third, middle_third, right_third.
    box = [x, y, w, h] in pixels, or in fractions of the canvas if all values are 0..1.
    mode: fit (keep aspect, inside), cover (keep aspect, fill and overflow), stretch."""
    scene = await scene_or_current(scene)
    sid = await item_id(scene, source)
    if not preset and not box:
        raise OBSError("serve preset o box")
    tr = await layout_transform(preset, box, mode)
    await obs.call("SetSceneItemTransform", {"sceneName": scene, "sceneItemId": sid, "sceneItemTransform": tr})
    return {"done": True, "scene": scene, "source": source, "box": [round(tr["positionX"]), round(tr["positionY"]), round(tr["boundsWidth"]), round(tr["boundsHeight"])]}


@mcp.tool()
async def set_item_transform(source: str, scene: str | None = None, x: float | None = None, y: float | None = None,
                             scale: float | None = None, scale_x: float | None = None, scale_y: float | None = None,
                             rotation: float | None = None, width: float | None = None, height: float | None = None,
                             crop: list[int] | None = None, alignment: str | None = None, clear_bounds: bool = False) -> dict:
    """Fine control of a source's transform in pixels. width/height resize keeping the other axis proportional if only
    one is given. crop = [left, top, right, bottom]. alignment of the position point: top_left | top | top_right | left |
    center | right | bottom_left | bottom | bottom_right. clear_bounds removes a bounding box set by set_item_layout."""
    scene = await scene_or_current(scene)
    sid = await item_id(scene, source)
    cur = (await obs.call("GetSceneItemTransform", {"sceneName": scene, "sceneItemId": sid}))["sceneItemTransform"]
    t: dict[str, Any] = {}
    if clear_bounds:
        t["boundsType"] = "OBS_BOUNDS_NONE"
    if x is not None:
        t["positionX"] = float(x)
    if y is not None:
        t["positionY"] = float(y)
    if rotation is not None:
        t["rotation"] = float(rotation)
    if scale is not None:
        t["scaleX"] = t["scaleY"] = float(scale)
    if scale_x is not None:
        t["scaleX"] = float(scale_x)
    if scale_y is not None:
        t["scaleY"] = float(scale_y)
    sw, sh = cur.get("sourceWidth") or 0, cur.get("sourceHeight") or 0
    if (width or height) and sw and sh:
        if width:
            t["scaleX"] = width / sw
        if height:
            t["scaleY"] = height / sh
        if width and not height:
            t["scaleY"] = t["scaleX"]
        if height and not width:
            t["scaleX"] = t["scaleY"]
    if crop:
        t.update(dict(zip(("cropLeft", "cropTop", "cropRight", "cropBottom"), [int(c) for c in crop])))
    if alignment:
        a = {"center": 0, "left": 1, "right": 2, "top": 4, "bottom": 8, "top_left": 5, "top_right": 6, "bottom_left": 9, "bottom_right": 10}.get(alignment)
        if a is None:
            raise OBSError("alignment non valido")
        t["alignment"] = a
    if not t:
        return {"transform": cur}
    await obs.call("SetSceneItemTransform", {"sceneName": scene, "sceneItemId": sid, "sceneItemTransform": t})
    new = (await obs.call("GetSceneItemTransform", {"sceneName": scene, "sceneItemId": sid}))["sceneItemTransform"]
    return {"done": True, "x": new.get("positionX"), "y": new.get("positionY"), "w": new.get("width"), "h": new.get("height")}


BLENDS = {"normal", "additive", "subtract", "screen", "multiply", "lighten", "darken"}


@mcp.tool()
async def set_item_state(source: str, scene: str | None = None, visible: bool | None = None, toggle: bool = False,
                         locked: bool | None = None, index: int | str | None = None, blend: str | None = None) -> dict:
    """Show/hide (visible or toggle), lock, reorder and blend a source in a scene. index: number (0 = bottom),
    or 'top' | 'bottom' | 'up' | 'down'. blend: normal | additive | subtract | screen | multiply | lighten | darken."""
    scene = await scene_or_current(scene)
    sid = await item_id(scene, source)
    base = {"sceneName": scene, "sceneItemId": sid}
    if toggle:
        cur = (await obs.call("GetSceneItemEnabled", base))["sceneItemEnabled"]
        visible = not cur
    if visible is not None:
        await obs.call("SetSceneItemEnabled", {**base, "sceneItemEnabled": visible})
    if locked is not None:
        await obs.call("SetSceneItemLocked", {**base, "sceneItemLocked": locked})
    if index is not None:
        n = len((await obs.call("GetSceneItemList", {"sceneName": scene}))["sceneItems"])
        cur = (await obs.call("GetSceneItemIndex", base))["sceneItemIndex"]
        idx = {"top": n - 1, "bottom": 0, "up": min(n - 1, cur + 1), "down": max(0, cur - 1)}.get(str(index), None)
        idx = int(index) if idx is None else idx
        await obs.call("SetSceneItemIndex", {**base, "sceneItemIndex": idx})
    if blend:
        if blend.lower() not in BLENDS:
            raise OBSError(f"blend: {', '.join(sorted(BLENDS))}")
        await obs.call("SetSceneItemBlendMode", {**base, "sceneItemBlendMode": "OBS_BLEND_" + blend.upper()})
    return {"done": True, "scene": scene, "source": source, **({"visible": visible} if visible is not None else {})}


@mcp.tool()
async def duplicate_item(source: str, scene: str | None = None, to_scene: str | None = None) -> dict:
    """Duplicate a scene item (same source, new item) in the same scene or into another scene."""
    scene = await scene_or_current(scene)
    sid = await item_id(scene, source)
    r = await obs.call("DuplicateSceneItem", {"sceneName": scene, "sceneItemId": sid, "destinationSceneName": to_scene})
    return {"done": True, "scene": to_scene or scene, "id": r.get("sceneItemId")}


@mcp.tool(annotations=DESTRUCTIVE)
async def remove_item(source: str, scene: str | None = None, confirm: bool = False) -> dict:
    """Remove a source from ONE scene (the source itself survives if used elsewhere). Needs confirm=true."""
    scene = await scene_or_current(scene)
    if not confirm:
        return need_confirm(f"togliere '{source}' dalla scena '{scene}'")
    sid = await item_id(scene, source)
    await obs.call("RemoveSceneItem", {"sceneName": scene, "sceneItemId": sid})
    return {"done": True, "removed": source, "scene": scene}


# ---------------------------------------------------------------------------
# Inputs (sources)
# ---------------------------------------------------------------------------


@mcp.tool(annotations=RO)
async def list_inputs(kind: str | None = None) -> dict:
    """All inputs (sources) with their kind. kind filters by real kind or alias (text, image, media, camera, mic, ...)."""
    inputs = (await obs.call("GetInputList"))["inputs"]
    if kind:
        wanted = set(INPUT_ALIASES.get(kind.lower(), [kind]))
        inputs = [i for i in inputs if i["inputKind"] in wanted or i.get("unversionedInputKind") in wanted]
    special = await obs.try_call("GetSpecialInputs") or {}
    return {"inputs": [{"name": i["inputName"], "kind": i["inputKind"]} for i in inputs],
            "special_audio": {k: v for k, v in special.items() if v}}


@mcp.tool(annotations=RO)
async def list_input_kinds() -> dict:
    """Source kinds available in this OBS (depends on OS and plugins) plus the friendly aliases create_input accepts."""
    kinds = await input_kinds()
    aliases = {}
    for a, c in INPUT_ALIASES.items():
        hit = next((k for k in c if k in kinds), None)
        if hit:
            aliases[a] = hit
    return {"kinds": kinds, "aliases": aliases}


@mcp.tool()
async def create_input(name: str, kind: str, scene: str | None = None, settings: dict | None = None,
                       layout: str | None = None, visible: bool = True) -> dict:
    """Create a new source and add it to a scene (default: program scene). kind = real kind or alias: text, image,
    media, browser, color, display, window, game, camera, mic, desktop_audio, app_audio, spout, ndi, slideshow, vlc.
    Friendly settings: text {text, font:{face,size}, color:'#RRGGBB'}; image {file}; media {file, looping,
    restart_on_activate} or {url}; browser {url, width, height, css}; color {color:'#RRGGBB', width, height};
    spout {spoutsenders: sender name}. Colors '#RRGGBB(AA)' are converted for OBS. layout = preset (set_item_layout)."""
    scene = await scene_or_current(scene)
    real = await resolve_input_kind(kind)
    s = norm_settings(real, settings)
    if real.startswith("color_source"):
        W, H = await canvas()
        s.setdefault("width", W)
        s.setdefault("height", H)
    r = await obs.call("CreateInput", {"sceneName": scene, "inputName": name, "inputKind": real, "inputSettings": s, "sceneItemEnabled": visible})
    if layout:
        await obs.call("SetSceneItemTransform", {"sceneName": scene, "sceneItemId": r["sceneItemId"], "sceneItemTransform": await layout_transform(layout, None, "fit")})
    return {"done": True, "name": name, "kind": real, "scene": scene, "id": r.get("sceneItemId")}


@mcp.tool(annotations=RO)
async def get_input_settings(name: str, with_defaults: bool = False) -> dict:
    """Settings of a source. with_defaults=true also returns the default values, which show every key you can set."""
    r = await obs.call("GetInputSettings", {"inputName": name})
    out = {"name": name, "kind": r["inputKind"], "settings": mask_secrets(r["inputSettings"])}
    if with_defaults:
        out["defaults"] = (await obs.call("GetInputDefaultSettings", {"inputKind": r["inputKind"]}))["defaultInputSettings"]
    return out


@mcp.tool()
async def set_input_settings(name: str, settings: dict, replace: bool = False) -> dict:
    """Change a source's settings (merged over the current ones unless replace=true). Same friendly keys as create_input."""
    kind = (await obs.call("GetInputSettings", {"inputName": name}))["inputKind"]
    await obs.call("SetInputSettings", {"inputName": name, "inputSettings": norm_settings(kind, settings), "overlay": not replace})
    return {"done": True, "name": name}


@mcp.tool()
async def set_text(name: str, text: str) -> dict:
    """Change the text of a text source (titles, lower thirds, counters, now-playing...)."""
    await obs.call("SetInputSettings", {"inputName": name, "inputSettings": {"text": text}, "overlay": True})
    return {"done": True, "name": name, "text": text}


@mcp.tool()
async def rename_input(name: str, new_name: str) -> dict:
    """Rename a source (everywhere it is used)."""
    await obs.call("SetInputName", {"inputName": name, "newInputName": new_name})
    return {"done": True, "name": new_name}


@mcp.tool(annotations=DESTRUCTIVE)
async def remove_input(name: str, confirm: bool = False) -> dict:
    """Delete a source from ALL scenes. Needs confirm=true."""
    if not confirm:
        return need_confirm(f"cancellare la sorgente '{name}' da tutte le scene")
    await obs.call("RemoveInput", {"inputName": name})
    return {"done": True, "removed": name}


@mcp.tool(annotations=RO)
async def list_property_items(name: str, property: str) -> dict:
    """Choices of a list property of a source: e.g. camera devices (dshow_input 'video_device_id'), windows
    ('window'), monitors ('monitor_id'), audio devices ('device_id'), Spout senders ('spoutsenders')."""
    items = (await obs.call("GetInputPropertiesListPropertyItems", {"inputName": name, "propertyName": property}))["propertyItems"]
    return {"items": [{"name": i.get("itemName"), "value": i.get("itemValue"), "enabled": i.get("itemEnabled")} for i in items]}


@mcp.tool()
async def press_input_button(name: str, property: str = "refreshnocache") -> dict:
    """Press a button in a source's properties, e.g. browser 'refreshnocache' (reload page), camera 'activate'."""
    await obs.call("PressInputPropertiesButton", {"inputName": name, "propertyName": property})
    return {"done": True}


# ---------------------------------------------------------------------------
# Audio
# ---------------------------------------------------------------------------

MONITOR = {"off": "OBS_MONITORING_TYPE_NONE", "monitor_only": "OBS_MONITORING_TYPE_MONITOR_ONLY", "monitor_and_output": "OBS_MONITORING_TYPE_MONITOR_AND_OUTPUT"}


async def audio_inputs() -> list[str]:
    inputs = (await obs.call("GetInputList"))["inputs"]
    res = await obs.batch([{"requestType": "GetInputVolume", "requestData": {"inputName": i["inputName"]}} for i in inputs])
    return [inputs[n]["inputName"] for n, x in enumerate(res) if x.get("requestStatus", {}).get("result")]


@mcp.tool(annotations=RO)
async def audio_mixer() -> dict:
    """The audio mixer in one call: every audio source with volume (dB), mute, balance, sync offset, monitoring and tracks."""
    names = await audio_inputs()
    reqs = []
    for n in names:
        for t in ("GetInputVolume", "GetInputMute", "GetInputAudioBalance", "GetInputAudioSyncOffset", "GetInputAudioMonitorType", "GetInputAudioTracks"):
            reqs.append({"requestType": t, "requestData": {"inputName": n}})
    res = await obs.batch(reqs)
    out = []
    for i, n in enumerate(names):
        chunk = [(x.get("responseData") or {}) for x in res[i * 6:(i + 1) * 6]]
        vol, mute, bal, sync, mon, tracks = chunk
        out.append({"name": n, "volume_db": round(vol.get("inputVolumeDb", -100), 1), "muted": mute.get("inputMuted"),
                    "balance": bal.get("inputAudioBalance"), "sync_ms": sync.get("inputAudioSyncOffset"),
                    "monitor": {v: k for k, v in MONITOR.items()}.get(mon.get("monitorType"), mon.get("monitorType")),
                    "tracks": [int(k) for k, v in (tracks.get("inputAudioTracks") or {}).items() if v]})
    return {"audio": out}


@mcp.tool()
async def set_audio(name: str, volume_db: float | None = None, volume_percent: float | None = None, muted: bool | None = None,
                    toggle_mute: bool = False, balance: float | None = None, sync_ms: int | None = None,
                    monitor: str | None = None, tracks: list[int] | None = None) -> dict:
    """Set volume (dB, 0 = unity, or percent of the fader), mute/unmute/toggle, balance (0 left .. 0.5 center .. 1 right),
    sync offset in ms, monitoring (off | monitor_only | monitor_and_output) and output tracks (e.g. [1, 3])."""
    base = {"inputName": name}
    if volume_db is not None:
        await obs.call("SetInputVolume", {**base, "inputVolumeDb": max(-100.0, min(26.0, float(volume_db)))})
    if volume_percent is not None:
        await obs.call("SetInputVolume", {**base, "inputVolumeMul": max(0.0, min(20.0, volume_percent / 100.0))})
    if toggle_mute:
        await obs.call("ToggleInputMute", base)
    elif muted is not None:
        await obs.call("SetInputMute", {**base, "inputMuted": muted})
    if balance is not None:
        await obs.call("SetInputAudioBalance", {**base, "inputAudioBalance": max(0.0, min(1.0, balance))})
    if sync_ms is not None:
        await obs.call("SetInputAudioSyncOffset", {**base, "inputAudioSyncOffset": int(sync_ms)})
    if monitor:
        if monitor not in MONITOR:
            raise OBSError("monitor: off | monitor_only | monitor_and_output")
        await obs.call("SetInputAudioMonitorType", {**base, "monitorType": MONITOR[monitor]})
    if tracks is not None:
        await obs.call("SetInputAudioTracks", {**base, "inputAudioTracks": {str(t): (t in tracks) for t in range(1, 7)}})
    vol = await obs.call("GetInputVolume", base)
    mute = await obs.call("GetInputMute", base)
    return {"done": True, "name": name, "volume_db": round(vol["inputVolumeDb"], 1), "muted": mute["inputMuted"]}


@mcp.tool(annotations=RO)
async def audio_levels(seconds: float = 2.0, inputs: list[str] | None = None) -> dict:
    """Listen to the real audio meters for a few seconds and report peak/average level per source (dBFS).
    Answers 'is my mic actually picking up sound?' or 'is the music clipping?'. Silent < -60 dB, hot > -3 dB."""
    await obs.meters_start()
    try:
        await asyncio.sleep(max(0.3, min(seconds, 15)))
        data = {k: list(v) for k, v in obs.meters.items()}
    finally:
        await obs.meters_stop()
    out = []
    for name, frames in data.items():
        if inputs and name not in inputs:
            continue
        peaks, mags = [], []
        for chans in frames:
            if chans:
                peaks.append(max(c[1] for c in chans if len(c) > 1) if any(len(c) > 1 for c in chans) else 0)
                mags.append(max(c[0] for c in chans))
        if not peaks:
            continue
        p, a = mul_to_db(max(peaks)), mul_to_db(sum(mags) / len(mags))
        out.append({"name": name, "peak_db": p, "avg_db": a,
                    "verdict": "silenzio" if p < -60 else "basso" if p < -30 else "satura/vicino a 0 dB" if p > -3 else "ok"})
    return {"seconds": seconds, "levels": sorted(out, key=lambda x: -x["peak_db"]),
            **({} if out else {"note": "nessun livello ricevuto: nessuna sorgente audio attiva?"})}


@mcp.tool()
async def fade_audio(name: str, to_db: float, seconds: float = 2.0, mute_at_end: bool = False) -> dict:
    """Smooth volume fade of a source to to_db over `seconds` (e.g. music down to -30 dB under the voice, or to -100
    to fade out). mute_at_end mutes it once the fade finishes; fading up from muted unmutes first."""
    base = {"inputName": name}
    cur = (await obs.call("GetInputVolume", base))["inputVolumeDb"]
    if (await obs.call("GetInputMute", base))["inputMuted"] and to_db > cur:
        await obs.call("SetInputMute", {**base, "inputMuted": False})
    start = max(cur, -60.0) if to_db > cur else cur
    target = max(-100.0, min(26.0, to_db))
    steps = max(1, int(seconds * 25))
    for i in range(1, steps + 1):
        f = i / steps
        f = f * f * (3 - 2 * f)  # smoothstep
        db = start + (max(target, -60.0) - start) * f
        await obs.call("SetInputVolume", {**base, "inputVolumeDb": db})
        await asyncio.sleep(seconds / steps)
    await obs.call("SetInputVolume", {**base, "inputVolumeDb": target})
    if mute_at_end:
        await obs.call("SetInputMute", {**base, "inputMuted": True})
    return {"done": True, "name": name, "from_db": round(cur, 1), "to_db": target}


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


@mcp.tool(annotations=RO)
async def list_filters(source: str) -> dict:
    """Filters of a source or scene, in order, with kind, enabled and settings."""
    f = (await obs.call("GetSourceFilterList", {"sourceName": source}))["filters"]
    return {"source": source, "filters": [{"name": x["filterName"], "kind": x["filterKind"], "enabled": x["filterEnabled"],
                                           "index": x["filterIndex"], "settings": x.get("filterSettings")} for x in f]}


@mcp.tool()
async def add_filter(source: str, kind: str, name: str | None = None, settings: dict | None = None, index: int | None = None) -> dict:
    """Add a filter to a source or scene. kind = real kind or alias: chroma_key, color_key, luma_key, color_correction,
    sharpen, mask, scroll, crop, scale, video_delay, lut, noise_suppress, noise_gate, compressor, limiter, expander,
    upward_compressor, gain, eq3, audio_delay, invert_polarity, vst. Colors '#RRGGBB' are converted.
    Examples: chroma_key {key_color_type:'green', similarity:400}; color_correction {saturation:-1} (b/w)."""
    real = await resolve_filter_kind(kind)
    fname = name or kind.replace("_", " ").title()
    s = {k: hex_to_abgr(v) for k, v in (settings or {}).items()}
    await obs.call("CreateSourceFilter", {"sourceName": source, "filterName": fname, "filterKind": real, "filterSettings": s})
    if index is not None:
        await obs.call("SetSourceFilterIndex", {"sourceName": source, "filterName": fname, "filterIndex": index})
    return {"done": True, "source": source, "filter": fname, "kind": real}


@mcp.tool()
async def set_filter(source: str, filter: str, enabled: bool | None = None, toggle: bool = False, settings: dict | None = None,
                     index: int | None = None, new_name: str | None = None) -> dict:
    """Enable/disable/toggle a filter, change its settings (merged), move it in the chain or rename it."""
    base = {"sourceName": source, "filterName": filter}
    if toggle:
        cur = (await obs.call("GetSourceFilter", base))["filterEnabled"]
        enabled = not cur
    if enabled is not None:
        await obs.call("SetSourceFilterEnabled", {**base, "filterEnabled": enabled})
    if settings:
        await obs.call("SetSourceFilterSettings", {**base, "filterSettings": {k: hex_to_abgr(v) for k, v in settings.items()}, "overlay": True})
    if index is not None:
        await obs.call("SetSourceFilterIndex", {**base, "filterIndex": index})
    if new_name:
        await obs.call("SetSourceFilterName", {**base, "newFilterName": new_name})
    return {"done": True, "source": source, "filter": new_name or filter, **({"enabled": enabled} if enabled is not None else {})}


@mcp.tool(annotations=DESTRUCTIVE)
async def remove_filter(source: str, filter: str) -> dict:
    """Remove a filter from a source."""
    await obs.call("RemoveSourceFilter", {"sourceName": source, "filterName": filter})
    return {"done": True, "removed": filter}


@mcp.tool()
async def clean_mic(source: str, strength: str = "medium") -> dict:
    """Build a voice chain on a microphone in one call: Noise Suppression (RNNoise) -> Noise Gate -> Compressor ->
    Limiter. Stages whose kind already exists on the source are skipped. strength: light | medium | strong."""
    gate = {"light": (-40, -46), "medium": (-32, -38), "strong": (-26, -32)}.get(strength)
    if not gate:
        raise OBSError("strength: light | medium | strong")
    existing = {f["filterKind"] for f in (await obs.call("GetSourceFilterList", {"sourceName": source}))["filters"]}
    chain = [
        ("noise_suppress", "Riduzione rumore", {"method": "rnnoise"}),
        ("noise_gate", "Noise gate", {"open_threshold": gate[0], "close_threshold": gate[1], "attack_time": 25, "hold_time": 200, "release_time": 150}),
        ("compressor", "Compressore", {"ratio": 4.0, "threshold": -18.0, "attack_time": 6, "release_time": 60, "output_gain": 0.0}),
        ("limiter", "Limiter", {"threshold": -3.0, "release_time": 60}),
    ]
    added, skipped = [], []
    for alias, fname, s in chain:
        real = await resolve_filter_kind(alias)
        if real in existing or any(k in existing for k in FILTER_ALIASES[alias]):
            skipped.append(fname)
            continue
        await obs.call("CreateSourceFilter", {"sourceName": source, "filterName": fname, "filterKind": real, "filterSettings": s})
        added.append(fname)
    return {"done": True, "source": source, "added": added, "skipped_existing": skipped,
            "tip": "controlla con audio_levels mentre parli: picchi tra -12 e -6 dB sono ideali"}


# ---------------------------------------------------------------------------
# Transitions / studio mode
# ---------------------------------------------------------------------------


@mcp.tool()
async def transitions(name: str | None = None, duration_ms: int | None = None, settings: dict | None = None) -> dict:
    """List transitions, or set the current one (name), its duration and settings (e.g. stinger path)."""
    if name:
        await obs.call("SetCurrentSceneTransition", {"transitionName": name})
    if duration_ms is not None:
        await obs.call("SetCurrentSceneTransitionDuration", {"transitionDuration": max(50, min(20000, duration_ms))})
    if settings:
        await obs.call("SetCurrentSceneTransitionSettings", {"transitionSettings": settings, "overlay": True})
    tr = await obs.call("GetSceneTransitionList")
    return {"current": tr.get("currentSceneTransitionName"), "duration_ms": (await obs.try_call("GetCurrentSceneTransition") or {}).get("transitionDuration"),
            "transitions": [{"name": t["transitionName"], "kind": t["transitionKind"], "fixed": t.get("transitionFixed")} for t in tr["transitions"]]}


@mcp.tool()
async def studio_mode(enabled: bool | None = None, take: bool = False, tbar: float | None = None, release: bool = True) -> dict:
    """Turn studio mode on/off, 'take' (preview -> program with the current transition), or move the T-bar
    (0..1; release=false to hold it mid-way)."""
    if enabled is not None:
        await obs.call("SetStudioModeEnabled", {"studioModeEnabled": enabled})
    if tbar is not None:
        await obs.call("SetTBarPosition", {"position": max(0.0, min(1.0, tbar)), "release": release})
    if take:
        await obs.call("TriggerStudioModeTransition")
    sl = await obs.call("GetSceneList")
    return {"studio_mode": (await obs.call("GetStudioModeEnabled"))["studioModeEnabled"],
            "program": sl.get("currentProgramSceneName"), "preview": sl.get("currentPreviewSceneName")}


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------


@mcp.tool()
async def stream(action: str = "status", caption: str | None = None, confirm: bool = False) -> dict:
    """Streaming: status | start | stop | caption (send a CEA-608 caption line). stop needs confirm=true."""
    if action == "start":
        await obs.call("StartStream")
    elif action == "stop":
        if not confirm:
            return need_confirm("fermare la diretta")
        await obs.call("StopStream")
    elif action == "caption":
        await obs.call("SendStreamCaption", {"captionText": caption or ""})
    elif action != "status":
        raise OBSError("action: status | start | stop | caption")
    if action in ("start", "stop"):
        await asyncio.sleep(1.0)
    s = await obs.call("GetStreamStatus")
    return {"active": s.get("outputActive"), "reconnecting": s.get("outputReconnecting"), "timecode": s.get("outputTimecode"),
            "congestion": s.get("outputCongestion"), "skipped_frames": s.get("outputSkippedFrames"), "total_frames": s.get("outputTotalFrames"),
            "mb_sent": round((s.get("outputBytes") or 0) / 1e6, 1)}


@mcp.tool()
async def record(action: str = "status", chapter: str | None = None, confirm: bool = False) -> dict:
    """Recording: status | start | stop (returns the file path, needs confirm=true) | pause | resume | split (new file)
    | chapter (add a chapter marker, Hybrid MP4)."""
    out: dict[str, Any] = {}
    if action == "start":
        await obs.call("StartRecord")
        await asyncio.sleep(0.5)
    elif action == "stop":
        if not confirm:
            return need_confirm("fermare la registrazione")
        out["file"] = (await obs.call("StopRecord")).get("outputPath")
    elif action == "pause":
        await obs.call("PauseRecord")
    elif action == "resume":
        await obs.call("ResumeRecord")
    elif action == "split":
        await obs.call("SplitRecordFile")
    elif action == "chapter":
        await obs.call("CreateRecordChapter", {"chapterName": chapter})
    elif action != "status":
        raise OBSError("action: status | start | stop | pause | resume | split | chapter")
    s = await obs.call("GetRecordStatus")
    folder = (await obs.try_call("GetRecordDirectory") or {}).get("recordDirectory")
    return {**out, "active": s.get("outputActive"), "paused": s.get("outputPaused"), "timecode": s.get("outputTimecode"),
            "mb": round((s.get("outputBytes") or 0) / 1e6, 1), "folder": folder}


@mcp.tool()
async def replay_buffer(action: str = "status") -> dict:
    """Replay buffer: status | start | stop | save (saves the last N seconds and returns the file path)."""
    out: dict[str, Any] = {}
    if (await obs.try_call("GetReplayBufferStatus")) is None:
        return {"available": False, "active": False,
                "message": "Replay buffer non attivo in questo profilo: OBS > Impostazioni > Uscita > Replay Buffer > "
                           "Abilita Replay Buffer (con la durata in secondi), poi OK."}
    if action == "start":
        await obs.call("StartReplayBuffer")
    elif action == "stop":
        await obs.call("StopReplayBuffer")
    elif action == "save":
        seq = obs.event_seq
        await obs.call("SaveReplayBuffer")
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            ev = [e for e in obs.events if e["seq"] > seq and e["type"] == "ReplayBufferSaved"]
            if ev:
                out["file"] = (ev[-1]["data"] or {}).get("savedReplayPath")
                break
            await asyncio.sleep(0.2)
        if "file" not in out:
            out["file"] = (await obs.try_call("GetLastReplayBufferReplay") or {}).get("savedReplayPath")
    elif action != "status":
        raise OBSError("action: status | start | stop | save")
    return {**out, "active": (await obs.call("GetReplayBufferStatus"))["outputActive"]}


@mcp.tool()
async def virtual_cam(action: str = "status") -> dict:
    """Virtual camera (OBS as a webcam for Zoom/Meet/Teams/Discord): status | start | stop."""
    if action == "start":
        await obs.call("StartVirtualCam")
    elif action == "stop":
        await obs.call("StopVirtualCam")
    elif action != "status":
        raise OBSError("action: status | start | stop")
    return {"active": (await obs.call("GetVirtualCamStatus"))["outputActive"]}


@mcp.tool()
async def stream_service(server: str | None = None, key: str | None = None, service: str | None = None, confirm: bool = False) -> dict:
    """Read the stream destination (the key is always hidden), or change server/key (rtmp_custom) / service name.
    Changing needs confirm=true. Never repeat a stream key back to the user."""
    cur = await obs.call("GetStreamServiceSettings")
    if server or key or service:
        if not confirm:
            return need_confirm("cambiare la destinazione della diretta")
        s = dict(cur.get("streamServiceSettings") or {})
        typ = cur.get("streamServiceType")
        if server:
            s["server"] = server
            if not service:
                typ = "rtmp_custom"
        if key:
            s["key"] = key
        if service:
            s["service"] = service
            typ = "rtmp_common"
        await obs.call("SetStreamServiceSettings", {"streamServiceType": typ, "streamServiceSettings": s})
        cur = await obs.call("GetStreamServiceSettings")
    return mask_secrets({"type": cur.get("streamServiceType"), "settings": cur.get("streamServiceSettings")})


# ---------------------------------------------------------------------------
# Media
# ---------------------------------------------------------------------------

MEDIA_ACTIONS = {"play", "pause", "stop", "restart", "next", "previous"}


@mcp.tool()
async def media_control(name: str, action: str = "status", seek_seconds: float | None = None, offset_seconds: float | None = None) -> dict:
    """Media/VLC sources: status | play | pause | stop | restart | next | previous; seek_seconds jumps to a time,
    offset_seconds moves forward/back."""
    base = {"inputName": name}
    if action in MEDIA_ACTIONS:
        await obs.call("TriggerMediaInputAction", {**base, "mediaAction": "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_" + action.upper()})
    elif action != "status":
        raise OBSError(f"action: status | {' | '.join(sorted(MEDIA_ACTIONS))}")
    if seek_seconds is not None:
        await obs.call("SetMediaInputCursor", {**base, "mediaCursor": int(seek_seconds * 1000)})
    if offset_seconds is not None:
        await obs.call("OffsetMediaInputCursor", {**base, "mediaCursorOffset": int(offset_seconds * 1000)})
    await asyncio.sleep(0.15)
    s = await obs.call("GetMediaInputStatus", base)
    d, c = s.get("mediaDuration"), s.get("mediaCursor")
    return {"name": name, "state": (s.get("mediaState") or "").replace("OBS_MEDIA_STATE_", "").lower(),
            "position_s": round(c / 1000, 1) if c is not None else None, "duration_s": round(d / 1000, 1) if d else None,
            "remaining_s": round((d - c) / 1000, 1) if d and c is not None else None}


# ---------------------------------------------------------------------------
# Screenshots
# ---------------------------------------------------------------------------


@mcp.tool(annotations=RO)
async def screenshot(source: str | None = None, width: int = 1280, format: str = "jpg", quality: int = 85, save_path: str | None = None):
    """Look at OBS: image of the program output (default), 'preview' (studio mode), or any scene/source by name.
    save_path also writes a full-quality file on the OBS computer. Use it to check layouts you just built."""
    if source in (None, "", "program"):
        source = await current_scene()
    elif source == "preview":
        source = (await obs.call("GetCurrentPreviewScene"))["currentPreviewSceneName"]
    fmt = "jpg" if format.lower() in ("jpg", "jpeg") else "png"
    width = max(8, min(4096, int(width)))
    # OBS renders a scene/source that is not on screen only when something looks at it: the first capture of a
    # hidden scene comes out empty or stale. Wake it with a tiny capture and give it a few frames.
    act = await obs.try_call("GetSourceActive", {"sourceName": source}) or {}
    if not act.get("videoShowing"):
        await obs.try_call("GetSourceScreenshot", {"sourceName": source, "imageFormat": "png", "imageWidth": 8})
        await asyncio.sleep(0.3)
    if save_path:
        await obs.call("SaveSourceScreenshot", {"sourceName": source, "imageFormat": fmt, "imageFilePath": save_path, "imageCompressionQuality": -1})
    r = await obs.call("GetSourceScreenshot", {"sourceName": source, "imageFormat": fmt, "imageWidth": width,
                                               "imageCompressionQuality": quality if fmt == "jpg" else -1}, timeout=30)
    data = r["imageData"].split(",", 1)[-1]
    img = Image(data=base64.b64decode(data), format="jpeg" if fmt == "jpg" else "png")
    info = f"screenshot di '{source}' ({width}px)" + (f", salvato in {save_path}" if save_path else "")
    return [img, info]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@mcp.tool()
async def scene_collections(action: str = "list", name: str | None = None, confirm: bool = False) -> dict:
    """Scene collections: list | switch (needs confirm, reloads every scene) | create (new empty collection, switches to it, needs confirm)."""
    if action in ("switch", "create"):
        if not name:
            raise OBSError("serve name")
        if not confirm:
            return need_confirm(f"{'passare alla' if action == 'switch' else 'creare la'} collezione di scene '{name}' (ricarica tutte le scene)")
        await obs.call("SetCurrentSceneCollection" if action == "switch" else "CreateSceneCollection", {"sceneCollectionName": name}, timeout=60)
        await asyncio.sleep(1.5)
    elif action != "list":
        raise OBSError("action: list | switch | create")
    r = await obs.call("GetSceneCollectionList")
    return {"current": r["currentSceneCollectionName"], "collections": r["sceneCollections"]}


@mcp.tool()
async def profiles(action: str = "list", name: str | None = None, confirm: bool = False) -> dict:
    """Profiles (output/encoder/stream settings): list | switch (needs confirm=true; not while live)."""
    if action == "switch":
        if not name:
            raise OBSError("serve name")
        if not confirm:
            return need_confirm(f"passare al profilo '{name}'")
        await obs.call("SetCurrentProfile", {"profileName": name}, timeout=60)
    elif action != "list":
        raise OBSError("action: list | switch")
    r = await obs.call("GetProfileList")
    return {"current": r["currentProfileName"], "profiles": r["profiles"]}


@mcp.tool()
async def video_settings(canvas_width: int | None = None, canvas_height: int | None = None, output_width: int | None = None,
                         output_height: int | None = None, fps: float | None = None, confirm: bool = False) -> dict:
    """Read or change canvas (base) resolution, output (scaled) resolution and fps. Changing needs confirm=true and
    is refused while streaming/recording (OBS rule)."""
    if any(v is not None for v in (canvas_width, canvas_height, output_width, output_height, fps)):
        if not confirm:
            return need_confirm("cambiare risoluzione/fps")
        d: dict[str, Any] = {"baseWidth": canvas_width, "baseHeight": canvas_height, "outputWidth": output_width, "outputHeight": output_height}
        if fps:
            num, den = {29.97: (30000, 1001), 59.94: (60000, 1001), 23.976: (24000, 1001)}.get(round(fps, 3), (int(round(fps)), 1))
            d.update(fpsNumerator=num, fpsDenominator=den)
        await obs.call("SetVideoSettings", d)
    v = await obs.call("GetVideoSettings")
    return {"canvas": [v["baseWidth"], v["baseHeight"]], "output": [v["outputWidth"], v["outputHeight"]],
            "fps": round(v["fpsNumerator"] / v["fpsDenominator"], 3)}


@mcp.tool()
async def record_directory(path: str | None = None) -> dict:
    """Read or change the folder where recordings and replays are saved."""
    if path:
        await obs.call("SetRecordDirectory", {"recordDirectory": path})
    return {"folder": (await obs.call("GetRecordDirectory"))["recordDirectory"]}


# ---------------------------------------------------------------------------
# UI
# ---------------------------------------------------------------------------


@mcp.tool()
async def open_projector(what: str = "program", monitor: int | None = None) -> dict:
    """Open a projector: what = program | preview | multiview | <scene or source name>. monitor = screen index for
    fullscreen (see the list returned when omitted); without monitor it opens windowed."""
    mons = (await obs.try_call("GetMonitorList") or {}).get("monitors", [])
    mix = {"program": "OBS_WEBSOCKET_VIDEO_MIX_TYPE_PROGRAM", "preview": "OBS_WEBSOCKET_VIDEO_MIX_TYPE_PREVIEW", "multiview": "OBS_WEBSOCKET_VIDEO_MIX_TYPE_MULTIVIEW"}
    m = monitor if monitor is not None else -1
    if what in mix:
        await obs.call("OpenVideoMixProjector", {"videoMixType": mix[what], "monitorIndex": m})
    else:
        await obs.call("OpenSourceProjector", {"sourceName": what, "monitorIndex": m})
    return {"done": True, "what": what, "monitor": monitor, "monitors": [{"index": x.get("monitorIndex"), "name": x.get("monitorName"),
                                                                         "size": f"{x.get('monitorWidth')}x{x.get('monitorHeight')}"} for x in mons]}


@mcp.tool()
async def open_dialog(source: str, dialog: str = "properties") -> dict:
    """Open an OBS window for the user on a source: properties | filters | interact (browser sources)."""
    t = {"properties": "OpenInputPropertiesDialog", "filters": "OpenInputFiltersDialog", "interact": "OpenInputInteractDialog"}.get(dialog)
    if not t:
        raise OBSError("dialog: properties | filters | interact")
    await obs.call(t, {"inputName": source})
    return {"done": True}


# ---------------------------------------------------------------------------
# Pipelines
# ---------------------------------------------------------------------------

FILE_KEYS = {"image_source": "file", "ffmpeg_source": "local_file", "vlc_source": None, "slideshow": None, "slideshow_v2": None}


@mcp.tool(annotations=RO)
async def show_check(listen_seconds: float = 0) -> dict:
    """Pre-show / live health check: fps, CPU, render & encoder lag, dropped network frames, disk space, missing media
    files, muted or very low microphones, empty scenes, stream destination set. listen_seconds > 0 also listens to the
    real audio meters. Returns problems (bloccante/attenzione) and a verdict."""
    problems: list[dict] = []

    def add(level, what, fix):
        problems.append({"level": level, "what": what, "fix": fix})

    st = await obs.call("GetStats")
    vs = await obs.call("GetVideoSettings")
    target = vs["fpsNumerator"] / vs["fpsDenominator"]
    if st.get("activeFps", target) < target * 0.95:
        add("attenzione", f"fps {st['activeFps']:.1f} invece di {target:.0f}", "riduci sorgenti pesanti o risoluzione")
    budget = 1000 / target
    if st.get("averageFrameRenderTime", 0) > budget * 0.8:
        add("attenzione", f"render {st['averageFrameRenderTime']:.1f} ms per frame (budget {budget:.1f} ms)", "GPU al limite: meno filtri/browser source")
    rt = st.get("renderTotalFrames") or 0
    if rt and (st.get("renderSkippedFrames") or 0) / rt > 0.01:
        add("attenzione", f"{st['renderSkippedFrames']} frame saltati nel render", "GPU sovraccarica (giochi in primo piano? avvia OBS come amministratore)")
    ot = st.get("outputTotalFrames") or 0
    if ot and (st.get("outputSkippedFrames") or 0) / ot > 0.01:
        add("bloccante", f"{st['outputSkippedFrames']} frame saltati dall'encoder", "encoder sovraccarico: preset più veloce, encoder hardware o bitrate/risoluzione più bassi")
    if st.get("cpuUsage", 0) > 85:
        add("attenzione", f"CPU di OBS al {st['cpuUsage']:.0f}%", "usa NVENC/QuickSync invece di x264")
    disk_gb = (st.get("availableDiskSpace") or 0) / 1024
    if disk_gb and disk_gb < 10:
        add("bloccante" if disk_gb < 3 else "attenzione", f"solo {disk_gb:.1f} GB liberi sul disco di registrazione", "libera spazio o cambia record_directory")
    ss = await obs.call("GetStreamStatus")
    if ss.get("outputActive"):
        if (ss.get("outputCongestion") or 0) > 0.1:
            add("bloccante", f"congestione rete {ss['outputCongestion']:.0%}", "abbassa il bitrate o usa il cavo")
        if ss.get("outputReconnecting"):
            add("bloccante", "la diretta si sta riconnettendo", "controlla la connessione")
    else:
        svc = await obs.try_call("GetStreamServiceSettings") or {}
        ss_set = svc.get("streamServiceSettings") or {}
        if not ss_set.get("key") and svc.get("streamServiceType") in ("rtmp_common", "rtmp_custom") and not ss_set.get("bearer_token"):
            add("attenzione", "nessuna chiave di streaming impostata", "Impostazioni > Stream, oppure stream_service")

    inputs = (await obs.call("GetInputList"))["inputs"]
    res = await obs.batch([{"requestType": "GetInputSettings", "requestData": {"inputName": i["inputName"]}} for i in inputs])
    for i, x in zip(inputs, res):
        s = (x.get("responseData") or {}).get("inputSettings") or {}
        k = i["inputKind"]
        path = None
        if k == "image_source":
            path = s.get("file")
        elif k == "ffmpeg_source" and s.get("is_local_file", True):
            path = s.get("local_file")
        if obs.is_local and path and not Path(path).exists():
            add("bloccante", f"file mancante in '{i['inputName']}': {path}", "ricollega il file (set_input_settings file=...)")
        if k == "browser_source" and not s.get("url") and not s.get("is_local_file"):
            add("attenzione", f"browser source '{i['inputName']}' senza URL", "imposta url")

    mic_kinds = set(INPUT_ALIASES["mic"])
    names = await audio_inputs()
    kinds = {i["inputName"]: i["inputKind"] for i in inputs}
    if names:
        res = await obs.batch([{"requestType": t, "requestData": {"inputName": n}} for n in names for t in ("GetInputMute", "GetInputVolume")])
        for j, n in enumerate(names):
            muted = (res[2 * j].get("responseData") or {}).get("inputMuted")
            db = (res[2 * j + 1].get("responseData") or {}).get("inputVolumeDb", 0)
            if kinds.get(n) in mic_kinds:
                if muted:
                    add("attenzione", f"microfono '{n}' in muto", "set_audio muted=false")
                elif db < -20:
                    add("attenzione", f"microfono '{n}' molto basso ({db:.0f} dB)", "alza il fader")
    else:
        add("attenzione", "nessuna sorgente audio", "aggiungi un microfono o l'audio desktop")

    if listen_seconds > 0:
        lv = await audio_levels(listen_seconds)
        for l in lv["levels"]:
            if l["peak_db"] > -1:
                add("attenzione", f"'{l['name']}' satura (picco {l['peak_db']} dB)", "abbassa il gain o aggiungi un limiter (clean_mic)")
        heard = {l["name"] for l in lv["levels"] if l["peak_db"] > -60}
        for n in names:
            if kinds.get(n) in mic_kinds and n not in heard:
                add("attenzione", f"dal microfono '{n}' non arriva suono", "dispositivo giusto? parla durante il controllo")

    sl = await obs.call("GetSceneList")
    empty = []
    for sc in sl["scenes"]:
        it = await obs.try_call("GetSceneItemList", {"sceneName": sc["sceneName"]}) or {}
        if not it.get("sceneItems"):
            empty.append(sc["sceneName"])
    if empty:
        add("attenzione", f"scene vuote: {', '.join(empty)}", "riempile o cancellale")

    block = sum(p["level"] == "bloccante" for p in problems)
    return {"verdict": "NON pronto" if block else ("pronto, con avvisi" if problems else "pronto"),
            "problems": problems, "fps": round(st.get("activeFps", 0), 1), "cpu": round(st.get("cpuUsage", 0), 1),
            "disk_free_gb": round(disk_gb, 1), "streaming": ss.get("outputActive")}


@mcp.tool()
async def build_scene(name: str, items: list[dict], switch_to: bool = False, replace: bool = False, confirm: bool = False) -> dict:
    """Create a whole scene in one call, with rollback if anything fails. items are listed bottom layer first:
    {"name": "Sfondo", "kind": "color", "settings": {"color": "#101018"}},
    {"name": "Camera", "kind": "camera", "layout": "pip_bottom_right", "filters": [{"kind": "chroma_key"}]},
    {"name": "Titolo", "kind": "text", "settings": {"text": "LIVE", "font": {"face": "Arial", "size": 96}}, "layout": "lower_third"},
    {"name": "Webcam esistente"} (no kind = reuse an existing source), {"name": "Altra scena", "kind": "scene"} (nested).
    Optional per item: settings, layout (preset), box [x,y,w,h], mode (fit|cover|stretch), visible, filters.
    replace=true deletes an existing scene with the same name first (needs confirm=true)."""
    scenes = [s["sceneName"] for s in (await obs.call("GetSceneList"))["scenes"]]
    if name in scenes:
        if not replace:
            raise OBSError(f"la scena '{name}' esiste già (replace=true per rifarla)")
        if not confirm:
            return need_confirm(f"cancellare e rifare la scena '{name}'")
        await obs.call("RemoveScene", {"sceneName": name})
    existing = {i["inputName"] for i in (await obs.call("GetInputList"))["inputs"]}
    created_inputs: list[str] = []
    report = []
    await obs.call("CreateScene", {"sceneName": name})
    try:
        for it in items:
            nm = it["name"]
            kind = it.get("kind")
            if kind == "scene" or (not kind and (nm in existing or nm in scenes)):
                sid = (await obs.call("CreateSceneItem", {"sceneName": name, "sourceName": nm, "sceneItemEnabled": it.get("visible", True)}))["sceneItemId"]
                real = "scene" if kind == "scene" or nm in scenes else "existing"
            elif not kind:
                raise OBSError(f"'{nm}': manca kind e non esiste una sorgente con questo nome")
            else:
                if nm in existing:
                    raise OBSError(f"esiste già una sorgente '{nm}': togli kind per riusarla, o cambia nome")
                real = await resolve_input_kind(kind)
                s = norm_settings(real, it.get("settings"))
                if real.startswith("color_source"):
                    W, H = await canvas()
                    s.setdefault("width", W)
                    s.setdefault("height", H)
                sid = (await obs.call("CreateInput", {"sceneName": name, "inputName": nm, "inputKind": real, "inputSettings": s,
                                                      "sceneItemEnabled": it.get("visible", True)}))["sceneItemId"]
                created_inputs.append(nm)
            if it.get("layout") or it.get("box"):
                await obs.call("SetSceneItemTransform", {"sceneName": name, "sceneItemId": sid,
                                                         "sceneItemTransform": await layout_transform(it.get("layout"), it.get("box"), it.get("mode", "fit"))})
            for f in it.get("filters", []) or []:
                fk = await resolve_filter_kind(f["kind"])
                await obs.call("CreateSourceFilter", {"sourceName": nm, "filterName": f.get("name") or f["kind"].replace("_", " ").title(),
                                                      "filterKind": fk, "filterSettings": {k: hex_to_abgr(v) for k, v in (f.get("settings") or {}).items()}})
            report.append({"name": nm, "kind": real, "id": sid})
    except Exception as e:
        for n in created_inputs:
            await obs.try_call("RemoveInput", {"inputName": n})
        await obs.try_call("RemoveScene", {"sceneName": name})
        raise OBSError(f"build_scene annullato, niente è stato lasciato a metà: {e}") from None
    if switch_to:
        await obs.call("SetCurrentProgramScene", {"sceneName": name})
    return {"done": True, "scene": name, "items": report, "tip": "controlla con screenshot(source='" + name + "')"}


LOOKS_FILE = Path(os.environ.get("AJ_OBS_LOOKS", Path.home() / ".aj-mcp" / "obs-looks.json"))


def _looks_load() -> dict:
    try:
        return json.loads(LOOKS_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


@mcp.tool()
async def look(action: str = "list", name: str | None = None, scene: str | None = None, include_audio: bool = True, fade_seconds: float = 0) -> dict:
    """Save and recall 'looks': which sources are visible in a scene (or all scenes) plus audio volumes/mutes.
    action: save | apply | list | delete. apply restores everything in one batch; fade_seconds > 0 fades audio."""
    looks = _looks_load()
    if action == "list":
        return {"looks": {k: {"scenes": list(v.get("scenes", {})), "audio": len(v.get("audio", {})), "saved": v.get("saved")} for k, v in looks.items()}}
    if not name:
        raise OBSError("serve name")
    if action == "delete":
        looks.pop(name, None)
        LOOKS_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOOKS_FILE.write_text(json.dumps(looks, indent=2, ensure_ascii=False), encoding="utf-8")
        return {"done": True, "deleted": name}
    if action == "save":
        scenes = [scene] if scene else [s["sceneName"] for s in (await obs.call("GetSceneList"))["scenes"]]
        data: dict[str, Any] = {"saved": time.strftime("%Y-%m-%d %H:%M"), "scenes": {}, "audio": {}}
        for sc in scenes:
            its = (await obs.call("GetSceneItemList", {"sceneName": sc}))["sceneItems"]
            data["scenes"][sc] = {i["sourceName"]: i["sceneItemEnabled"] for i in its}
        if include_audio:
            for a in (await audio_mixer())["audio"]:
                data["audio"][a["name"]] = {"volume_db": a["volume_db"], "muted": a["muted"]}
        looks[name] = data
        LOOKS_FILE.parent.mkdir(parents=True, exist_ok=True)
        LOOKS_FILE.write_text(json.dumps(looks, indent=2, ensure_ascii=False), encoding="utf-8")
        return {"done": True, "saved": name, "scenes": len(data["scenes"]), "audio": len(data["audio"]), "file": str(LOOKS_FILE)}
    if action == "apply":
        lk = looks.get(name)
        if not lk:
            raise OBSError(f"look '{name}' non trovato. Salvati: {', '.join(looks) or 'nessuno'}")
        reqs = []
        for sc, items in lk["scenes"].items():
            cur = (await obs.try_call("GetSceneItemList", {"sceneName": sc}) or {}).get("sceneItems", [])
            for i in cur:
                if i["sourceName"] in items and items[i["sourceName"]] != i["sceneItemEnabled"]:
                    reqs.append({"requestType": "SetSceneItemEnabled", "requestData": {"sceneName": sc, "sceneItemId": i["sceneItemId"], "sceneItemEnabled": items[i["sourceName"]]}})
        fades = []
        if include_audio:
            for n, a in lk.get("audio", {}).items():
                if fade_seconds > 0 and not a["muted"]:
                    fades.append(fade_audio(n, a["volume_db"], fade_seconds))
                else:
                    reqs.append({"requestType": "SetInputVolume", "requestData": {"inputName": n, "inputVolumeDb": a["volume_db"]}})
                    reqs.append({"requestType": "SetInputMute", "requestData": {"inputName": n, "inputMuted": a["muted"]}})
        res = await obs.batch(reqs) if reqs else []
        if fades:
            await asyncio.gather(*fades, return_exceptions=True)
        failed = [x.get("requestStatus", {}).get("comment") for x in res if not x.get("requestStatus", {}).get("result")]
        return {"done": True, "applied": name, "changes": len(reqs) + len(fades), **({"failed": failed} if failed else {})}
    raise OBSError("action: save | apply | list | delete")


# ---------------------------------------------------------------------------
# 1.2: outputs, persistent data, plugins (vendor requests), per-scene transitions,
# source inspection, stream health, countdown
# ---------------------------------------------------------------------------


@mcp.tool()
async def outputs(action: str = "list", name: str | None = None, settings: dict | None = None, confirm: bool = False) -> dict:
    """All OBS outputs, including the ones added by plugins (Aitum Multistream/Vertical, NDI, Source Record, ...).
    action: list | status | settings (read, or write with settings=) | start | stop (needs confirm=true) | toggle."""
    if action == "list":
        outs = (await obs.call("GetOutputList"))["outputs"]
        return {"outputs": [{"name": o.get("outputName"), "kind": o.get("outputKind"), "active": o.get("outputActive"),
                             "size": f"{o.get('outputWidth')}x{o.get('outputHeight')}"} for o in outs]}
    if not name:
        raise OBSError("serve name (vedi outputs action='list')")
    base = {"outputName": name}
    if action == "start":
        await obs.call("StartOutput", base)
    elif action in ("stop", "toggle"):
        if not confirm:
            return need_confirm(f"{action} dell'uscita '{name}'")
        await obs.call("StopOutput" if action == "stop" else "ToggleOutput", base)
    elif action == "settings":
        if settings:
            if not confirm:
                return need_confirm(f"cambiare le impostazioni dell'uscita '{name}'")
            await obs.call("SetOutputSettings", {**base, "outputSettings": settings})
        return {"name": name, "settings": mask_secrets((await obs.call("GetOutputSettings", base)).get("outputSettings", {}))}
    elif action != "status":
        raise OBSError("action: list | status | settings | start | stop | toggle")
    s = await obs.call("GetOutputStatus", base)
    return {"name": name, "active": s.get("outputActive"), "reconnecting": s.get("outputReconnecting"),
            "timecode": s.get("outputTimecode"), "congestion": s.get("outputCongestion"),
            "mb": round((s.get("outputBytes") or 0) / 1e6, 1), "skipped_frames": s.get("outputSkippedFrames"),
            "total_frames": s.get("outputTotalFrames")}


REALMS = {"global": "OBS_WEBSOCKET_DATA_REALM_GLOBAL", "profile": "OBS_WEBSOCKET_DATA_REALM_PROFILE"}


@mcp.tool()
async def persistent_data(key: str, value: Any = None, realm: str = "global", delete: bool = False) -> dict:
    """Small notes stored INSIDE OBS (they survive restarts): read when value is omitted, write otherwise.
    realm: global (all profiles) | profile (only the current profile). Handy for show notes, counters, last scene."""
    r = REALMS.get(realm)
    if not r:
        raise OBSError("realm: global | profile")
    if delete:
        await obs.call("SetPersistentData", {"realm": r, "slotName": key, "slotValue": None}, keep_none=True)
        return {"done": True, "deleted": key}
    if value is not None:
        await obs.call("SetPersistentData", {"realm": r, "slotName": key, "slotValue": value})
    got = await obs.call("GetPersistentData", {"realm": r, "slotName": key})
    return {"key": key, "realm": realm, "value": got.get("slotValue")}


KNOWN_VENDORS = {
    "AdvancedSceneSwitcher": "Advanced Scene Switcher (macro: AdvancedSceneSwitcherMessage)",
    "aitum-vertical-canvas": "Aitum Vertical (scene verticali, stream/record verticale)",
    "move": "Move transition / Move value",
    "obs-shaderfilter": "Shader filter",
    "downstream-keyer": "Downstream Keyer",
    "source-record": "Source Record",
    "obs-browser": "Browser source (emit_event)",
}


@mcp.tool()
async def vendor_request(vendor: str | None = None, request: str | None = None, data: dict | None = None) -> dict:
    """Talk to OBS plugins that register obs-websocket vendor requests (Advanced Scene Switcher, Aitum Vertical,
    Downstream Keyer, Move, obs-browser 'emit_event', ...). Without vendor returns the list of common vendor names.
    Example: vendor='obs-browser', request='emit_event', data={'event_name': 'alert', 'event_data': {...}}."""
    if not vendor or not request:
        return {"known_vendors": KNOWN_VENDORS, "hint": "se il plugin non è installato OBS risponde 'vendor not found'"}
    r = await obs.call("CallVendorRequest", {"vendorName": vendor, "requestType": request, "requestData": data or {}}, timeout=30)
    return {"vendor": vendor, "request": request, "response": mask_secrets(r.get("responseData", r))}


@mcp.tool()
async def broadcast_event(data: dict) -> dict:
    """Send a custom event to every client connected to obs-websocket (overlays, Stream Deck, bots, your own pages).
    They receive it as 'CustomEvent'. Example: {'type': 'goal', 'value': 50}."""
    await obs.call("BroadcastCustomEvent", {"eventData": data})
    return {"done": True, "sent": data}


@mcp.tool()
async def scene_transition(scene: str, transition: str | None = None, duration_ms: int | None = None, clear: bool = False) -> dict:
    """Per-scene transition override: when OBS switches TO this scene it uses this transition/duration instead of the
    global one. Without transition/duration it just reads the override. clear=true removes it."""
    base = {"sceneName": scene}
    if clear:
        await obs.call("SetSceneSceneTransitionOverride", {**base, "transitionName": None, "transitionDuration": None}, keep_none=True)
    elif transition is not None or duration_ms is not None:
        d: dict[str, Any] = dict(base)
        if transition is not None:
            d["transitionName"] = transition
        if duration_ms is not None:
            d["transitionDuration"] = max(50, min(20000, int(duration_ms)))
        await obs.call("SetSceneSceneTransitionOverride", d)
    o = await obs.call("GetSceneSceneTransitionOverride", base)
    return {"scene": scene, "transition": o.get("transitionName"), "duration_ms": o.get("transitionDuration")}


@mcp.tool(annotations=RO)
async def source_info(name: str) -> dict:
    """Everything about one source in one call: kind, settings (secrets hidden), whether it is live on program/preview,
    which scenes use it (and if it is visible there), audio state, filters and media state."""
    lst = (await obs.call("GetInputList"))["inputs"]
    inp = next((i for i in lst if i["inputName"] == name), None)
    scenes = [s["sceneName"] for s in (await obs.call("GetSceneList"))["scenes"]]
    is_scene = name in scenes
    if not inp and not is_scene:
        near = [i["inputName"] for i in lst if name.lower() in i["inputName"].lower()]
        raise OBSError(f"sorgente '{name}' non trovata" + (f". Forse: {', '.join(near[:8])}" if near else ""))
    out: dict[str, Any] = {"name": name, "type": "scene" if is_scene and not inp else "input"}
    if inp:
        kind = inp.get("inputKind") or inp.get("unversionedInputKind")
        out["kind"] = kind
        out["settings"] = mask_secrets((await obs.call("GetInputSettings", {"inputName": name})).get("inputSettings", {}))
    act = await obs.try_call("GetSourceActive", {"sourceName": name}) or {}
    out["on_program"], out["showing"] = act.get("videoActive"), act.get("videoShowing")
    used = []
    for sc in scenes:
        items = (await obs.try_call("GetSceneItemList", {"sceneName": sc}) or {}).get("sceneItems", [])
        for i in items:
            if i["sourceName"] == name:
                used.append({"scene": sc, "id": i["sceneItemId"], "visible": i["sceneItemEnabled"]})
    out["used_in"] = used
    if inp:
        vol = await obs.try_call("GetInputVolume", {"inputName": name})
        if vol:
            mute = await obs.try_call("GetInputMute", {"inputName": name}) or {}
            out["audio"] = {"volume_db": round(vol["inputVolumeDb"], 1), "muted": mute.get("inputMuted")}
        med = await obs.try_call("GetMediaInputStatus", {"inputName": name})
        if med and med.get("mediaState") not in (None, "OBS_MEDIA_STATE_NONE"):
            out["media"] = {"state": str(med.get("mediaState")).replace("OBS_MEDIA_STATE_", "").lower(),
                            "position_s": round((med.get("mediaCursor") or 0) / 1000, 1), "length_s": round((med.get("mediaDuration") or 0) / 1000, 1)}
    flt = await obs.try_call("GetSourceFilterList", {"sourceName": name}) or {}
    out["filters"] = [{"name": f["filterName"], "kind": f["filterKind"], "enabled": f["filterEnabled"]} for f in flt.get("filters", [])]
    if not used and not is_scene:
        out["warning"] = "non è in nessuna scena: non si vede né si sente in uscita (a parte l'audio globale)"
    return out


@mcp.tool(annotations=RO)
async def stream_health(seconds: float = 5.0) -> dict:
    """Measures the live output for a few seconds: real bitrate (kbps), dropped/lagged frames in the window, congestion,
    CPU, render time and disk, for stream and recording. Ends with a verdict and what to change."""
    seconds = max(1.0, min(60.0, seconds))

    async def snap():
        res = await obs.batch([{"requestType": t} for t in ("GetStats", "GetStreamStatus", "GetRecordStatus")])
        return [(x.get("responseData") or {}) if x.get("requestStatus", {}).get("result") else {} for x in res]

    t0 = time.monotonic()
    a = await snap()
    await asyncio.sleep(seconds)
    b = await snap()
    dt = time.monotonic() - t0
    out: dict[str, Any] = {"seconds": round(dt, 1)}
    issues: list[str] = []
    for label, i in (("stream", 1), ("record", 2)):
        x, y = a[i], b[i]
        if not y.get("outputActive"):
            out[label] = {"active": False}
            continue
        kbps = max(0, (y.get("outputBytes", 0) - x.get("outputBytes", 0))) * 8 / 1000 / dt
        sk = max(0, y.get("outputSkippedFrames", 0) - x.get("outputSkippedFrames", 0))
        tot = max(1, y.get("outputTotalFrames", 0) - x.get("outputTotalFrames", 0))
        info = {"active": True, "kbps": round(kbps), "dropped_frames": sk, "dropped_percent": round(100 * sk / tot, 2)}
        if label == "stream":
            info["congestion"] = round(y.get("outputCongestion") or 0, 3)
            info["reconnecting"] = y.get("outputReconnecting")
            if y.get("outputReconnecting"):
                issues.append("la diretta si sta riconnettendo: rete instabile")
            if info["dropped_percent"] > 1 or info["congestion"] > 0.2:
                issues.append("frame persi in rete: abbassa il bitrate o usa il cavo al posto del Wi-Fi")
            if kbps < 500:
                issues.append("bitrate molto basso: controlla la connessione o il bitrate impostato")
        out[label] = info
    s0, s1 = a[0], b[0]
    rend = max(0, s1.get("renderSkippedFrames", 0) - s0.get("renderSkippedFrames", 0))
    rtot = max(1, s1.get("renderTotalFrames", 0) - s0.get("renderTotalFrames", 0))
    enc = max(0, s1.get("outputSkippedFrames", 0) - s0.get("outputSkippedFrames", 0))
    etot = max(1, s1.get("outputTotalFrames", 0) - s0.get("outputTotalFrames", 0))
    out["system"] = {"cpu_percent": round(s1.get("cpuUsage", 0), 1), "fps": round(s1.get("activeFps", 0), 1),
                     "render_ms": round(s1.get("averageFrameRenderTime", 0), 2),
                     "render_lag_percent": round(100 * rend / rtot, 2), "encoder_lag_percent": round(100 * enc / etot, 2),
                     "memory_mb": round(s1.get("memoryUsage", 0)), "disk_free_gb": round(s1.get("availableDiskSpace", 0) / 1024, 1)}
    sy = out["system"]
    if sy["render_lag_percent"] > 1:
        issues.append("la GPU non ce la fa (frame saltati in render): togli filtri/sorgenti pesanti o limita gli FPS del gioco")
    if sy["encoder_lag_percent"] > 1:
        issues.append("l'encoder è sovraccarico: usa NVENC, un preset più veloce o una risoluzione di uscita più bassa")
    if sy["cpu_percent"] > 85:
        issues.append("CPU quasi al massimo")
    if sy["disk_free_gb"] and sy["disk_free_gb"] < 10 and out.get("record", {}).get("active"):
        issues.append("meno di 10 GB liberi sul disco della registrazione")
    out["verdict"] = "ok" if not issues else "problemi"
    out["issues"] = issues
    return out


_countdowns: dict[str, asyncio.Task] = {}


def _fmt_left(sec: int, fmt: str) -> str:
    h, rem = divmod(max(0, sec), 3600)
    mnt, s = divmod(rem, 60)
    return fmt.replace("{h}", str(h)).replace("{mm}", f"{mnt:02d}").replace("{m}", str(mnt + 60 * h if "{h}" not in fmt else mnt)).replace("{ss}", f"{s:02d}")


@mcp.tool()
async def countdown(text_source: str, action: str = "start", seconds: int | None = None, until: str | None = None,
                    format: str = "{m}:{ss}", end_text: str = "", end_scene: str | None = None) -> dict:
    """Live countdown on a text source (e.g. 'starting soon'): updates every second in the background.
    seconds=300 or until='21:00' (local time). format: {h} {m} {mm} {ss}, e.g. 'Si parte tra {m}:{ss}'.
    At zero writes end_text and, if given, switches to end_scene. action: start | stop | status."""
    if action == "status":
        return {"running": sorted(k for k, t in _countdowns.items() if not t.done())}
    old = _countdowns.pop(text_source, None)
    if old and not old.done():
        old.cancel()
    if action == "stop":
        return {"done": True, "stopped": text_source if old else None}
    if action != "start":
        raise OBSError("action: start | stop | status")
    await obs.call("GetInputSettings", {"inputName": text_source})  # exists?
    if until:
        hh, mm = (int(x) for x in until.split(":")[:2])
        now = time.localtime()
        target = time.mktime((now.tm_year, now.tm_mon, now.tm_mday, hh, mm, 0, 0, 0, -1))
        if target <= time.time():
            target += 86400
    elif seconds:
        target = time.time() + int(seconds)
    else:
        raise OBSError("serve seconds o until")

    async def run():
        try:
            while True:
                left = int(round(target - time.time()))
                if left <= 0:
                    break
                await obs.try_call("SetInputSettings", {"inputName": text_source, "inputSettings": {"text": _fmt_left(left, format)}, "overlay": True})
                await asyncio.sleep(max(0.05, (target - time.time()) - (left - 1)))
            await obs.try_call("SetInputSettings", {"inputName": text_source, "inputSettings": {"text": end_text}, "overlay": True})
            if end_scene:
                await obs.try_call("SetCurrentProgramScene", {"sceneName": end_scene})
        except asyncio.CancelledError:
            pass

    _countdowns[text_source] = asyncio.create_task(run())
    left = int(round(target - time.time()))
    return {"done": True, "source": text_source, "seconds": left, "first_text": _fmt_left(left, format), "end_scene": end_scene}


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


@mcp.prompt()
def prepara_diretta(piattaforma: str = "Twitch") -> str:
    """Controllo completo prima di andare in diretta."""
    return (f"Sto per andare in diretta su {piattaforma} con OBS. 1) obs_status. 2) show_check con listen_seconds=3 "
            "mentre parlo al microfono. 3) screenshot del program per vedere cosa uscirà. 4) Elencami i problemi in ordine "
            "di gravità con la correzione, e correggi da solo quelli sicuri (volumi, filtri voce con clean_mic, file mancanti) "
            "chiedendomi prima di tutto ciò che cambia la destinazione o le impostazioni video. Non avviare la diretta: lo decido io.")


@mcp.prompt()
def controlla_diretta() -> str:
    """Controllo della diretta in corso, senza interromperla."""
    return ("La diretta OBS è in corso. stream_health con seconds=10, poi audio_levels per 3 secondi. Dimmi in due righe se va "
            "tutto bene; se no, la causa più probabile e la correzione. Non fermare stream o registrazione e non cambiare "
            "bitrate o encoder senza chiedermelo.")


@mcp.prompt()
def costruisci_scena(descrizione: str) -> str:
    """Costruisce una scena da una descrizione a parole."""
    return (f"Costruisci in OBS questa scena: {descrizione}. Prima list_inputs e list_input_kinds per riusare sorgenti "
            "esistenti (camera, microfono) invece di duplicarle. Poi un'unica chiamata build_scene con i layout preset. "
            "Infine screenshot della scena e correggi posizioni o dimensioni finché non è pulita.")


def main() -> None:
    if "--version" in sys.argv:
        print(VERSION)
        return
    if "--open" in sys.argv:  # uv run --script obs_mcp.py --open  -> apre OBS da solo, senza Claude
        args = sys.argv[sys.argv.index("--open") + 1:]
        kw = {"scene_collection": None, "scene": None, "profile": None}
        for flag, key in (("--collection", "scene_collection"), ("--scene", "scene"), ("--profile", "profile")):
            if flag in args and args.index(flag) + 1 < len(args):
                kw[key] = args[args.index(flag) + 1]
        try:
            print(json.dumps(asyncio.run(open_obs(**kw, minimize_to_tray="--tray" in args)), indent=2, ensure_ascii=False))
        except OBSError as e:
            print("errore:", e)
            sys.exit(1)
        return
    mcp.run()


if __name__ == "__main__":
    main()
