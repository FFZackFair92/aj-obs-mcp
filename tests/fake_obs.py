"""Finto OBS: un server obs-websocket v5 (con password) che simula scene, sorgenti, audio, filtri e uscite.
Serve a provare aj-obs-mcp senza OBS. Avvio: python tests/fake_obs.py [porta] [password]"""
from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import json
import os
import struct
import sys
import zlib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from obs_mcp import ws_read_frame, ws_write_frame  # noqa: E402


def tiny_png() -> str:
    raw = b"\x00\xff\x00\x00"
    def chunk(t, d):
        return struct.pack("!I", len(d)) + t + d + struct.pack("!I", zlib.crc32(t + d) & 0xFFFFFFFF)
    png = b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", struct.pack("!IIBBBBB", 1, 1, 8, 2, 0, 0, 0)) + chunk(b"IDAT", zlib.compress(raw)) + chunk(b"IEND", b"")
    return base64.b64encode(png).decode()


class Fail(Exception):
    def __init__(self, code, comment):
        self.code, self.comment = code, comment


AUDIO_KINDS = {"wasapi_input_capture", "wasapi_output_capture", "ffmpeg_source", "browser_source"}
KINDS = ["text_gdiplus_v3", "image_source", "ffmpeg_source", "browser_source", "color_source_v3", "monitor_capture",
         "window_capture", "game_capture", "dshow_input", "wasapi_input_capture", "wasapi_output_capture", "spout_capture"]
FILTER_KINDS = ["chroma_key_filter_v2", "color_key_filter_v2", "color_filter_v2", "noise_suppress_filter_v2", "noise_gate_filter",
                "compressor_filter", "limiter_filter", "gain_filter", "crop_filter", "scroll_filter", "mask_filter_v2"]


class State:
    def __init__(self):
        self.scenes = []  # list of {"name", "items": [item]} ; items bottom first
        self.inputs = {}
        self.next_id = 1
        self.program = None
        self.preview = None
        self.studio = False
        self.stream = False
        self.record = False
        self.paused = False
        self.replay = False
        self.vcam = False
        self.transition = "Dissolvenza"
        self.duration = 300
        self.video = {"baseWidth": 1920, "baseHeight": 1080, "outputWidth": 1280, "outputHeight": 720, "fpsNumerator": 60, "fpsDenominator": 1}
        self.profiles = ["Senza titolo", "Twitch 1080p"]
        self.profile = "Senza titolo"
        self.collections = ["Senza titolo", "Concerto"]
        self.collection = "Senza titolo"
        self.record_dir = "C:/Users/FABIO/Videos"
        self.service = {"streamServiceType": "rtmp_common", "streamServiceSettings": {"service": "Twitch", "server": "auto", "key": "live_SEGRETISSIMA"}}
        self.cursor = {}
        self.add_input("Mic/Aux", "wasapi_input_capture", {"device_id": "default"}, None)
        self.add_input("Audio desktop", "wasapi_output_capture", {}, None)
        self.create_scene("Scena")
        self.program = "Scena"
        self.add_input("Webcam", "dshow_input", {"video_device_id": "cam1"}, "Scena")
        self.add_input("Logo", "image_source", {"file": "/non/esiste/logo.png"}, "Scena")
        self.create_scene("Vuota")

    def scene(self, name):
        for s in self.scenes:
            if s["name"] == name:
                return s
        if name in self.inputs and self.inputs[name]["kind"] == "group":
            return self.inputs[name]
        raise Fail(600, f"No source was found by the name of `{name}`.")

    def create_scene(self, name):
        if any(s["name"] == name for s in self.scenes) or name in self.inputs:
            raise Fail(601, "A source already exists by that scene name.")
        self.scenes.append({"name": name, "items": [], "filters": []})

    def add_item(self, scene, source, enabled=True):
        sc = self.scene(scene)
        iid = self.next_id
        self.next_id += 1
        sc["items"].append({"sceneItemId": iid, "sourceName": source, "sceneItemEnabled": enabled, "sceneItemLocked": False,
                            "sceneItemBlendMode": "OBS_BLEND_NORMAL",
                            "sceneItemTransform": {"positionX": 0.0, "positionY": 0.0, "rotation": 0.0, "scaleX": 1.0, "scaleY": 1.0,
                                                   "alignment": 5, "boundsType": "OBS_BOUNDS_NONE", "boundsAlignment": 0, "boundsWidth": 0.0,
                                                   "boundsHeight": 0.0, "cropLeft": 0, "cropTop": 0, "cropRight": 0, "cropBottom": 0,
                                                   "sourceWidth": 1280.0, "sourceHeight": 720.0, "width": 1280.0, "height": 720.0}})
        return iid

    def add_input(self, name, kind, settings, scene, enabled=True):
        if name in self.inputs or any(s["name"] == name for s in self.scenes):
            raise Fail(601, "A source already exists by that input name.")
        if kind not in KINDS:
            raise Fail(605, "Your specified input kind is not supported by OBS.")
        self.inputs[name] = {"kind": kind, "settings": dict(settings or {}), "mul": 1.0, "muted": False, "balance": 0.5, "sync": 0,
                             "monitor": "OBS_MONITORING_TYPE_NONE", "tracks": {str(i): i == 1 for i in range(1, 7)}, "filters": []}
        return self.add_item(scene, name, enabled) if scene else None

    def source_filters(self, name):
        if name in self.inputs:
            return self.inputs[name]["filters"]
        return self.scene(name)["filters"]

    def item(self, scene, iid):
        for i in self.scene(scene)["items"]:
            if i["sceneItemId"] == iid:
                return i
        raise Fail(600, f"No scene items were found in scene `{scene}` with the ID `{iid}`.")

    def audio(self, name):
        if name not in self.inputs:
            raise Fail(600, f"No source was found by the name of `{name}`.")
        if self.inputs[name]["kind"] not in AUDIO_KINDS:
            raise Fail(604, "The specified input does not support audio.")
        return self.inputs[name]


class FakeOBS:
    def __init__(self, password="segreta"):
        self.password = password
        self.st = State()
        self.clients = []
        self.requests_seen = []

    async def handle(self, reader, writer):
        raw = await reader.readuntil(b"\r\n\r\n")
        key = [l.split(":", 1)[1].strip() for l in raw.decode().split("\r\n") if l.lower().startswith("sec-websocket-key")][0]
        acc = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        writer.write(f"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {acc}\r\n"
                     "Sec-WebSocket-Protocol: obswebsocket.json\r\n\r\n".encode())
        await writer.drain()
        client = {"w": writer, "subs": 0, "ok": False}
        salt, challenge = base64.b64encode(os.urandom(8)).decode(), base64.b64encode(os.urandom(8)).decode()
        hello = {"obsWebSocketVersion": "5.5.4", "rpcVersion": 1}
        if self.password:
            hello["authentication"] = {"salt": salt, "challenge": challenge}
        await self.send(client, 0, hello)
        meter_task = asyncio.create_task(self.meters(client))
        try:
            while True:
                op, fin, data = await ws_read_frame(reader)
                if op == 8:
                    break
                if op != 1:
                    continue
                msg = json.loads(data)
                d = msg["d"]
                if msg["op"] == 1:
                    if self.password:
                        secret = base64.b64encode(hashlib.sha256((self.password + salt).encode()).digest()).decode()
                        expect = base64.b64encode(hashlib.sha256((secret + challenge).encode()).digest()).decode()
                        if d.get("authentication") != expect:
                            await ws_write_frame(writer, 8, struct.pack("!H", 4009) + b"Authentication failed.", mask=False)
                            break
                    client["subs"], client["ok"] = d.get("eventSubscriptions", 0), True
                    self.clients.append(client)
                    await self.send(client, 2, {"negotiatedRpcVersion": 1})
                elif msg["op"] == 3:
                    client["subs"] = d.get("eventSubscriptions", client["subs"])
                    await self.send(client, 2, {"negotiatedRpcVersion": 1})
                elif msg["op"] == 6:
                    r = await self.run(d["requestType"], d.get("requestData") or {})
                    await self.send(client, 7, {"requestType": d["requestType"], "requestId": d["requestId"], **r})
                elif msg["op"] == 8:
                    results = []
                    for q in d["requests"]:
                        r = await self.run(q["requestType"], q.get("requestData") or {})
                        results.append({"requestType": q["requestType"], "requestId": q.get("requestId"), **r})
                        if d.get("haltOnFailure") and not r["requestStatus"]["result"]:
                            break
                    await self.send(client, 9, {"requestId": d["requestId"], "results": results})
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            meter_task.cancel()
            if client in self.clients:
                self.clients.remove(client)
            writer.close()

    async def send(self, client, op, d):
        await ws_write_frame(client["w"], 1, json.dumps({"op": op, "d": d}).encode(), mask=False)

    async def emit(self, etype, data, intent=1):
        for c in list(self.clients):
            if c["subs"] & intent:
                try:
                    await self.send(c, 5, {"eventType": etype, "eventIntent": intent, "eventData": data})
                except Exception:
                    pass

    async def meters(self, client):
        while True:
            await asyncio.sleep(0.05)
            if client["ok"] and client["subs"] & (1 << 16):
                ins = [{"inputName": n, "inputLevelsMul": [[0.1 * i["mul"], 0.3 * i["mul"], 0.3], [0.1 * i["mul"], 0.25 * i["mul"], 0.25]]}
                       for n, i in self.st.inputs.items() if i["kind"] in AUDIO_KINDS and not i["muted"]]
                await self.send(client, 5, {"eventType": "InputVolumeMeters", "eventIntent": 1 << 16, "eventData": {"inputs": ins}})

    async def run(self, t, d):
        self.requests_seen.append(t)
        try:
            fn = getattr(self, "r_" + t, None)
            if fn is None:
                raise Fail(204, f"Your request type is not valid: {t}")
            res = fn(d)
            if asyncio.iscoroutine(res):
                res = await res
            out = {"requestStatus": {"result": True, "code": 100}}
            if res:
                out["responseData"] = res
            return out
        except Fail as f:
            return {"requestStatus": {"result": False, "code": f.code, "comment": f.comment}}
        except KeyError as e:
            return {"requestStatus": {"result": False, "code": 300, "comment": f"Your request is missing the `{e.args[0]}` field."}}

    # ---- general
    def r_GetVersion(self, d):
        return {"obsVersion": "31.1.2", "obsWebSocketVersion": "5.5.4", "rpcVersion": 1, "platform": "windows", "platformDescription": "Windows 11 (finto)",
                "availableRequests": [], "supportedImageFormats": ["png", "jpg"]}

    def r_GetStats(self, d):
        return {"cpuUsage": 12.5, "memoryUsage": 512.0, "availableDiskSpace": 2048.0, "activeFps": 60.0, "averageFrameRenderTime": 3.2,
                "renderSkippedFrames": 0, "renderTotalFrames": 10000, "outputSkippedFrames": 0, "outputTotalFrames": 10000}

    def r_GetHotkeyList(self, d):
        return {"hotkeys": ["OBSBasic.Screenshot", "ReplayBuffer.Save"]}

    def r_TriggerHotkeyByName(self, d):
        if d["hotkeyName"] not in ("OBSBasic.Screenshot", "ReplayBuffer.Save"):
            raise Fail(600, "No hotkeys were found by that name.")

    def r_TriggerHotkeyByKeySequence(self, d):
        pass

    async def r_Sleep(self, d):
        await asyncio.sleep(d.get("sleepMillis", 0) / 1000)

    # ---- scenes
    def r_GetSceneList(self, d):
        n = len(self.st.scenes)
        return {"currentProgramSceneName": self.st.program, "currentPreviewSceneName": self.st.preview if self.st.studio else None,
                "scenes": [{"sceneName": s["name"], "sceneIndex": n - 1 - i} for i, s in enumerate(self.st.scenes)]}

    def r_GetCurrentProgramScene(self, d):
        return {"currentProgramSceneName": self.st.program, "sceneName": self.st.program}

    def r_GetCurrentPreviewScene(self, d):
        if not self.st.studio:
            raise Fail(506, "Studio mode is not active.")
        return {"currentPreviewSceneName": self.st.preview}

    async def r_SetCurrentProgramScene(self, d):
        self.st.scene(d["sceneName"])
        self.st.program = d["sceneName"]
        await self.emit("CurrentProgramSceneChanged", {"sceneName": d["sceneName"]}, 1 << 2)

    def r_SetCurrentPreviewScene(self, d):
        if not self.st.studio:
            raise Fail(506, "Studio mode is not active.")
        self.st.scene(d["sceneName"])
        self.st.preview = d["sceneName"]

    def r_CreateScene(self, d):
        self.st.create_scene(d["sceneName"])

    def r_RemoveScene(self, d):
        sc = self.st.scene(d["sceneName"])
        self.st.scenes.remove(sc)
        for s in self.st.scenes:
            s["items"] = [i for i in s["items"] if i["sourceName"] != d["sceneName"]]
        if self.st.program == d["sceneName"]:
            self.st.program = self.st.scenes[0]["name"] if self.st.scenes else None

    def r_SetSceneName(self, d):
        self.st.scene(d["sceneName"])["name"] = d["newSceneName"]
        if self.st.program == d["sceneName"]:
            self.st.program = d["newSceneName"]

    def _items(self, sc):
        out = []
        for idx, i in enumerate(sc["items"]):
            x = copy.deepcopy(i)
            x["sceneItemIndex"] = idx
            src = self.st.inputs.get(i["sourceName"])
            x["inputKind"] = src["kind"] if src else None
            x["isGroup"] = bool(src and src["kind"] == "group")
            x["sourceType"] = "OBS_SOURCE_TYPE_INPUT" if src else "OBS_SOURCE_TYPE_SCENE"
            out.append(x)
        return out

    def r_GetSceneItemList(self, d):
        return {"sceneItems": self._items(self.st.scene(d["sceneName"]))}

    def r_GetGroupSceneItemList(self, d):
        return {"sceneItems": self._items(self.st.scene(d["sceneName"]))}

    def r_GetSceneItemId(self, d):
        for i in self.st.scene(d["sceneName"])["items"]:
            if i["sourceName"] == d["sourceName"]:
                return {"sceneItemId": i["sceneItemId"]}
        raise Fail(600, "No scene items were found in the specified scene by that name or offset.")

    def r_CreateSceneItem(self, d):
        if d["sourceName"] not in self.st.inputs and not any(s["name"] == d["sourceName"] for s in self.st.scenes):
            raise Fail(600, "No source was found by the name of `%s`." % d["sourceName"])
        if d["sourceName"] == d["sceneName"]:
            raise Fail(601, "You cannot create scene item of a scene within itself.")
        return {"sceneItemId": self.st.add_item(d["sceneName"], d["sourceName"], d.get("sceneItemEnabled", True))}

    def r_RemoveSceneItem(self, d):
        sc = self.st.scene(d["sceneName"])
        sc["items"].remove(self.st.item(d["sceneName"], d["sceneItemId"]))

    def r_DuplicateSceneItem(self, d):
        i = self.st.item(d["sceneName"], d["sceneItemId"])
        return {"sceneItemId": self.st.add_item(d.get("destinationSceneName") or d["sceneName"], i["sourceName"], i["sceneItemEnabled"])}

    def r_GetSceneItemTransform(self, d):
        return {"sceneItemTransform": self.st.item(d["sceneName"], d["sceneItemId"])["sceneItemTransform"]}

    def r_SetSceneItemTransform(self, d):
        t = self.st.item(d["sceneName"], d["sceneItemId"])["sceneItemTransform"]
        for k, v in d["sceneItemTransform"].items():
            if k in ("boundsWidth", "boundsHeight") and v < 1:
                raise Fail(402, "boundsWidth/boundsHeight must be >= 1")
            if k not in t:
                continue
            t[k] = v
        t["width"] = t["sourceWidth"] * t["scaleX"]
        t["height"] = t["sourceHeight"] * t["scaleY"]
        if t["boundsType"] != "OBS_BOUNDS_NONE":
            t["width"], t["height"] = t["boundsWidth"], t["boundsHeight"]

    def r_GetSceneItemEnabled(self, d):
        return {"sceneItemEnabled": self.st.item(d["sceneName"], d["sceneItemId"])["sceneItemEnabled"]}

    def r_SetSceneItemEnabled(self, d):
        self.st.item(d["sceneName"], d["sceneItemId"])["sceneItemEnabled"] = d["sceneItemEnabled"]

    def r_SetSceneItemLocked(self, d):
        self.st.item(d["sceneName"], d["sceneItemId"])["sceneItemLocked"] = d["sceneItemLocked"]

    def r_GetSceneItemIndex(self, d):
        sc = self.st.scene(d["sceneName"])
        return {"sceneItemIndex": sc["items"].index(self.st.item(d["sceneName"], d["sceneItemId"]))}

    def r_SetSceneItemIndex(self, d):
        sc = self.st.scene(d["sceneName"])
        it = self.st.item(d["sceneName"], d["sceneItemId"])
        sc["items"].remove(it)
        sc["items"].insert(d["sceneItemIndex"], it)

    def r_SetSceneItemBlendMode(self, d):
        self.st.item(d["sceneName"], d["sceneItemId"])["sceneItemBlendMode"] = d["sceneItemBlendMode"]

    # ---- inputs
    def r_GetInputList(self, d):
        return {"inputs": [{"inputName": n, "inputKind": i["kind"], "unversionedInputKind": i["kind"].replace("_v3", "")} for n, i in self.st.inputs.items()
                           if not d.get("inputKind") or i["kind"] == d["inputKind"]]}

    def r_GetInputKindList(self, d):
        return {"inputKinds": KINDS}

    def r_GetSpecialInputs(self, d):
        return {"desktop1": "Audio desktop", "desktop2": None, "mic1": "Mic/Aux", "mic2": None}

    async def r_CreateInput(self, d):
        iid = self.st.add_input(d["inputName"], d["inputKind"], d.get("inputSettings"), d["sceneName"], d.get("sceneItemEnabled", True))
        await self.emit("InputCreated", {"inputName": d["inputName"], "inputKind": d["inputKind"]}, 1 << 3)
        return {"inputUuid": "uuid-" + d["inputName"], "sceneItemId": iid}

    def _inp(self, n):
        if n not in self.st.inputs:
            raise Fail(600, f"No source was found by the name of `{n}`.")
        return self.st.inputs[n]

    def r_RemoveInput(self, d):
        self._inp(d["inputName"])
        del self.st.inputs[d["inputName"]]
        for s in self.st.scenes:
            s["items"] = [i for i in s["items"] if i["sourceName"] != d["inputName"]]

    def r_SetInputName(self, d):
        self.st.inputs[d["newInputName"]] = self.st.inputs.pop(self._inp(d["inputName"]) and d["inputName"])
        for s in self.st.scenes:
            for i in s["items"]:
                if i["sourceName"] == d["inputName"]:
                    i["sourceName"] = d["newInputName"]

    def r_GetInputSettings(self, d):
        i = self._inp(d["inputName"])
        return {"inputSettings": i["settings"], "inputKind": i["kind"]}

    def r_GetInputDefaultSettings(self, d):
        return {"defaultInputSettings": {"text": "", "font": {"face": "Arial", "size": 256}} if "text" in d["inputKind"] else {}}

    def r_SetInputSettings(self, d):
        i = self._inp(d["inputName"])
        if d.get("overlay", True):
            i["settings"].update(d["inputSettings"])
        else:
            i["settings"] = dict(d["inputSettings"])

    def r_GetInputVolume(self, d):
        import math
        m = self.st.audio(d["inputName"])["mul"]
        return {"inputVolumeMul": m, "inputVolumeDb": 20 * math.log10(m) if m > 0 else float("-100")}

    def r_SetInputVolume(self, d):
        a = self.st.audio(d["inputName"])
        if "inputVolumeDb" in d:
            a["mul"] = 10 ** (d["inputVolumeDb"] / 20)
        else:
            a["mul"] = d["inputVolumeMul"]

    def r_GetInputMute(self, d):
        return {"inputMuted": self.st.audio(d["inputName"])["muted"]}

    async def r_SetInputMute(self, d):
        self.st.audio(d["inputName"])["muted"] = d["inputMuted"]
        await self.emit("InputMuteStateChanged", {"inputName": d["inputName"], "inputMuted": d["inputMuted"]}, 1 << 3)

    def r_ToggleInputMute(self, d):
        a = self.st.audio(d["inputName"])
        a["muted"] = not a["muted"]
        return {"inputMuted": a["muted"]}

    def r_GetInputAudioBalance(self, d):
        return {"inputAudioBalance": self.st.audio(d["inputName"])["balance"]}

    def r_SetInputAudioBalance(self, d):
        self.st.audio(d["inputName"])["balance"] = d["inputAudioBalance"]

    def r_GetInputAudioSyncOffset(self, d):
        return {"inputAudioSyncOffset": self.st.audio(d["inputName"])["sync"]}

    def r_SetInputAudioSyncOffset(self, d):
        self.st.audio(d["inputName"])["sync"] = d["inputAudioSyncOffset"]

    def r_GetInputAudioMonitorType(self, d):
        return {"monitorType": self.st.audio(d["inputName"])["monitor"]}

    def r_SetInputAudioMonitorType(self, d):
        self.st.audio(d["inputName"])["monitor"] = d["monitorType"]

    def r_GetInputAudioTracks(self, d):
        return {"inputAudioTracks": self.st.audio(d["inputName"])["tracks"]}

    def r_SetInputAudioTracks(self, d):
        self.st.audio(d["inputName"])["tracks"].update(d["inputAudioTracks"])

    def r_GetInputPropertiesListPropertyItems(self, d):
        self._inp(d["inputName"])
        return {"propertyItems": [{"itemName": "Logitech C920", "itemValue": "cam1", "itemEnabled": True}]}

    def r_PressInputPropertiesButton(self, d):
        self._inp(d["inputName"])

    # ---- filters
    def r_GetSourceFilterKindList(self, d):
        return {"sourceFilterKinds": FILTER_KINDS}

    def r_GetSourceFilterList(self, d):
        return {"filters": [dict(f, filterIndex=i) for i, f in enumerate(self.st.source_filters(d["sourceName"]))]}

    def _filter(self, d):
        for f in self.st.source_filters(d["sourceName"]):
            if f["filterName"] == d["filterName"]:
                return f
        raise Fail(600, "No filter was found in the source by that name.")

    def r_GetSourceFilter(self, d):
        return self._filter(d)

    def r_CreateSourceFilter(self, d):
        fl = self.st.source_filters(d["sourceName"])
        if d["filterKind"] not in FILTER_KINDS:
            raise Fail(605, "Your specified filter kind is not supported by OBS.")
        if any(f["filterName"] == d["filterName"] for f in fl):
            raise Fail(601, "A filter already exists by that name.")
        fl.append({"filterName": d["filterName"], "filterKind": d["filterKind"], "filterEnabled": True, "filterSettings": d.get("filterSettings") or {}})

    def r_RemoveSourceFilter(self, d):
        self.st.source_filters(d["sourceName"]).remove(self._filter(d))

    def r_SetSourceFilterEnabled(self, d):
        self._filter(d)["filterEnabled"] = d["filterEnabled"]

    def r_SetSourceFilterSettings(self, d):
        self._filter(d)["filterSettings"].update(d["filterSettings"])

    def r_SetSourceFilterIndex(self, d):
        fl = self.st.source_filters(d["sourceName"])
        f = self._filter(d)
        fl.remove(f)
        fl.insert(d["filterIndex"], f)

    def r_SetSourceFilterName(self, d):
        self._filter(d)["filterName"] = d["newFilterName"]

    # ---- transitions / studio
    def r_GetSceneTransitionList(self, d):
        return {"currentSceneTransitionName": self.st.transition, "currentSceneTransitionKind": "fade_transition",
                "transitions": [{"transitionName": "Dissolvenza", "transitionKind": "fade_transition", "transitionFixed": False},
                                {"transitionName": "Taglio", "transitionKind": "cut_transition", "transitionFixed": True}]}

    def r_GetCurrentSceneTransition(self, d):
        return {"transitionName": self.st.transition, "transitionDuration": self.st.duration}

    def r_SetCurrentSceneTransition(self, d):
        if d["transitionName"] not in ("Dissolvenza", "Taglio"):
            raise Fail(600, "No transition was found by that name.")
        self.st.transition = d["transitionName"]

    def r_SetCurrentSceneTransitionDuration(self, d):
        self.st.duration = d["transitionDuration"]

    def r_SetCurrentSceneTransitionSettings(self, d):
        pass

    def r_GetStudioModeEnabled(self, d):
        return {"studioModeEnabled": self.st.studio}

    def r_SetStudioModeEnabled(self, d):
        self.st.studio = d["studioModeEnabled"]
        self.st.preview = self.st.program if self.st.studio else None

    def r_TriggerStudioModeTransition(self, d):
        if not self.st.studio:
            raise Fail(506, "Studio mode is not active.")
        self.st.program, self.st.preview = self.st.preview, self.st.program

    def r_SetTBarPosition(self, d):
        if not self.st.studio:
            raise Fail(506, "Studio mode is not active.")

    # ---- outputs
    def r_GetStreamStatus(self, d):
        return {"outputActive": self.st.stream, "outputReconnecting": False, "outputTimecode": "00:01:02.000", "outputDuration": 62000,
                "outputCongestion": 0.0, "outputBytes": 1_000_000, "outputSkippedFrames": 0, "outputTotalFrames": 3600}

    async def r_StartStream(self, d):
        if self.st.stream:
            raise Fail(500, "The stream output is already active.")
        self.st.stream = True
        await self.emit("StreamStateChanged", {"outputActive": True, "outputState": "OBS_WEBSOCKET_OUTPUT_STARTED"}, 1 << 6)

    def r_StopStream(self, d):
        if not self.st.stream:
            raise Fail(501, "The stream output is not active.")
        self.st.stream = False

    def r_SendStreamCaption(self, d):
        if not self.st.stream:
            raise Fail(501, "The stream output is not active.")

    def r_GetRecordStatus(self, d):
        return {"outputActive": self.st.record, "outputPaused": self.st.paused, "outputTimecode": "00:00:10.000", "outputDuration": 10000, "outputBytes": 5_000_000}

    def r_StartRecord(self, d):
        self.st.record = True

    def r_StopRecord(self, d):
        if not self.st.record:
            raise Fail(501, "The record output is not active.")
        self.st.record = False
        return {"outputPath": self.st.record_dir + "/2026-11-19 02-30-00.mkv"}

    def r_PauseRecord(self, d):
        self.st.paused = True

    def r_ResumeRecord(self, d):
        self.st.paused = False

    def r_SplitRecordFile(self, d):
        pass

    def r_CreateRecordChapter(self, d):
        pass

    def r_GetReplayBufferStatus(self, d):
        return {"outputActive": self.st.replay}

    def r_StartReplayBuffer(self, d):
        self.st.replay = True

    def r_StopReplayBuffer(self, d):
        self.st.replay = False

    async def r_SaveReplayBuffer(self, d):
        if not self.st.replay:
            raise Fail(501, "Replay buffer is not active.")
        async def later():
            await asyncio.sleep(0.3)
            await self.emit("ReplayBufferSaved", {"savedReplayPath": self.st.record_dir + "/Replay 2026-11-19.mkv"}, 1 << 6)
        asyncio.create_task(later())

    def r_GetLastReplayBufferReplay(self, d):
        return {"savedReplayPath": self.st.record_dir + "/Replay 2026-11-19.mkv"}

    def r_GetVirtualCamStatus(self, d):
        return {"outputActive": self.st.vcam}

    def r_StartVirtualCam(self, d):
        self.st.vcam = True

    def r_StopVirtualCam(self, d):
        self.st.vcam = False

    def r_GetStreamServiceSettings(self, d):
        return copy.deepcopy(self.st.service)

    def r_SetStreamServiceSettings(self, d):
        self.st.service = {"streamServiceType": d["streamServiceType"], "streamServiceSettings": d["streamServiceSettings"]}

    # ---- media
    def r_GetMediaInputStatus(self, d):
        self._inp(d["inputName"])
        return {"mediaState": "OBS_MEDIA_STATE_PLAYING", "mediaDuration": 60000, "mediaCursor": self.st.cursor.get(d["inputName"], 1000)}

    def r_TriggerMediaInputAction(self, d):
        self._inp(d["inputName"])

    def r_SetMediaInputCursor(self, d):
        self.st.cursor[d["inputName"]] = d["mediaCursor"]

    def r_OffsetMediaInputCursor(self, d):
        self.st.cursor[d["inputName"]] = self.st.cursor.get(d["inputName"], 1000) + d["mediaCursorOffset"]

    # ---- screenshots
    def r_GetSourceScreenshot(self, d):
        if d["sourceName"] not in self.st.inputs and not any(s["name"] == d["sourceName"] for s in self.st.scenes):
            raise Fail(600, "No source was found.")
        if not 8 <= d.get("imageWidth", 8) <= 4096:
            raise Fail(402, "imageWidth out of range")
        return {"imageData": "data:image/png;base64," + tiny_png()}

    def r_SaveSourceScreenshot(self, d):
        Path(d["imageFilePath"]).write_bytes(base64.b64decode(tiny_png()))

    # ---- config
    def r_GetSceneCollectionList(self, d):
        return {"currentSceneCollectionName": self.st.collection, "sceneCollections": self.st.collections}

    def r_SetCurrentSceneCollection(self, d):
        if d["sceneCollectionName"] not in self.st.collections:
            raise Fail(600, "No scene collection was found by that name.")
        self.st.collection = d["sceneCollectionName"]

    def r_CreateSceneCollection(self, d):
        self.st.collections.append(d["sceneCollectionName"])
        self.st.collection = d["sceneCollectionName"]

    def r_GetProfileList(self, d):
        return {"currentProfileName": self.st.profile, "profiles": self.st.profiles}

    def r_SetCurrentProfile(self, d):
        if d["profileName"] not in self.st.profiles:
            raise Fail(600, "No profile was found by that name.")
        self.st.profile = d["profileName"]

    def r_GetVideoSettings(self, d):
        return dict(self.st.video)

    def r_SetVideoSettings(self, d):
        if self.st.stream or self.st.record:
            raise Fail(500, "Video settings cannot be changed while an output is active.")
        self.st.video.update({k: v for k, v in d.items() if v is not None})

    def r_GetRecordDirectory(self, d):
        return {"recordDirectory": self.st.record_dir}

    def r_SetRecordDirectory(self, d):
        self.st.record_dir = d["recordDirectory"]

    # ---- ui
    def r_GetMonitorList(self, d):
        return {"monitors": [{"monitorIndex": 0, "monitorName": "Display 1", "monitorWidth": 2560, "monitorHeight": 1440}]}

    def r_OpenVideoMixProjector(self, d):
        pass

    def r_OpenSourceProjector(self, d):
        self.st.scene(d["sourceName"]) if d["sourceName"] not in self.st.inputs else None

    def r_OpenInputPropertiesDialog(self, d):
        self._inp(d["inputName"])

    r_OpenInputFiltersDialog = r_OpenInputPropertiesDialog
    r_OpenInputInteractDialog = r_OpenInputPropertiesDialog


async def serve(port=4455, password="segreta"):
    fake = FakeOBS(password)
    server = await asyncio.start_server(fake.handle, "127.0.0.1", port)
    return fake, server


if __name__ == "__main__":
    async def main():
        port = int(sys.argv[1]) if len(sys.argv) > 1 else 4455
        pw = sys.argv[2] if len(sys.argv) > 2 else "segreta"
        _, server = await serve(port, pw)
        print(f"finto OBS su ws://127.0.0.1:{port} (password: {pw})")
        async with server:
            await server.serve_forever()
    asyncio.run(main())
