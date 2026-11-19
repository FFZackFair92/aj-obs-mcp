"""Prova tutti i tool di aj-obs-mcp contro il finto OBS (nessun OBS necessario).
Avvio: uv run --with "mcp[cli]>=1.10,<3" python tests/test_offline.py"""
from __future__ import annotations

import asyncio
import os
import socket
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

s = socket.socket()
s.bind(("127.0.0.1", 0))
PORT = s.getsockname()[1]
s.close()
os.environ["OBS_URL"] = f"ws://127.0.0.1:{PORT}"
os.environ["OBS_PASSWORD"] = "segreta"
os.environ["AJ_OBS_LOOKS"] = str(Path(tempfile.mkdtemp()) / "looks.json")

import obs_mcp as m  # noqa: E402
import fake_obs  # noqa: E402

ok = 0
fail = 0


def check(name, cond, info=""):
    global ok, fail
    if cond:
        ok += 1
        print(f"  ok   {name}")
    else:
        fail += 1
        print(f"  FAIL {name} {info}")


async def raises(coro, text=""):
    try:
        await coro
    except Exception as e:
        return text.lower() in str(e).lower()
    return False


async def main():
    fake, server = await fake_obs.serve(PORT, "segreta")
    st = fake.st

    print("connessione")
    s = await m.obs_status()
    check("obs_status", s["obs"] == "31.1.2" and s["program_scene"] == "Scena" and s["canvas"] == "1920x1080", s)

    bad = m.OBS()
    bad.password = "sbagliata"
    check("password sbagliata spiegata", await raises(bad.call("GetVersion"), "password sbagliata"))
    nobody = m.OBS()
    nobody.url = "ws://127.0.0.1:1"
    check("OBS spento spiegato", await raises(nobody.call("GetVersion"), "WebSocket"))

    print("generali")
    r = await m.obs_request("GetVersion")
    check("obs_request", r["obsWebSocketVersion"] == "5.5.4")
    r = await m.obs_request("RemoveScene", {"sceneName": "Vuota"})
    check("obs_request pericolosa chiede conferma", r.get("needs_confirm") and any(x["name"] == "Vuota" for x in st.scenes))
    r = await m.obs_request("GetStreamServiceSettings")
    check("chiave stream nascosta in obs_request", "SEGRETISSIMA" not in str(r))
    r = await m.obs_batch([{"requestType": "GetVersion"}, {"requestType": "Sleep", "requestData": {"sleepMillis": 50}}, {"requestType": "Nope"}])
    check("obs_batch", r["count"] == 3 and not r["ok"] and r["results"][2]["code"] == 204, r)
    check("obs_request tipo sconosciuto", await raises(m.obs_request("Nope"), "not valid"))
    r = await m.hotkey()
    check("hotkey lista", "OBSBasic.Screenshot" in r["hotkeys"])
    r = await m.hotkey(name="ReplayBuffer.Save")
    check("hotkey per nome", r["done"])

    print("scene")
    r = await m.list_scenes()
    check("list_scenes", r["scenes"] == ["Scena", "Vuota"] and r["program"] == "Scena", r)
    await m.create_scene("Intro")
    r = await m.set_scene("Intro", transition="Taglio", duration_ms=500)
    check("set_scene + transizione", st.program == "Intro" and st.transition == "Taglio" and st.duration == 500)
    ev = await m.obs_events(types=["CurrentProgramSceneChanged"])
    check("obs_events riceve il cambio scena", ev["events"] and ev["events"][-1]["data"]["sceneName"] == "Intro", ev)
    await m.rename_scene("Intro", "Apertura")
    check("rename_scene", st.program == "Apertura")
    r = await m.remove_scene("Apertura")
    check("remove_scene senza conferma", r.get("needs_confirm"))
    await m.remove_scene("Apertura", confirm=True)
    check("remove_scene", not any(x["name"] == "Apertura" for x in st.scenes))
    await m.set_scene("Scena")

    r = await m.get_scene_items()
    check("get_scene_items", [i["source"] for i in r["items"]] == ["Logo", "Webcam"], r)

    print("elementi di scena")
    r = await m.set_item_layout("Webcam", preset="pip_bottom_right")
    t = st.item("Scena", r and (await m.item_id("Scena", "Webcam")))["sceneItemTransform"]
    check("set_item_layout preset", t["boundsType"] == "OBS_BOUNDS_SCALE_INNER" and round(t["positionX"]) == 1344, t)
    await m.set_item_layout("Logo", box=[0.5, 0.5, 0.25, 0.25], mode="stretch")
    t = st.item("Scena", await m.item_id("Scena", "Logo"))["sceneItemTransform"]
    check("set_item_layout box frazioni", t["positionX"] == 960 and t["boundsWidth"] == 480, t)
    check("preset sbagliato spiegato", await raises(m.set_item_layout("Logo", preset="boh"), "preset sconosciuto"))
    check("sorgente assente spiegata", await raises(m.set_item_layout("Nessuno", preset="full"), "Elementi presenti"))
    r = await m.set_item_transform("Webcam", clear_bounds=True, x=10, y=20, width=640, crop=[1, 2, 3, 4], alignment="top_left")
    t = st.item("Scena", await m.item_id("Scena", "Webcam"))["sceneItemTransform"]
    check("set_item_transform", t["positionX"] == 10 and t["scaleX"] == 0.5 and t["scaleY"] == 0.5 and t["cropBottom"] == 4, t)
    await m.set_item_state("Logo", visible=False)
    check("set_item_state nascondi", not st.item("Scena", await m.item_id("Scena", "Logo"))["sceneItemEnabled"])
    await m.set_item_state("Logo", toggle=True, locked=True, index="bottom", blend="screen")
    it = st.item("Scena", await m.item_id("Scena", "Logo"))
    check("set_item_state toggle/lock/indice/blend", it["sceneItemEnabled"] and it["sceneItemLocked"] and st.scene("Scena")["items"][0] is it
          and it["sceneItemBlendMode"] == "OBS_BLEND_SCREEN")
    r = await m.duplicate_item("Webcam", to_scene="Vuota")
    check("duplicate_item", [i["sourceName"] for i in st.scene("Vuota")["items"]] == ["Webcam"])
    r = await m.add_to_scene("Vuota", scene="Scena", layout="top_left")
    check("add_to_scene scena annidata", any(i["sourceName"] == "Vuota" for i in st.scene("Scena")["items"]))
    r = await m.remove_item("Vuota", scene="Scena", confirm=True)
    check("remove_item", not any(i["sourceName"] == "Vuota" for i in st.scene("Scena")["items"]))

    print("sorgenti")
    r = await m.list_input_kinds()
    check("list_input_kinds alias", r["aliases"]["text"] == "text_gdiplus_v3" and r["aliases"]["camera"] == "dshow_input")
    r = await m.create_input("Titolo", "testo", settings={"text": "CIAO", "color": "#FF0000"}, layout="lower_third")
    check("create_input con alias e colore", st.inputs["Titolo"]["kind"] == "text_gdiplus_v3" and st.inputs["Titolo"]["settings"]["color"] == 0xFF0000FF, st.inputs["Titolo"])
    await m.create_input("Clip", "media", settings={"file": "C:/v.mp4", "looping": True})
    check("media: file -> local_file", st.inputs["Clip"]["settings"] == {"local_file": "C:/v.mp4", "is_local_file": True, "looping": True})
    await m.create_input("Fondo", "color", settings={"color": "#101018"})
    check("color: dimensioni del canvas", st.inputs["Fondo"]["settings"]["width"] == 1920)
    check("alias sconosciuto spiegato", await raises(m.create_input("X", "boh"), "sconosciuto"))
    check("tipo senza plugin spiegato", await raises(m.create_input("X", "ndi"), "plugin"))
    r = await m.list_inputs(kind="text")
    check("list_inputs filtro", [i["name"] for i in r["inputs"]] == ["Titolo"] and r["special_audio"]["mic1"] == "Mic/Aux")
    await m.set_text("Titolo", "IN DIRETTA")
    check("set_text", st.inputs["Titolo"]["settings"]["text"] == "IN DIRETTA" and st.inputs["Titolo"]["settings"]["color"] == 0xFF0000FF)
    r = await m.get_input_settings("Titolo", with_defaults=True)
    check("get_input_settings + default", r["settings"]["text"] == "IN DIRETTA" and "font" in r["defaults"])
    await m.set_input_settings("Clip", {"file": "D:/nuovo.mp4"})
    check("set_input_settings normalizza", st.inputs["Clip"]["settings"]["local_file"] == "D:/nuovo.mp4")
    await m.rename_input("Clip", "Video")
    check("rename_input", "Video" in st.inputs and "Clip" not in st.inputs)
    r = await m.list_property_items("Webcam", "video_device_id")
    check("list_property_items", r["items"][0]["value"] == "cam1")
    r = await m.press_input_button("Webcam", "activate")
    check("press_input_button", r["done"])
    r = await m.remove_input("Fondo")
    check("remove_input chiede conferma", r.get("needs_confirm") and "Fondo" in st.inputs)
    await m.remove_input("Fondo", confirm=True)
    check("remove_input", "Fondo" not in st.inputs)

    print("audio")
    r = await m.audio_mixer()
    names = [a["name"] for a in r["audio"]]
    check("audio_mixer", names == ["Mic/Aux", "Audio desktop", "Video"] and r["audio"][0]["tracks"] == [1], r)
    r = await m.set_audio("Mic/Aux", volume_db=-6, balance=0.3, sync_ms=120, monitor="monitor_only", tracks=[1, 3])
    a = st.inputs["Mic/Aux"]
    check("set_audio", r["volume_db"] == -6 and a["balance"] == 0.3 and a["sync"] == 120 and a["monitor"] == "OBS_MONITORING_TYPE_MONITOR_ONLY"
          and a["tracks"]["3"] and not a["tracks"]["2"], r)
    r = await m.set_audio("Mic/Aux", toggle_mute=True)
    check("set_audio toggle mute", r["muted"])
    await m.set_audio("Mic/Aux", muted=False, volume_percent=100)
    check("set_audio percentuale", abs(st.inputs["Mic/Aux"]["mul"] - 1) < 1e-6)
    check("set_audio su sorgente non audio", await raises(m.set_audio("Webcam", volume_db=0), "audio"))
    r = await m.audio_levels(0.5)
    lv = {l["name"]: l for l in r["levels"]}
    check("audio_levels", "Mic/Aux" in lv and -11 < lv["Mic/Aux"]["peak_db"] < -10 and lv["Mic/Aux"]["verdict"] == "ok", r)
    check("audio_levels ripristina gli eventi", not (m.obs.subs & m.EV_VOLUME_METERS))
    r = await m.fade_audio("Audio desktop", -30, seconds=0.3)
    check("fade_audio", abs(20 * __import__("math").log10(st.inputs["Audio desktop"]["mul"]) + 30) < 0.01, r)
    await m.fade_audio("Audio desktop", -100, seconds=0.2, mute_at_end=True)
    check("fade_audio a zero + muto", st.inputs["Audio desktop"]["muted"])
    await m.fade_audio("Audio desktop", 0, seconds=0.2)
    check("fade_audio da muto riattiva", not st.inputs["Audio desktop"]["muted"] and abs(st.inputs["Audio desktop"]["mul"] - 1) < 1e-6)

    print("filtri")
    r = await m.add_filter("Webcam", "chroma_key", settings={"key_color_type": "custom", "key_color": "#00FF00"})
    check("add_filter alias", st.inputs["Webcam"]["filters"][0]["filterKind"] == "chroma_key_filter_v2"
          and st.inputs["Webcam"]["filters"][0]["filterSettings"]["key_color"] == 0xFF00FF00)
    await m.add_filter("Webcam", "color_correction", name="B/N", settings={"saturation": -1}, index=0)
    check("add_filter con indice", st.inputs["Webcam"]["filters"][0]["filterName"] == "B/N")
    await m.set_filter("Webcam", "B/N", toggle=True, settings={"contrast": 0.2}, index=1, new_name="Bianco e nero")
    f = st.inputs["Webcam"]["filters"][1]
    check("set_filter", f["filterName"] == "Bianco e nero" and not f["filterEnabled"] and f["filterSettings"]["contrast"] == 0.2)
    r = await m.list_filters("Webcam")
    check("list_filters", [x["name"] for x in r["filters"]] == ["Chroma Key", "Bianco e nero"])
    await m.remove_filter("Webcam", "Chroma Key")
    check("remove_filter", len(st.inputs["Webcam"]["filters"]) == 1)
    await m.add_filter("Scena", "crop")
    check("filtro su scena", st.scene("Scena")["filters"][0]["filterKind"] == "crop_filter")
    st.inputs["Mic/Aux"]["filters"].append({"filterName": "Mio gate", "filterKind": "noise_gate_filter", "filterEnabled": True, "filterSettings": {}})
    r = await m.clean_mic("Mic/Aux")
    check("clean_mic salta l'esistente", r["added"] == ["Riduzione rumore", "Compressore", "Limiter"] and r["skipped_existing"] == ["Noise gate"], r)
    r = await m.clean_mic("Mic/Aux")
    check("clean_mic ripetuto non duplica", r["added"] == [])

    print("transizioni e studio")
    r = await m.transitions("Dissolvenza", 800)
    check("transitions", r["current"] == "Dissolvenza" and r["duration_ms"] == 800 and len(r["transitions"]) == 2)
    check("transizione sbagliata", await raises(m.transitions("Boh"), "transition"))
    r = await m.studio_mode(True)
    check("studio_mode on", r["studio_mode"] and r["preview"] == "Scena")
    await m.set_scene("Vuota", preview=True)
    r = await m.studio_mode(take=True, tbar=0.5, release=False)
    check("studio_mode take", r["program"] == "Vuota" and r["preview"] == "Scena", r)
    await m.studio_mode(False)
    await m.set_scene("Scena")

    print("uscite")
    r = await m.stream("start")
    check("stream start", r["active"] and r["mb_sent"] == 1.0)
    ev = await m.obs_events(types=["StreamStateChanged"], wait_seconds=1)
    check("evento StreamStateChanged", ev["events"])
    r = await m.stream("caption", caption="ciao")
    check("stream caption", r["active"])
    r = await m.stream("stop")
    check("stream stop chiede conferma", r.get("needs_confirm") and st.stream)
    r = await m.stream("stop", confirm=True)
    check("stream stop", not r["active"])
    await m.record("start")
    await m.record("pause")
    check("record pause", st.paused)
    await m.record("resume")
    await m.record("chapter", chapter="Brano 2")
    await m.record("split")
    r = await m.record("stop", confirm=True)
    check("record stop restituisce il file", r["file"].endswith(".mkv") and not r["active"], r)
    await m.replay_buffer("start")
    r = await m.replay_buffer("save")
    check("replay_buffer save -> file", r["file"] and "Replay" in r["file"], r)
    await m.replay_buffer("stop")
    r = await m.virtual_cam("start")
    check("virtual_cam", r["active"])
    await m.virtual_cam("stop")
    r = await m.stream_service()
    check("stream_service nasconde la chiave", "SEGRETISSIMA" not in str(r) and r["settings"]["service"] == "Twitch")
    r = await m.stream_service(server="rtmp://a.b/live", key="NUOVA")
    check("stream_service chiede conferma", r.get("needs_confirm"))
    r = await m.stream_service(server="rtmp://a.b/live", key="NUOVA", confirm=True)
    check("stream_service cambia", st.service["streamServiceType"] == "rtmp_custom" and st.service["streamServiceSettings"]["key"] == "NUOVA" and "NUOVA" not in str(r))

    print("media e screenshot")
    r = await m.media_control("Video", "play", seek_seconds=30)
    check("media_control", r["state"] == "playing" and r["position_s"] == 30 and r["remaining_s"] == 30, r)
    r = await m.media_control("Video", offset_seconds=-10)
    check("media_control offset", r["position_s"] == 20)
    out = Path(tempfile.mkdtemp()) / "shot.png"
    r = await m.screenshot(save_path=str(out), format="png")
    check("screenshot", isinstance(r[0], m.Image) and out.exists() and "Scena" in r[1])
    res = await m.mcp.call_tool("screenshot", {"source": "Webcam", "width": 640})
    check("screenshot via MCP (contenuto immagine)", "image" in str(res).lower(), str(res)[:200])

    print("configurazione e UI")
    r = await m.scene_collections()
    check("scene_collections", r["current"] == "Senza titolo" and "Concerto" in r["collections"])
    r = await m.scene_collections("switch", "Concerto")
    check("scene_collections conferma", r.get("needs_confirm"))
    r = await m.scene_collections("switch", "Concerto", confirm=True)
    check("scene_collections switch", r["current"] == "Concerto")
    r = await m.profiles("switch", "Twitch 1080p", confirm=True)
    check("profiles", r["current"] == "Twitch 1080p")
    r = await m.video_settings(fps=29.97, output_width=1920, output_height=1080, confirm=True)
    check("video_settings", r["fps"] == 29.97 and r["output"] == [1920, 1080], r)
    await m.video_settings(fps=60, output_width=1280, output_height=720, confirm=True)
    r = await m.record_directory("D:/Registrazioni")
    check("record_directory", r["folder"] == "D:/Registrazioni")
    r = await m.open_projector("multiview")
    check("open_projector", r["monitors"][0]["size"] == "2560x1440")
    r = await m.open_dialog("Webcam", "filters")
    check("open_dialog", r["done"])

    print("pipeline")
    st.inputs["Mic/Aux"]["muted"] = True
    r = await m.show_check(listen_seconds=0.3)
    whats = " | ".join(p["what"] for p in r["problems"])
    check("show_check trova file mancante", "logo.png" in whats and "C:/v.mp4" not in whats, whats)  # Video now at D:/nuovo.mp4 (missing)
    check("show_check trova mic in muto", "in muto" in whats, whats)
    check("show_check trova poco disco", "GB liberi" in whats, whats)
    check("show_check verdetto", r["verdict"] == "NON pronto", r["verdict"])
    st.inputs["Mic/Aux"]["muted"] = False

    r = await m.build_scene("Live", [
        {"name": "Sfondo", "kind": "color", "settings": {"color": "#101018"}},
        {"name": "Webcam", "layout": "pip_bottom_right"},
        {"name": "Sottotitolo", "kind": "text", "settings": {"text": "AJ"}, "layout": "lower_third", "filters": [{"kind": "scroll", "settings": {"speed_x": 50}}]},
        {"name": "Scena", "kind": "scene", "visible": False},
    ], switch_to=True)
    names = [i["sourceName"] for i in st.scene("Live")["items"]]
    check("build_scene", names == ["Sfondo", "Webcam", "Sottotitolo", "Scena"] and st.program == "Live"
          and st.inputs["Sottotitolo"]["filters"][0]["filterKind"] == "scroll_filter", names)
    before = set(st.inputs)
    ok_rb = await raises(m.build_scene("Rotta", [{"name": "A", "kind": "text"}, {"name": "B", "kind": "ndi"}]), "annullato")
    check("build_scene annulla tutto se fallisce", ok_rb and set(st.inputs) == before and not any(s_["name"] == "Rotta" for s_ in st.scenes))
    check("build_scene non sovrascrive", await raises(m.build_scene("Live", []), "esiste già"))
    r = await m.build_scene("Live", [{"name": "Webcam"}], replace=True)
    check("build_scene replace chiede conferma", r.get("needs_confirm"))

    r = await m.look("save", "Tutto acceso")
    check("look save", r["scenes"] >= 3 and r["audio"] == 3, r)
    await m.set_item_state("Webcam", scene="Live", visible=False)
    await m.set_audio("Mic/Aux", volume_db=-40, muted=True)
    r = await m.look("apply", "Tutto acceso")
    check("look apply", st.item("Live", await m.item_id("Live", "Webcam"))["sceneItemEnabled"] and not st.inputs["Mic/Aux"]["muted"]
          and abs(st.inputs["Mic/Aux"]["mul"] - 1) < 0.01, r)
    await m.set_audio("Mic/Aux", volume_db=-40)
    await m.look("apply", "Tutto acceso", fade_seconds=0.3)
    check("look apply con fade", abs(st.inputs["Mic/Aux"]["mul"] - 1) < 0.01)
    r = await m.look("list")
    check("look list", "Tutto acceso" in r["looks"])
    await m.look("delete", "Tutto acceso")
    check("look delete", "Tutto acceso" not in (await m.look("list"))["looks"])

    print("server MCP")
    tools = await m.mcp.list_tools()
    names = sorted(t.name for t in tools)
    check(f"{len(names)} tool registrati", len(names) >= 50, names)
    prompts = await m.mcp.list_prompts()
    check("prompt", {p.name for p in prompts} == {"prepara_diretta", "costruisci_scena"})
    ro = {t.name for t in tools if t.annotations and (getattr(t.annotations, "readOnlyHint", None) or getattr(t.annotations, "read_only_hint", None))}
    check("tool in sola lettura marcati", {"obs_status", "screenshot", "audio_mixer", "show_check"} <= ro)

    print("riconnessione")
    for c in list(fake.clients):
        c["w"].close()
    await asyncio.sleep(0.2)
    r = await m.obs_status()
    check("si riconnette da solo se OBS si riavvia", r["obs"] == "31.1.2")

    server.close()
    print(f"\n{ok} ok, {fail} falliti")
    sys.exit(1 if fail else 0)


asyncio.run(main())
