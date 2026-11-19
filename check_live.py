# /// script
# requires-python = ">=3.10"
# dependencies = ["mcp[cli]>=1.10,<3"]
# ///
"""Prova aj-obs-mcp contro il TUO OBS aperto. Non tocca diretta, registrazione né scene esistenti:
crea una scena temporanea '_aj_check', ci lavora e la cancella.
Avvio: uv run --script check_live.py"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import obs_mcp as m  # noqa: E402

TMP = "_aj_check"
results = []


async def step(name, coro):
    try:
        r = await coro
        results.append((True, name))
        print(f"  ok   {name}")
        return r
    except Exception as e:
        results.append((False, name))
        print(f"  FAIL {name}: {e}")


async def main():
    print(f"OBS su {m.obs.url}")
    s = await step("obs_status", m.obs_status())
    if not s:
        print("\nOBS non raggiungibile: aprilo e attiva Strumenti > Impostazioni server WebSocket.")
        return
    print(f"       OBS {s['obs']}, websocket {s['obs_websocket']}, scena '{s['program_scene']}', {s['canvas']} @ {s['fps_now']} fps")
    for name, c in [("list_scenes", m.list_scenes()), ("get_scene_items", m.get_scene_items()), ("list_inputs", m.list_inputs()),
                    ("list_input_kinds", m.list_input_kinds()), ("audio_mixer", m.audio_mixer()), ("transitions", m.transitions()),
                    ("scene_collections", m.scene_collections()), ("profiles", m.profiles()), ("video_settings", m.video_settings()),
                    ("record_directory", m.record_directory()), ("stream status", m.stream()), ("record status", m.record()),
                    ("replay_buffer status", m.replay_buffer()), ("virtual_cam status", m.virtual_cam()),
                    ("stream_service (chiave nascosta)", m.stream_service()), ("hotkey list", m.hotkey()),
                    ("audio_levels 2s", m.audio_levels(2)), ("obs_events", m.obs_events())]:
        await step(name, c)

    scenes = (await m.list_scenes())["scenes"]
    if TMP in scenes:
        await m.remove_scene(TMP, confirm=True)
    kinds = (await m.list_input_kinds())["aliases"]
    print("\nscena temporanea")
    items = [{"name": "_aj_fondo", "kind": "color", "settings": {"color": "#1E1E3C"}},
             {"name": "_aj_testo", "kind": "text", "settings": {"text": "aj-obs-mcp OK", "color": "#FFFFFF"}, "layout": "center",
              "filters": [{"kind": "color_correction", "settings": {"gamma": 0.1}}]}]
    await step("build_scene", m.build_scene(TMP, items))
    await step("set_item_layout", m.set_item_layout("_aj_testo", scene=TMP, preset="lower_third"))
    await step("set_item_transform", m.set_item_transform("_aj_testo", scene=TMP, rotation=2))
    await step("set_item_state", m.set_item_state("_aj_testo", scene=TMP, toggle=True))
    await step("set_item_state (di nuovo)", m.set_item_state("_aj_testo", scene=TMP, toggle=True, index="top", blend="normal"))
    await step("set_text", m.set_text("_aj_testo", "testo cambiato"))
    await step("add_filter", m.add_filter("_aj_testo", "scroll", settings={"speed_x": 30}))
    await step("set_filter", m.set_filter("_aj_testo", "Scroll", toggle=True))
    await step("list_filters", m.list_filters("_aj_testo"))
    await step("get_scene_items", m.get_scene_items(TMP))
    shot = await step("screenshot", m.screenshot(TMP, width=640))
    if shot:
        out = Path(__file__).with_name("check_live_screenshot.jpg")
        out.write_bytes(shot[0].data)
        print(f"       salvato {out.name}: aprilo e controlla che si veda il testo")
    await step("look save", m.look("save", "_aj_check", scene=TMP, include_audio=False))
    await step("look apply", m.look("apply", "_aj_check", include_audio=False))
    await step("look delete", m.look("delete", "_aj_check"))
    if "mic" in kinds:
        await step("show_check", m.show_check())

    print("\npulizia")
    await step("remove_scene", m.remove_scene(TMP, confirm=True))
    for n in ("_aj_fondo", "_aj_testo"):
        await step(f"remove_input {n}", m.remove_input(n, confirm=True))

    bad = sum(not r[0] for r in results)
    print(f"\n{len(results) - bad} ok, {bad} falliti")


asyncio.run(main())
