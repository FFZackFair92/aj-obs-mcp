# aj-obs-mcp

Server MCP per **OBS Studio**: Claude (o qualsiasi client MCP) gestisce scene, sorgenti, audio, filtri, diretta, registrazione, replay, camera virtuale e screenshot del program.

- **Niente plugin da installare**: usa il server obs-websocket v5 che OBS ha già dentro dalla versione 28.
- **Un solo file**, si avvia con `uv`. L'unica dipendenza è `mcp`: il client WebSocket è scritto con la libreria standard.
- **La password la trova da solo**: la legge dal file di configurazione di OBS (`obs-websocket/config.json`) se non imposti `OBS_PASSWORD`.
- **Prudente**: fermare la diretta o la registrazione, cancellare scene o sorgenti, cambiare destinazione, profilo, collezione o risoluzione richiede `confirm=true`. La chiave di streaming non viene mai mostrata.

## Installazione

1. In OBS: **Strumenti → Impostazioni server WebSocket → Abilita server WebSocket** (porta 4455, lascia la password attiva).
2. Aggiungi a `%APPDATA%\Claude\claude_desktop_config.json`:

```json
"obs": {
  "command": "uv",
  "args": ["run", "--script", "C:\\percorso\\aj-obs-mcp\\obs_mcp.py"]
}
```

3. Riavvia Claude Desktop.

Variabili opzionali: `OBS_URL` (default `ws://127.0.0.1:4455`), `OBS_PASSWORD`, `AJ_OBS_EXE` (percorso di `obs64.exe` per `open_obs`), `AJ_OBS_LOOKS` (dove salvare i look, default `~/.aj-mcp/obs-looks.json`).

Prova con OBS aperto: `uv run --script check_live.py` (crea una scena temporanea `_aj_check` e la cancella; non tocca diretta, registrazione né le tue scene).
Prova senza OBS: `uv run --with "mcp[cli]>=1.10,<3" python tests/test_offline.py` (finto OBS in `tests/fake_obs.py`).

## Tool (55) e prompt (2)

| Area | Tool |
|---|---|
| Generale | `obs_status`, `obs_request` (qualsiasi richiesta obs-websocket), `obs_batch` (più richieste in un colpo, con `Sleep` per sequenze a tempo o al frame), `obs_events` (cosa è successo, o aspetta un evento), `hotkey`, `open_obs` |
| Scene | `list_scenes`, `set_scene` (con transizione e durata), `create_scene`, `remove_scene`, `rename_scene`, `get_scene_items` |
| Elementi di scena | `add_to_scene`, `set_item_layout` (preset: full, pip_bottom_right, lower_third, left_half… o box in frazioni), `set_item_transform`, `set_item_state` (visibile, blocco, ordine, blend), `duplicate_item`, `remove_item` |
| Sorgenti | `list_inputs`, `list_input_kinds`, `create_input` (alias: text, image, media, browser, color, display, window, game, camera, mic, desktop_audio, app_audio, spout, ndi…), `get_input_settings`, `set_input_settings`, `set_text`, `rename_input`, `remove_input`, `list_property_items` (camere, finestre, mittenti Spout), `press_input_button` |
| Audio | `audio_mixer`, `set_audio` (dB, %, muto, bilanciamento, sync, monitor, tracce), `audio_levels` (ascolta i VU reali: “il microfono prende?”), `fade_audio` |
| Filtri | `list_filters`, `add_filter` (alias: chroma_key, color_correction, noise_suppress, compressor, limiter, scroll…), `set_filter`, `remove_filter`, `clean_mic` (catena voce in un colpo) |
| Transizioni | `transitions`, `studio_mode` (take e T-bar) |
| Uscite | `stream` (anche sottotitoli), `record` (pausa, split, capitoli, restituisce il file), `replay_buffer` (save restituisce il file), `virtual_cam`, `stream_service` |
| Media | `media_control` (play/pausa/seek/rimanente) |
| Vista | `screenshot` (program, preview o qualsiasi sorgente; Claude vede l'immagine) |
| Configurazione | `scene_collections`, `profiles`, `video_settings`, `record_directory` |
| Finestre | `open_projector` (program, preview, multiview, sorgente su monitor), `open_dialog` |
| Pipeline | `show_check` (controllo pre-diretta: fps, GPU, encoder, rete, disco, file mancanti, microfoni muti o silenziosi, scene vuote), `build_scene` (scena intera in una chiamata; se qualcosa fallisce non lascia niente a metà), `look` (salva e richiama visibilità + volumi, con dissolvenza audio) |

Prompt: `prepara_diretta`, `costruisci_scena`.

## Con il resto dell'hub

Il ponte con Resolume passa da Spout: in `resolume-mcp` c'è `obs_bridge`; qui `create_input(kind="spout", settings={"spoutsenders": "Arena"})` e `list_property_items(nome, "spoutsenders")` per scegliere il mittente (serve il plugin Spout2 for OBS).

## Confronto (novembre 2026)

- **obs-mcp (PyPI, 148 tool)**: copre tutto il protocollo con un tool per richiesta, ha `clean_audio_input` e `diagnose_av_health`; non ha i lotti di richieste. Qui gli stessi casi passano da `obs_request`/`obs_batch` più tool pensati per il lavoro vero (layout preset, `build_scene` con annullamento, look, livelli audio reali, fade, conferme e chiave nascosta).
- **royshil/obs-mcp (npm)**: generale, scene, sorgenti, elementi, stream/registrazione, transizioni; niente screenshot, filtri o audio dettagliato.
- **obs-studio-mcp (PyPI)**, **obs_mcp (Dart)**, **obs-controller**: coperture simili, nessuno ha lotti con `Sleep`, eventi in attesa, livelli audio o pipeline con annullamento.
