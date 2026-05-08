# Plan: bandcamp-warden Mac Firefox Extension

Stand: 2026-05-08, nach 5 Tagen empirischer Bestätigung dass jede Form von Server-Side-HTTP-Mimicry (curl_cffi, headers, TLS-fingerprint, Range-resume, warmup-browse, Playwright headless) auf ~7 KB/s gedrosselt wird. Der User's normale Firefox auf demselben Mac bekommt 39 MB/s. Die Drosselung hängt nicht am Code-Pfad, sondern an Bandcamp's Erkennung von "non-real-browser-context".

Diese Extension ist die Antwort: läuft im realen Firefox des Users, behält ihren Browser-Identity, lädt im Hintergrund ohne UI-Interaktion.

## Scope

**Was die Extension macht:**
- Iteriert die Bandcamp-Collection des Users
- Skippt bereits heruntergeladene Items
- Lädt N Items pro Tag mit menschen-ähnlichem Pacing
- Persistiert ihre Queue über Browser-Restarts hinweg
- Macht Auto-Resume + Auto-Retry bei Fehlern
- Zeigt minimalen Status (Toolbar-Badge mit Tageszähler)
- Schreibt Logs in `browser.storage.local` für Debugging

**Was die Extension explizit NICHT macht:**
- Keine ZIP-Extraktion (WebExtension-API kann's nicht)
- Keine Folder-Struktur Artist/Album/ (geht nur flach in den FF-Download-Ordner)
- Keine Discogs-Lookups, keine Audio-Library-Logik
- Keine Mausbewegung, kein Focus-Steal, keine UI-Popups

## Architektur

```
   ┌──────────────────────────────────────┐
   │         Dein Firefox (Mac)           │
   │                                      │
   │   ┌─────────────────────────────┐    │
   │   │  bandcamp-warden-helper     │    │
   │   │  (unsere Extension)         │    │
   │   │                             │    │
   │   │  • alarms (scheduling)      │    │
   │   │  • storage.local (Queue)    │    │
   │   │  • downloads.download(url)  │    │
   │   └────────────┬────────────────┘    │
   │                │                     │
   └────────────────┼─────────────────────┘
                    │
                    ▼ ZIP-Files
   ┌──────────────────────────────────────┐
   │  /Volumes/storage/media/bandcamp/    │
   │      _inbox/                         │
   │          Artist - Album.zip          │
   │          Artist2 - Album2.zip        │
   │          …                           │
   │  (SMB-Mount auf TrueNAS)             │
   └────────────────┬─────────────────────┘
                    │
                    ▼ inotify watch
   ┌──────────────────────────────────────┐
   │   TrueNAS Sidecar (existing)         │
   │                                      │
   │   • erkennt neue ZIP                 │
   │   • entpackt nach Artist/Album/      │
   │   • schreibt bandcamp_<id>.json      │
   │   • löscht ZIP                       │
   └──────────────────────────────────────┘
```

**Zwei kooperierende Teile**:
1. Extension (Mac, dein Firefox) — verantwortlich fürs Beschaffen der Bytes
2. Sidecar (TrueNAS, existiert) — verantwortlich für Library-Struktur + Metadaten

Saubere Trennung: Extension hat Browser-Identity, Sidecar hat Filesystem-Zugriff. Jeder macht das was er kann.

## Tech-Stack

- **Sprache**: TypeScript (statische Typen, native Sprache für WebExtensions)
- **Build**: Vite oder esbuild (schnell, einfach)
- **Manifest**: V2 (Firefox-only, MV3 unnötige Komplexität für unsere Zwecke)
- **Browser-API**: `webextension-polyfill` für sauberes Promise-API
- **Lint/Format**: optional, aber `tsc --noEmit` als Pre-Commit-Check
- **Tests**: minimal — vitest für Pure-Logic-Funktionen (URL-Parsing, Queue-Reducer)

Layout im Repo:

```
extension/
├── manifest.json
├── package.json
├── tsconfig.json
├── vite.config.ts
├── src/
│   ├── background/
│   │   ├── index.ts            # entrypoint, alarms-handler
│   │   ├── queue.ts            # persistent queue logic
│   │   ├── downloader.ts       # downloads.download wrapper
│   │   ├── pacing.ts           # jitter + circuit breaker
│   │   └── api.ts              # Bandcamp Fan API + page scraping
│   ├── options/
│   │   ├── options.html
│   │   └── options.ts          # config UI
│   └── shared/
│       ├── types.ts            # shared TS types
│       └── log.ts
└── public/
    └── icons/
        ├── 16.png
        ├── 32.png
        └── 128.png
```

## Implementation-Details

### Queue-Persistence

State in `browser.storage.local`:

```ts
interface State {
    queue: { id: number; bandName: string; itemTitle: string; downloadPageUrl: string }[];
    inFlight: number | null;     // current item being downloaded
    completed: number[];          // item ids successfully downloaded
    failed: { id: number; error: string; lastTryAt: string }[];
    lastRunAt: string;
    todayDownloaded: number;
    todayResetAt: string;         // for daily quota reset
    consecutiveFailures: number;
}
```

Atomic update via `browser.storage.local.set` ist schon transactional — kein zusätzliches Locking nötig.

### Per-Item-Workflow

```
1. Pop item from queue (atomic)
2. Random sleep 30-180s
3. fetch(item.downloadPageUrl, credentials: 'include')
   → parse data-blob from HTML
   → extract digital_items[].downloads.flac.url
4. browser.downloads.download({
       url: signedUrl,
       filename: `bandcamp_${item.id}.zip`,    // forced flat naming
       conflictAction: 'uniquify',
   })
5. Wait for browser.downloads.onChanged (state=complete)
6. Mark item completed, save state, increment counter
7. If error: push to failed list, increment consecutiveFailures
8. If consecutiveFailures >= 5: pause 1h, reset counter
9. If todayDownloaded >= 500: pause until midnight + 60s
10. Loop
```

### Browser.alarms-Scheduling

```ts
browser.alarms.create('warden-tick', {
    periodInMinutes: 1,           // wake up every minute
    delayInMinutes: 0,
});
browser.alarms.onAlarm.addListener(async (alarm) => {
    if (alarm.name !== 'warden-tick') return;
    await processNextItem();      // returns immediately if mid-download
});
```

Die Extension ist also wie ein cron-Job der minütlich aufwacht und schaut ob was zu tun ist. Wenn ein Download läuft, kein No-Op. Wenn Quota erreicht oder consecutive failures: skip.

### Erste Befüllung der Queue

Beim ersten Lauf (oder via "refresh"-Button im Options-UI):

```ts
async function refreshQueue() {
    // Use bandcamp's authenticated /api/fancollection/1/collection_items
    const fanId = await getFanIdFromHomepage();
    const allItems = await paginateCollection(fanId);
    const completed = await getCompletedIds();
    const newItems = allItems.filter(i => !completed.has(i.id));
    await browser.storage.local.set({ queue: newItems });
}
```

Die Extension nutzt also bandcampsync's API-Pattern (POST `/api/fancollection/1/collection_items`) ABER aus dem Browser-Context — also mit den lebenden Cookies und der Browser-TLS-Verbindung des Users.

### Quota + Pacing

- `todayDownloaded` zählt seit Mitternacht lokal (Mac-Zeit)
- Cap konfigurierbar, default 500
- Inter-download delay: random uniform [30, 180] Sekunden
- Bei 500/day in 12h Schlaf-Window = 12*3600/500 = 86s avg between downloads
- Random jitter ist breit genug dass es nicht uniform wirkt

### Circuit-Breaker

- 5 consecutive failures → pause 1h
- Nach 1h: 1 Probe-Item, wenn das auch fehlschlägt → 4h Pause
- Konfigurierbar
- Reset nach erstem erfolgreichen Item

### Konfiguration via Options-Page

Minimal UI mit:
- **Daily Quota** (default 500)
- **Format** (FLAC default; Dropdown)
- **Min/Max Delay** zwischen Downloads
- **Active Hours** (z.B. nur 22:00-08:00; default 24h)
- **Refresh Queue** (Button — re-pulls collection)
- **Status-Anzeige**: queue size, completed today, total completed, current state, last error

### Toolbar-Badge

`browser.browserAction.setBadgeText({ text: '47' })` — Anzahl der heute heruntergeladenen Albums. Subtle, kein Popup.

## Server-Side: Inbox-Watcher

Der TrueNAS-Sidecar bekommt einen neuen Endpoint + Background-Task:

```python
# Im sidecar/app.py:
async def _inbox_watcher_loop():
    inbox = Path(settings.downloads_view_path) / "_inbox"
    inbox.mkdir(parents=True, exist_ok=True)
    while True:
        for zip_path in inbox.glob("bandcamp_*.zip"):
            try:
                # Extract item_id from filename
                m = re.match(r"bandcamp_(\d+)\.zip", zip_path.name)
                if not m:
                    continue
                item_id = int(m.group(1))
                # Look up band/title via Fan API or cache
                # Extract ZIP into Artist/Album/
                # Write bandcamp_<id>.json metadata
                # Append to ignores.txt
                # Delete original zip
            except Exception as e:
                log.error(...)
        await asyncio.sleep(30)
```

Das nutzt die `MetadataEnricher`-Logik die wir schon haben. Erweitert sie um den ZIP-Verarbeitungspfad.

## Distribution

Drei Optionen für Install in deinen Firefox:

### Option A: Firefox Developer Edition (empfohlen)

- Du installierst Firefox Developer Edition parallel zu deinem normalen Firefox
- Setting `xpinstall.signatures.required = false` in `about:config`
- `web-ext run` während Dev oder permanent installieren
- **Vorteil**: keine AMO-Reviewer-Wartezeit, eigene Updates jederzeit
- **Nachteil**: Du musst zwei Firefox-Profile pflegen ODER Bandcamp-Cookies nach Developer Edition kopieren

### Option B: about:debugging temporary install

- Im normalen Firefox: `about:debugging` → "This Firefox" → "Load Temporary Add-on" → unsere XPI
- **Vorteil**: kein zweiter Firefox, läuft in deinem Hauptbrowser
- **Nachteil**: bei Browser-Restart ist Extension weg, du müsstest sie neu laden. Persistent storage.local bleibt aber!

### Option C: AMO Self-Distribution Signing

- Wir reichen die XPI bei Mozilla zur Signierung ein (`web-ext sign --use-submission-api`)
- Sign-only, keine öffentliche AMO-Listung
- **Vorteil**: läuft in regulärem Firefox dauerhaft
- **Nachteil**: 2-7 Tage Mozilla-Review beim ersten Submit, danach schneller

**Meine Empfehlung: Option C**. Einmalig 3-5 Tage Review, danach problemfrei. Du installierst die signierte XPI per drag-and-drop in den Firefox-Tab und sie bleibt.

## Pacing & Caffeinate

- Extension scheduled sich selbst — kein externer Cron nötig
- macOS Sleep-Prevention: User startet Terminal-Tab mit `caffeinate -i` über Nacht
- Optional: LaunchAgent-plist `com.warden.caffeinate.plist` das automatisch caffeinate startet wenn ein File `~/.warden/active` existiert (Extension legt das an wenn aktiv, löscht wenn quota erreicht)

## Aufwand-Schätzung

| Komponente | Aufwand |
|---|---|
| Extension scaffolding (manifest, vite, ts setup) | 1h |
| Background queue + alarms | 2h |
| Bandcamp API integration (fancollection + page scrape) | 2h |
| Download + onChanged listener | 1h |
| Pacing + circuit breaker | 1h |
| Options page UI | 2h |
| Logging + status badge | 30min |
| Server-side inbox-watcher | 2h |
| Testing + iteration | 3h |
| **Total** | **~14h** |

Über 2-3 Sessions verteilt machbar. MVP nach 6-8h, Rest ist Polish.

## Entscheidungen (vom User bestätigt 2026-05-08)

1. **ZIP-Handling**: **Variante 1** — TrueNAS-Sidecar erkennt ZIPs in Inbox, entpackt nach `Artist/Album/`, schreibt `bandcamp_<id>.json`, löscht ZIP.
2. **Distribution**: **AMO Self-Distribution Signing** (unlisted). Wir benutzen `web-ext sign --use-submission-api`. Resultat: signierte `.xpi` die in Standard-Firefox per Drag-and-Drop installierbar ist und permanent läuft. Kein Public-Listing auf addons.mozilla.org. Erster Submit braucht 1-7 Tage Review, Folgereleases meist <24h.
3. **SMB-Mount-Pfad** auf Mac: `/Volumes/storage/media/bandcamp` ist korrekt.
4. **caffeinate**: **LaunchAgent-Auto** — wird zusammen mit der Extension installiert.
5. **Quota**: **250 Alben/Tag**.
6. **Active Hours**: **24/7** (kein Zeitfenster).

Daraus abgeleitete Pacing-Defaults:
- Bei 250/24h = 1 Album alle ~5.7 Min im Schnitt
- Random Jitter zwischen Downloads: **uniform [60s, 300s]** (statt der ursprünglich vorgeschlagenen 30-180s, weil 250/Tag mehr Zeit-Reserve gibt und breiter gestreutes Pacing harmloser aussieht)
- In Options-Page weiterhin tweakbar

## Implementation-Reihenfolge (Roadmap)

Folge-Claude soll in dieser Reihenfolge arbeiten und nach jedem Punkt einen sinnvollen Commit machen:

### Phase 1: Extension-Skeleton + Build
1. `extension/` Verzeichnis-Layout wie oben spezifiziert anlegen
2. `manifest.json` MV2 mit Permissions: `storage`, `alarms`, `downloads`, `tabs`, Hosts `https://bandcamp.com/*` und `https://*.bandcamp.com/*`. `browser_specific_settings.gecko.id` setzen — irgendwas eindeutiges wie `warden-helper@idleherb.bandcamp-warden`.
3. `package.json` mit Dependencies: `webextension-polyfill`, `typescript`, `vite`, `@types/firefox-webext-browser`, `vite-plugin-web-extension` (oder einfacher: rohes esbuild-build-script). `web-ext` als devDep für sign + run.
4. `tsconfig.json` strict, target ES2022, module ESNext
5. `vite.config.ts` der Background- und Options-Bundles produziert in `extension/dist/`
6. Icons: 16/32/128 PNG. Können simple Platzhalter sein. (User soll später ggf. selber gestalten — niedrige Priorität.)
7. Build-Test: `pnpm build` produziert eine ladbare `extension/dist/` Verzeichnisstruktur.

### Phase 2: Core-Datenstrukturen + Storage
8. `src/shared/types.ts`: `QueueItem`, `State`, `Config` Interfaces wie unter "Queue-Persistence" beschrieben.
9. `src/shared/storage.ts`: typsichere wrapper um `browser.storage.local.get/set` mit atomic update-helper (read-modify-write in einem `await`).
10. `src/shared/log.ts`: Ringbuffer-Logger der in `browser.storage.local["log"]` schreibt (capped auf z.B. 500 Zeilen). Wird in Options-Page angezeigt.
11. `src/shared/config.ts`: Config-Defaults (quota 250, jitter 60-300s, etc.) + Reader/Writer.

### Phase 3: Bandcamp-API-Integration
12. `src/background/api.ts`:
    - `getFanIdFromHomepage()` — fetched `https://bandcamp.com/`, parst `<div id="HomepageApp" data-blob>` JSON, extrahiert `pageContext.identity.fanId`.
    - `paginateCollection(fanId)` — POST an `https://bandcamp.com/api/fancollection/1/collection_items` mit `{fan_id, count: 100, older_than_token}`. Iteriert bis `more_available=false`. Returnt Array `{id, bandName, itemTitle, downloadPageUrl}`.
    - `resolveSignedUrl(downloadPageUrl, format)` — fetched die `bandcamp.com/download?...` Page, parst `<div id="pagedata" data-blob>`, navigiert nach `digital_items[0].downloads[<format>].url`.
    - Alle fetch-Calls mit `credentials: 'include'` damit User-Cookies des Browsers verwendet werden.
    - Defensive Parser: wenn JSON-Schema ändert, expliziter Throw mit Hinweis "Bandcamp HTML changed, parser needs update".

### Phase 4: Queue + Scheduler
13. `src/background/queue.ts`:
    - `refreshQueue()` — pullt komplette Collection via api.ts, dedupes gegen `state.completed`, schreibt rest in `state.queue`.
    - `popNextItem()` — atomically removes head, sets as `state.inFlight`. Returns null wenn queue leer.
    - `markCompleted(itemId)` — moves item from inFlight to completed list, increments todayDownloaded, persists.
    - `markFailed(itemId, error)` — increments consecutiveFailures, pushes to failed list.
14. `src/background/pacing.ts`:
    - `shouldRunNow(state, config)` — prüft: Quota für heute erreicht? Circuit-Break aktiv? Genug Zeit seit letztem Download?
    - `dailyResetIfNeeded(state)` — setzt `todayDownloaded=0` wenn neuer Tag.
    - `circuitBreakerLogic` — nach N consecutiveFailures: 1h Pause, dann Probe-Item, dann längere Pause wenn der auch failt. Reset auf 0 nach erstem Success.
15. `src/background/index.ts`:
    - On install/startup: `browser.alarms.create('warden-tick', { periodInMinutes: 1 })`
    - On alarm: `processOneTick()` — wenn `inFlight` schon gesetzt, no-op. Wenn nicht: `shouldRunNow` checken, Item ziehen, Download starten.
    - Browser-Action-Click handler: opens Options-Page tab.

### Phase 5: Download-Orchestrator
16. `src/background/downloader.ts`:
    - `downloadItem(item)`:
      - Resolve signed URL via api.ts
      - `browser.downloads.download({ url, filename: 'bandcamp_${id}.zip', conflictAction: 'uniquify' })`
      - Dateinamen-Schema: **`bandcamp_<itemId>.zip`** — flach, mit Item-ID damit Sidecar-Watcher die zuordnen kann.
      - Promise das resolved wenn `browser.downloads.onChanged` mit state=complete für die Download-ID ankommt
      - Reject wenn state=interrupted oder timeout (z.B. 1h pro Album)
    - Auf Success: `markCompleted(item.id)`. Auf Fail: `markFailed(item.id, error)`.

### Phase 6: Options-Page
17. `src/options/options.html` + `options.ts`:
    - Form-Felder: Daily Quota, Format Dropdown (default flac), Min/Max Delay, Download-Path-Anzeige (read-only — Firefox setzt's via `browser.downloads.setShelfEnabled` und download-folder-Pref, das müssen wir beim Install setten)
    - Buttons: "Refresh Queue Now" (re-pulls collection from Bandcamp), "Pause/Resume", "Reset Failed Items"
    - Status-Tabelle: total in queue / completed today / completed total / current state / current item (if any) / last 50 log lines
    - Vue/React/Svelte unnötig — plain TS + DOM-Manipulation ist ausreichend für die paar Felder.

### Phase 7: Browser-Action-Badge
18. In `index.ts`: nach jedem successful download `browser.browserAction.setBadgeText({text: String(state.todayDownloaded)})` setzen. Bei `paused` oder `circuit-break`: `text: '⏸'` oder ähnlich. Bei Quota erreicht: ✓.

### Phase 8: Server-Side Inbox-Watcher
19. In `sidecar/app.py`:
    - Neue settings: `inbox_path` (default `Path(downloads_view_path) / "_inbox"`), `inbox_poll_seconds` (default 30).
    - `_inbox_watcher_loop()` async coroutine in lifespan registrieren.
    - Pro ZIP in Inbox:
      - Filename pattern matchen: `bandcamp_(\d+)\.zip`
      - Item-ID extrahieren
      - Per Fan-API (über bestehende `MetadataEnricher._fetch_collection_sync`) den Eintrag holen → `band_name`, `item_title`, `item_url`
      - ZIP entpacken nach `<downloads>/<clean(band_name)>/<clean(item_title)>/`
      - Audio-File-Verifikation: ≥1 audio file? wenn nicht, ZIP nach `_inbox/quarantine/` verschieben mit Telegram-Alert
      - Bei Erfolg: `bandcamp_<id>.json` schreiben (gleiche Felder wie MetadataEnricher), Eintrag in `ignores_warden.txt` (oder `ignores.txt` wenn rw), ZIP löschen.
    - Telegram-Push pro 10 verarbeitete Items mit Stats.
20. Endpoint `GET /inbox-status` für Debugging: zeigt Anzahl Files in Inbox, last processed, Quarantäne-Inhalt.
21. Test: lege manuell eine ZIP `bandcamp_4154699124.zip` in `/mnt/storage/media/bandcamp/_inbox/`, prüfe dass Sidecar sie aufgreift, entpackt, ZIP weg ist, Album-Ordner steht.

### Phase 9: caffeinate LaunchAgent
22. `extension/macos/com.warden.caffeinate.plist` — LaunchAgent das `caffeinate -i` ausführt solange `~/.warden/active` existiert (Extension legt File an beim ersten Tick wenn Quota nicht voll, löscht es nach Quota-Hit).
    - `KeepAlive: { PathState: { /Users/<user>/.warden/active: true } }`
    - `ProgramArguments: [/usr/bin/caffeinate, -i]`
23. `install-launch-agent.sh` Script das die plist nach `~/Library/LaunchAgents/` kopiert mit ersetztem Username, dann `launchctl load`.
24. Extension schreibt `~/.warden/active` via Native Messaging Helper... Hmm, WebExtensions können nicht direkt File-System-Access. Alternative: Extension setzt eine HTTP-Request an localhost-Port den ein kleines Helfer-Skript hört. Oder einfacher: LaunchAgent läuft IMMER (egal ob aktiv) wenn User eingeloggt ist — das ist auch akzeptabel, der `caffeinate -i` Overhead ist null. **EINFACHE LÖSUNG: LaunchAgent startet `caffeinate -i` permanent bei Login, kein Active-File-Check.** User kann's via `launchctl unload ~/Library/LaunchAgents/com.warden.caffeinate.plist` deaktivieren wenn gewünscht.

### Phase 10: Distribution + Install-Doku
25. `web-ext sign --api-key=... --api-secret=... --channel=unlisted` Workflow ans Repo's `package.json` als `npm run sign` script anhängen.
26. `extension/INSTALL.md` mit Schritten:
    - Voraussetzungen: Mac, Firefox >= 115, SMB-Mount auf TrueNAS
    - Bandcamp im Firefox eingeloggt sein
    - Download der signierten `.xpi` von Repo-Releases
    - Drag-and-drop in `about:addons`
    - Options-Page öffnen, Quota verifizieren, "Refresh Queue" klicken
    - Firefox-Pref `browser.download.dir` auf `/Volumes/storage/media/bandcamp/_inbox` setzen (oder Extension setzt's automatisch via `browser.downloads.setShelfEnabled` + folder-pref-Tweak — TBD ob WebExtension das darf)
    - LaunchAgent installieren via `bash install-launch-agent.sh`
27. CI/CD: GHA-Workflow der bei Tag-Push die Extension baut, signed XPI als Release-Artefakt anhängt.

### Phase 11: Testing + Polish
28. End-to-End-Test: Extension in Firefox laden, eine kleine Test-Quota (z.B. 3 Alben) setzen, beobachten dass:
    - Queue wird mit ~3110 Items gefüllt
    - Erstes Album wird in `_inbox` gedumpt
    - TrueNAS-Sidecar entpackt, organisiert
    - Counter im Browser-Badge hochgeht
    - Nach 3 Alben: Quota-Hit, Pause bis Mitternacht
29. Resilienz-Tests:
    - Browser killen mid-download → nach Restart resumed Queue (ZIP geht verloren, item zurück in Queue)
    - Network-Aussetzer → onChanged interrupted → markFailed → next tick versucht's nochmal mit Pause
    - Bandcamp Anti-Bot triggert → consecutive failures → circuit breaker → Telegram (via Sidecar / Server-Side Push)

## Key Implementierungs-Details die der Folge-Claude wissen muss

### Wie WebExtensions auf Bandcamp's Cookies kommen
Sobald die Extension Hostpermission `*.bandcamp.com/*` hat, sendet `fetch(..., {credentials: 'include'})` automatisch die Browser-Cookies des Users. Wir müssen NICHTS manuell mit cookies.txt machen. Das ist anders als Server-Side wo wir cookies.txt parsen mussten.

### Wie der Download-Folder gesetzt wird
Firefox-Pref `browser.download.dir` ist NICHT direkt von WebExtensions setzbar. Optionen:
1. User setzt es einmalig manuell in Firefox-Settings auf `/Volumes/storage/media/bandcamp/_inbox`
2. Wir nutzen `browser.downloads.download({filename: '_inbox/bandcamp_${id}.zip'})` — der relative Pfad wird unter dem konfigurierten Default-Download-Folder erzeugt. Wenn der nicht stimmt, geht's halt nach `~/Downloads/_inbox/`. Funktioniert, ist aber off.
3. Empfohlen: Install-Doku weist User an einmal manuell den Default zu setzen. Dann ist `filename: 'bandcamp_${id}.zip'` (flach) okay.

### Bandcamp-Format-Strings
Aus dem `data-blob`: `digital_items[0].downloads` ist ein Object mit Keys wie `flac`, `mp3-v0`, `mp3-320`, `aac-hi`, `vorbis`, `alac`, `wav`, `aiff-lossless`. User-Default `flac`.

### Was bandcampsync v0.7.0 macht (Referenz)
Quelle der API-Patterns: https://github.com/meeb/bandcampsync (kein License, daher nur Logik-Inspiration, kein Code-Copy). Konkret: `bandcampsync/bandcamp.py:280` (`load_purchases`) und `bandcampsync/bandcamp.py:282` (`get_download_file_url`) sind die Referenzen für API-Endpoint und data-blob-Pfad.

### Existierender Sidecar-Code zum Wiederverwenden
- `MetadataEnricher` in `sidecar/app.py:~410` hat bereits Fan-API-Integration via bandcampsync-Library + Folder-Matching mit NFKD-Normalisierung. Der Inbox-Watcher kann grosse Teile davon wiederverwenden — speziell `clean_path_component`, `_build_record`, `_atomic_write`.
- `WardenDownloader` in `sidecar/downloader.py` ist im Plan-E-Setup obsolet, kann aber als Fallback bestehen bleiben (über `WARDEN_DOWNLOADER_STRATEGY=container` aktivierbar).
- `_kickoff_sidecar` im Orchestrator wird nicht mehr getriggert sobald die Extension läuft. Daily-Kickoff kann optional auf "noop" umgestellt werden — die Extension scheduled sich selbst.

### Sidecar-Konfiguration die geändert werden muss
- Default `WARDEN_DOWNLOADER_STRATEGY` bleibt `sidecar`, aber Inbox-Watcher läuft parallel und übernimmt die echte Arbeit.
- ODER: neue Strategy `WARDEN_DOWNLOADER_STRATEGY=inbox` die nur den Watcher startet und Daily-Kickoff komplett deaktiviert. Empfehlung: diese neue Strategy als Default setzen sobald Extension stabil läuft.

### Telegram-Notifications die der Sidecar weiter pusht
- ZIP angekommen + entpackt: optional, eher nicht (zu viel Spam bei 250/Tag)
- Quota-Erreicht (250 today): Telegram-Push 1× pro Tag
- Quarantäne-Item: Push mit Filename
- Inbox-Watcher-Down: Healthcheck-failure-Alert
- Cookie-Expiry: bestehende Logik, läuft weiter

### Was die Extension NICHT braucht
- Keine Telegram-Integration direkt. Status fließt über die Datei-Drops in den Sidecar, der pusht. Das hält Tokens lokal aufgeräumt.
- Keine eigene Cookie-Verwaltung. Browser-Cookies sind die Single Source of Truth.
- Keine Range-Resume-Logik. `browser.downloads` macht keine Range-Resumes; bei Abbruch wird das ganze ZIP neu geholt. Bei 250 Alben/Tag ist das 0.4% Wastage selbst wenn täglich 1 Item abbricht — vernachlässigbar.

## Beziehung zu bisherigen Plan-A bis Plan-D Code

| Komponente | Status nach Plan-E Implementierung |
|---|---|
| `sidecar/downloader.py` (Plan C) | bleibt, wird nicht mehr default-aktiv |
| `sidecar/patches/bandcampsync_download.py` | bleibt, deaktiviert über `bandcampsync_patch_enabled=false` |
| `sidecar/browser_downloader.py` (Plan D Playwright) | kann gelöscht werden ODER als Fallback behalten |
| `sidecar/app.py` Plan-D Endpoints (`/test-browser-download`, `/test-range-support`, `/test-browser-headers`, `/probe-url`) | als Diagnose-Endpoints behalten — nützlich falls man's je wieder braucht |
| `BandcampsyncController` (legacy container) | kann gelöscht werden |
| Dockerfile Playwright-Setup | rückgängig machen wenn Plan-D aufgegeben wird (Image schrumpft auf ~250MB), oder behalten als Optionalität |
| `_cleanup_orphan_bandcampsync` | kann weg sobald Container-Strategy entfernt |

Empfehlung: **alles Legacy behalten als Fallback**, aber default auf Inbox-Strategy umstellen. Dockerfile darf Playwright zurückbauen weil's ungenutzt 1GB kostet — das ist ein eigener Refactor-Commit.

## Was der Folge-Claude zuerst lesen soll

Reihenfolge der Files zur Orientierung:

1. `CLAUDE.md` (Projekt-Lighthouse)
2. `docs/ARCHITECTURE.md` (Server-Side-Architektur die wir behalten)
3. `docs/PLAN-mac-extension.md` (DIESES FILE)
4. `docs/OPERATIONS.md` (Existierende Telegram-Patterns + Endpoints)
5. `sidecar/app.py` (Settings-Class, MetadataEnricher, Telegram, State)
6. Memory-Files unter `~/.claude/projects/.../memory/`

## Status nach 5 Tagen Server-Side-Versuche (Datapoints für Folge-Claude)

- 295 Albums fertig (von ~3110)
- 14 Empty-Folder von Plan-A Bug bereits manuell gelöscht; ihre IDs sind in `ignores.txt` — werden vom Sidecar nicht nochmal angefasst, müssten via `/cleanup-stale-ignores?dry_run=false` aus ignores entfernt werden DAMIT die Extension sie als "noch zu downloaden" sieht.
- **Wichtig**: User will 250/Tag. Bei aktuellen 295 done = noch 2815 = ~12 Tage Realmaß.
- Server-Side mit jeder denkbaren HTTP-Mimicry: ~7 KB/s (=throttled). Nicht reparierbar, deshalb dieser Plan.
- User's regulärer Firefox auf demselben Mac: 39 MB/s (bewiesen, mehrfach). Daher die Extension-Strategie.
- Mac-SMB-Mount: `/Volumes/storage/media/bandcamp` (bestätigt). TrueNAS-Pfad: `/mnt/storage/media/bandcamp`.

## Dont's

- **Niemals** während der Implementation Test-Triggers gegen Bandcamp's Server-Side fahren. Der Account ist bereits auf einer Drosselungs-Liste, jeder weitere bot-artige Request verschlimmert das. Tests nur in der Extension via Firefox. Zur Not Test-Quota auf 1-3 stellen, schauen ob's klappt.
- **Niemals** Code von Batchcamp wörtlich kopieren — keine Lizenz, IP-rechtlich riskant. Logik/Patterns schon, weil Tatsachen über Bandcamp's API.
- **Niemals** AMO-Submission als "produktiv" markieren — wir wollen unlisted/self-distribution, sonst wird's gegen Bandcamps ToS geprüft (Bandcamp erlaubt private Backups, aber AMO-Reviewer könnten skeptisch sein).


## Risiken

1. **Bandcamp könnte auch echten Firefox erkennen wenn er zu schnell zu viele Downloads macht** — Risiko-Mitigation: Pacing-Knöbe, Circuit Breaker, Daily-Quota.
2. **Browser-Restart mid-Download** — Mitigation: persistent queue + onChanged-Resume-Logic. ZIP wird halt neu gezogen, Resume bei großen Files nicht möglich (browser.downloads kennt's nicht).
3. **macOS sperrt sich trotz caffeinate** — Edge-Cases (battery, lid closed). Mitigation: prüfen vor Run, Telegram-Alert wenn keine Aktivität in N Stunden.
4. **AMO lehnt ab** weil "wir bauen einen Bandcamp-Downloader" — möglich, dann Plan B (Option A oder B).
5. **Bandcamp ändert das HTML der Download-Seite** und unser Parser bricht — Mitigation: defensive parser, Telegram-Alert bei N consecutive parse-failures.
