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

## Offene Architekturfragen (vom User zu beantworten)

1. **Variante 1 (TrueNAS organisiert) oder Variante 2 (flacher Inbox)** für die ZIPs?
   → Empfehle V1, ist mehr Code aber sauber Audio-Library-ready.
2. **Distribution**: Developer Edition / about:debugging / AMO-Signing?
   → Empfehle C (AMO-Signing).
3. **SMB-Mount-Pfad** auf dem Mac: `/Volumes/storage/media/bandcamp` korrekt?
4. **caffeinate**: manuell ODER LaunchAgent-Auto?
5. **Quota** bestätigen: 500/Tag oder anders?
6. **Active hours**: durchgehend oder z.B. 22:00–08:00 nur Schlaf?

## Risiken

1. **Bandcamp könnte auch echten Firefox erkennen wenn er zu schnell zu viele Downloads macht** — Risiko-Mitigation: Pacing-Knöbe, Circuit Breaker, Daily-Quota.
2. **Browser-Restart mid-Download** — Mitigation: persistent queue + onChanged-Resume-Logic. ZIP wird halt neu gezogen, Resume bei großen Files nicht möglich (browser.downloads kennt's nicht).
3. **macOS sperrt sich trotz caffeinate** — Edge-Cases (battery, lid closed). Mitigation: prüfen vor Run, Telegram-Alert wenn keine Aktivität in N Stunden.
4. **AMO lehnt ab** weil "wir bauen einen Bandcamp-Downloader" — möglich, dann Plan B (Option A oder B).
5. **Bandcamp ändert das HTML der Download-Seite** und unser Parser bricht — Mitigation: defensive parser, Telegram-Alert bei N consecutive parse-failures.
