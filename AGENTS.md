# AGENTS.md

This file provides guidance for AI coding agents working on the **Local Clipboard** project.

## Project overview

Local Clipboard is a self-contained local-network clipboard/chat application. It lets devices on the same Wi-Fi or LAN share text snippets and files through a browser, without any internet dependency at runtime. The whole application ships as a single Go binary that embeds its own web assets.

Key characteristics:

- Single Go binary with embedded HTML/CSS/JS (`//go:embed web/*`).
- Real-time messaging over WebSocket.
- In-memory message storage; uploaded files are streamed to a temporary disk directory and deleted on clear, when the server stops, or when an isolated room is cleaned up.
- Isolated rooms via `/r/{roomID}`: each room has its own messages, files, clients, and auto-clear timer. The root path `/` remains the shared default room.
- Auto-clear timer, manual clear, pause/resume, and live countdown UI.
- QR code generation for easy mobile joining.
- Automatic update banner in the UI (checks GitHub Releases via `api.github.com`).
- Multi-platform releases (macOS Intel/Silicon, Linux amd64, Windows amd64) and a multi-arch Docker image published to GHCR.

## Technology stack

- **Language:** Go 1.25 (module `local-clipboard`).
- **Frontend:** Plain HTML5, CSS3, and vanilla JavaScript (no build step, no framework).
- **WebSocket:** `github.com/gorilla/websocket`.
- **QR code generation:** `github.com/skip2/go-qrcode` and `github.com/mdp/qrterminal/v3` (terminal QR).
- **Build orchestration:** `make`.
- **Changelog/release notes:** `git-cliff` via `cliff.toml`.
- **CI/CD:** GitHub Actions (`.github/workflows/release.yml`).
- **Container runtime:** Docker / GHCR.

## Project structure

```text
.
├── main.go                 # Entire Go backend (HTTP server, WebSocket hub, file store)
├── go.mod                  # Go module definition
├── go.sum                  # Go dependency checksums
├── Makefile                # Build, run, update, and vet targets
├── Dockerfile              # Multi-stage build producing a small Alpine image
├── cliff.toml              # git-cliff configuration for release changelogs
├── README.md               # Human-facing documentation
├── CLAUDE.md               # Existing Claude Code guidance (keep in sync)
├── .gitignore              # Standard Go + build + IDE ignore list
├── .gitattributes          # LF normalization
├── .github/workflows/      # release.yml — tag-triggered builds and Docker push
├── web/
│   ├── index.html          # Single-page UI
│   ├── script.js           # Frontend logic (WebSocket, file upload, update checks)
│   └── styles.css          # UI styling, animations, responsive breakpoints
└── assets/
    └── screenshot.jpeg     # README screenshot
```

## Build, run, and release commands

All common tasks are exposed through `make`:

```bash
# Show available targets
make help

# Run the server locally on the default port (8080)
make run

# Run on a custom port
make run PORT=3000

# Build release binaries for macOS (Intel/Silicon), Linux, and Windows
make build

# Build with a specific version injected into the binary
make build VERSION=1.2.3

# Update Go dependencies
make update

# Static analysis (go vet; uses modernize if installed)
make vet
```

Other useful direct commands:

```bash
# Install dependencies
go mod download

# Run directly without make
go run main.go -port 8080

# Run with a custom empty-room cleanup TTL
go run main.go -room-ttl 10m

# Build a single binary
go build -ldflags "-X main.Version=dev" -o local-clipboard main.go

# Docker build (pass VERSION so /api/version reports correctly)
docker build --build-arg VERSION=x.y.z -t local-clipboard .
docker run -p 8080:8080 local-clipboard

# When running in Docker, set LOCAL_CLIPBOARD_HOST to the host's reachable
# address so the QR code points to something clients can actually reach:
docker run -p 8080:8080 -e LOCAL_CLIPBOARD_HOST=192.168.1.100 local-clipboard
```

Binaries are written to `./build/` and named:

```text
local-clipboard-{VERSION}-mac-intel
local-clipboard-{VERSION}-mac-silicon
local-clipboard-{VERSION}-linux-amd64
local-clipboard-{VERSION}-windows-amd64.exe
```

## Runtime architecture

The backend lives entirely in `main.go`.

### Core components

- **RoomManager** — owns a map of `Room`s keyed by room ID. It lazily creates rooms on first access and runs a background goroutine that removes empty rooms after a configurable TTL (`-room-ttl`, default 5 minutes).
- **Room** — an isolated chat space containing one `Hub`, one `FileStore`, and one `MessageStore`. The special `"default"` room backs the root path `/`.
- **Hub** — per-room WebSocket manager running in its own goroutine. It owns:
  - `clients map[*websocket.Conn]string` (connection → resolved client IP).
  - `broadcast`, `register`, `unregister` channels.
  - Auto-clear control channels: `clearNowCh`, `setIntervalCh`, `togglePauseCh`.
  - `ClearConfig` (interval in minutes, paused flag, next clear time).
- **FileStore** — per-room in-memory map of file ID → `FileData` (metadata only), protected by `sync.RWMutex`. File content is streamed to a room-specific temporary disk directory and cleared on cleanup.
- **MessageStore** — per-room in-memory append-only list of text/file `Message`s, protected by `sync.RWMutex`. It lets newly connected clients catch up on the current session history; it is cleared alongside files on `/clear` and auto-clear.

### HTTP/WebSocket endpoints

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/` | GET | Serves embedded `web/index.html` (default room) |
| `/styles.css` | GET | Serves embedded stylesheet |
| `/script.js` | GET | Serves embedded JavaScript |
| `/ws` | WebSocket | Real-time message bus (default room) |
| `/upload` | POST | Multipart file upload in the default room |
| `/file/:id` | GET | Downloads a stored file from the default room |
| `/qr` | GET | PNG QR code pointing to the server's root URL |
| `/api/version` | GET | Plain-text version string |
| `/new-room` | POST | Creates a new isolated room with a UUID path and returns `{ "roomUrl": "/r/{uuid}" }` |
| `/clear` | POST | Clears default-room messages/files |
| `/set-interval` | POST | Sets default-room auto-clear interval (`{ "interval": N }`) |
| `/toggle-pause` | POST | Pauses or resumes the default-room auto-clear timer |
| `/r/{roomID}` | GET | Serves the chat UI for an isolated room |
| `/r/{roomID}/ws` | WebSocket | Real-time message bus for that room |
| `/r/{roomID}/upload` | POST | File upload scoped to that room |
| `/r/{roomID}/file/:id` | GET | File download scoped to that room |
| `/r/{roomID}/qr` | GET | QR code pointing to `/r/{roomID}` |
| `/r/{roomID}/clear` | POST | Clears that room's messages/files |
| `/r/{roomID}/set-interval` | POST | Sets that room's auto-clear interval |
| `/r/{roomID}/toggle-pause` | POST | Pauses or resumes that room's auto-clear timer |

### Message flow

1. Text: client sends a WebSocket `Message` → Hub sets `SenderIP` → Hub persists the message in the room's `MessageStore` → Hub broadcasts to all clients in that room.
2. Files: client uploads each file via `POST {room}/upload` → server streams the file to the room's temp directory and stores metadata in the room's `FileStore` → client sends a WebSocket `Message` containing only file metadata and ID → Hub persists the metadata in the room's `MessageStore` → Hub broadcasts metadata to all clients in that room → recipients download via `GET {room}/file/:id`.
3. Late join: a new WebSocket connection receives `type: "history"` containing the current room's persisted messages.

Rooms are fully isolated: a message or file uploaded to `/r/room-a` is never visible from `/r/room-b` or `/`.

### Auto-clear

- Default interval is **10 minutes** and starts immediately in `hub.run()` when `IntervalMin > 0`.
- A `nil` timer channel disables the timer without an extra flag.
- On timer fire or manual clear, the Hub clears both `FileStore` and `MessageStore`, then broadcasts `type: "clear"` to all clients in that room.
- On interval/pause changes, it broadcasts `type: "config"` with the new state.
- Newly connected clients receive the current room's config and message history immediately.
- Each room's timer is independent; clearing one room does not affect others.

### Connected-device count

The Hub counts **unique IPs**, not raw WebSocket connections. The register case drains pending unregisters first to avoid double-counting during quick reconnects. The result is broadcast as `type: "clients"` with a `count` field.

### Client IP resolution

`realIP(r)` checks, in order:

1. `X-Forwarded-For` (first entry of comma-separated list).
2. `X-Real-IP`.
3. `r.RemoteAddr`.

This is important behind reverse proxies or Docker, where NAT would otherwise make every client appear as the gateway IP.

## Frontend notes

- Single-page vanilla JS in `web/script.js`.
- Auto-reconnects on WebSocket close with a 2-second retry.
- Filters echoed own messages using an `ownMessageIds` Set keyed by message ID.
- Multi-file attachment via `<input type="file" multiple>`; selected files render as chips below the textarea.
- `dir="auto"` on message text and textarea for automatic RTL/LTR handling.
- Room-aware API URLs: the frontend computes `apiBase` from the path (`/r/{roomID}` or empty for the default room) and uses it for WebSocket, upload, file download, clear, interval, pause, and QR endpoints.
- Displays the current room name when visiting `/r/{roomID}`.
- Header action buttons: "New Private Room" creates a UUID-backed isolated room and navigates to it; "Copy Room URL" copies the current page URL to the clipboard.
- Desktop breakpoint expands the card width and message area height while keeping a visible border around the app.
- Update checker queries `api.github.com/repos/chucongqing/local-clipboard/releases/latest` on load and shows a banner when a newer release exists.
- Mobile devices hide the QR section via `isMobile()`.

## Code style guidelines

- Follow standard Go formatting. Run `gofmt` on `main.go` after any change.
- Prefer the existing channel-driven Hub pattern for concurrency.
- Protect shared state with mutexes (the project uses `sync.Mutex`/`sync.RWMutex`).
- Keep the frontend dependency-free; do not introduce npm/webpack.
- Maintain the embedded-assets contract (`//go:embed web/*`) so the binary remains self-contained.
- Respect the existing CSS animation names: attachment chip animations are `fileChipIn`/`fileChipOut`; do **not** reuse `slideIn` for chips because it conflicts with message entry animations.

## Testing instructions

There are currently no automated tests, test files, or lint configuration in this project. Validation is manual:

1. Run `make run` and open `http://localhost:8080`.
2. Test text sending from two browser tabs or devices.
3. Upload and download files.
4. Verify device count in the status bar reflects unique IPs.
5. Exercise auto-clear: set interval, wait, pause/resume, and use manual clear.
6. Open `http://localhost:8080/r/room-a` and `http://localhost:8080/r/room-b` in separate tabs and confirm messages/files are isolated.
7. Close all tabs for a room, wait for `-room-ttl` (default 5 minutes), and verify the room's files are no longer accessible.
8. Run `make vet` for static analysis.

When adding tests, prefer standard `*_test.go` files next to the code they exercise and keep them runnable with `go test ./...`.

## Security considerations

- The WebSocket upgrader allows **all origins** (`CheckOrigin: func(r *http.Request) bool { return true }`). This is intentional for local-network use but should be reconsidered if exposing the server to untrusted networks.
- The server binds to `0.0.0.0` by default, making it reachable from the local network.
- There is **no authentication or authorization**. Anyone on the same network can read, send, and clear messages.
- Uploaded files are streamed to a temporary disk directory; only metadata is held in memory. File size is limited by available disk space and the `-max-file-size` flag (default 2GB).
- Uploaded files are accessible to anyone who knows or guesses the nanosecond-based file ID.
- There is no input sanitization beyond HTML escaping in the frontend (`escapeHTML`). Do not treat the app as a secure file-sharing platform.
- When running behind a reverse proxy, configure it to forward the real client IP (`X-Forwarded-For` or `X-Real-IP`) so sender labels and device counts are accurate.

## Deployment and release process

- **Git tags trigger releases.** Pushing a tag matching `v*` runs `.github/workflows/release.yml`.
- The workflow:
  1. Builds cross-platform binaries with `make build VERSION=<tag>`.
  2. Generates a changelog with `git-cliff` using `cliff.toml`.
  3. Creates a GitHub Release attached to the tag and uploads `build/*`.
  4. Builds and pushes a multi-platform (`linux/amd64`, `linux/arm64`) Docker image to `ghcr.io/<owner>/local-clipboard` with semver and `latest` tags.
- Docker base-image versions (`GO_VERSION`, `ALPINE_VERSION`) are centralized in `release.yml` and passed as build args; keep them consistent with `go.mod`.
- Commit messages should follow Conventional Commits because `cliff.toml` groups changelog entries by prefix (`feat`, `fix`, `perf`, `refactor`, `docs`, `style`, `test`, `chore`/`ci`/`build`).

## Useful conventions

- Version injection: `-ldflags "-X main.Version=$(VERSION)"`. The default value is `"dev"`.
- Local IP discovery skips loopback and APIPA (`169.254.0.0/16`) addresses and returns the first valid private IPv4.
- Keep `CLAUDE.md` and `AGENTS.md` in sync when changing architecture, endpoints, or build/release behavior.
