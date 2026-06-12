package main

import (
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/mdp/qrterminal/v3"
	"github.com/skip2/go-qrcode"
)

//go:embed web/*
var webFS embed.FS

var Version = "dev"

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for local network
	},
}

type FileData struct {
	ID   string `json:"id,omitempty"` // File ID for download
	Name string `json:"name"`
	Size int64  `json:"size"`
	Type string `json:"type"`
}

type ClearConfig struct {
	IntervalMin   int       `json:"intervalMin"`
	Paused        bool      `json:"paused"`
	NextClearTime time.Time `json:"nextClearTime"`
}

type Message struct {
	ID         string       `json:"id,omitempty"`
	Type       string       `json:"type,omitempty"`
	Text       string       `json:"text,omitempty"`
	SenderIP   string       `json:"senderIp,omitempty"`
	SenderName string       `json:"senderName,omitempty"`
	File       *FileData    `json:"file,omitempty"`
	Config     *ClearConfig `json:"config,omitempty"`
	Count      int          `json:"count,omitempty"`
	Messages   []Message    `json:"messages,omitempty"`
}

type broadcastMsg struct {
	msg    Message
	sender *websocket.Conn
}

type FileStore struct {
	files   map[string]*FileData
	mu      sync.RWMutex
	tempDir string
}

func newFileStore(tempDir string) *FileStore {
	// Clean up any leftover files from a previous run, then recreate the directory.
	if err := os.RemoveAll(tempDir); err != nil {
		log.Printf("Warning: failed to remove old temp dir %s: %v", tempDir, err)
	}
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		log.Fatalf("Failed to create temp dir %s: %v", tempDir, err)
	}
	return &FileStore{
		files:   make(map[string]*FileData),
		tempDir: tempDir,
	}
}

func (fs *FileStore) filePath(id string) string {
	return filepath.Join(fs.tempDir, id+".bin")
}

func (fs *FileStore) set(id string, file *FileData) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.files[id] = file
}

func (fs *FileStore) get(id string) (*FileData, bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	file, ok := fs.files[id]
	return file, ok
}

func (fs *FileStore) clear() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for id := range fs.files {
		path := fs.filePath(id)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Printf("Error removing temp file %s: %v", path, err)
		}
	}
	fs.files = make(map[string]*FileData)
}

// MessageStore keeps a history of text/file messages so late-joining clients
// can catch up. It only stores metadata; file content lives on disk.
type MessageStore struct {
	messages []Message
	mu       sync.RWMutex
}

func newMessageStore() *MessageStore {
	return &MessageStore{
		messages: make([]Message, 0),
	}
}

func (ms *MessageStore) add(msg Message) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.messages = append(ms.messages, msg)
}

func (ms *MessageStore) all() []Message {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	out := make([]Message, len(ms.messages))
	copy(out, ms.messages)
	return out
}

func (ms *MessageStore) clear() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.messages = make([]Message, 0)
}

type connInfo struct {
	conn *websocket.Conn
	ip   string
	name string
}

type clientInfo struct {
	ip   string
	name string
}

type Hub struct {
	clients       map[*websocket.Conn]clientInfo // conn -> client info
	broadcast     chan broadcastMsg
	register      chan connInfo
	unregister    chan *websocket.Conn
	fileStore     *FileStore
	messageStore  *MessageStore
	mu            sync.Mutex
	clearNowCh    chan struct{}
	setIntervalCh chan int
	togglePauseCh chan struct{}
	clearConfig   ClearConfig
}

func newHub(fileStore *FileStore, messageStore *MessageStore) *Hub {
	return &Hub{
		clients:       make(map[*websocket.Conn]clientInfo),
		broadcast:     make(chan broadcastMsg),
		register:      make(chan connInfo),
		unregister:    make(chan *websocket.Conn),
		fileStore:     fileStore,
		messageStore:  messageStore,
		clearNowCh:    make(chan struct{}, 1),
		setIntervalCh: make(chan int, 1),
		togglePauseCh: make(chan struct{}, 1),
		clearConfig:   ClearConfig{IntervalMin: 10},
	}
}

// uniqueDeviceCount returns the number of distinct IPs in the clients map.
// Must be called with h.mu held.
func uniqueDeviceCount(clients map[*websocket.Conn]clientInfo) int {
	seen := make(map[string]struct{})
	for _, ci := range clients {
		seen[ci.ip] = struct{}{}
	}
	return len(seen)
}

func (h *Hub) broadcastToAll(msg Message) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for conn := range h.clients {
		if err := conn.WriteJSON(msg); err != nil {
			log.Printf("Error writing message: %v", err)
			delete(h.clients, conn)
			conn.Close()
		}
	}
}

func (h *Hub) sendConfigToConn(conn *websocket.Conn) {
	msg := Message{
		Type: "config",
		Config: &ClearConfig{
			IntervalMin:   h.clearConfig.IntervalMin,
			Paused:        h.clearConfig.Paused,
			NextClearTime: h.clearConfig.NextClearTime,
		},
	}
	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("Error sending config to new client: %v", err)
	}
}

func (h *Hub) sendWelcomeToConn(conn *websocket.Conn, name string) {
	msg := Message{
		Type:       "welcome",
		SenderName: name,
	}
	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("Error sending welcome to new client: %v", err)
	}
}

func (h *Hub) sendHistoryToConn(conn *websocket.Conn) {
	history := h.messageStore.all()
	if len(history) == 0 {
		return
	}
	msg := Message{
		Type:     "history",
		Messages: history,
	}
	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("Error sending history to new client: %v", err)
	}
}

func (h *Hub) run() {
	var timerChan <-chan time.Time
	if h.clearConfig.IntervalMin > 0 {
		next := time.Now().Add(time.Duration(h.clearConfig.IntervalMin) * time.Minute)
		h.clearConfig.NextClearTime = next
		timerChan = time.After(time.Duration(h.clearConfig.IntervalMin) * time.Minute)
	}

	for {
		select {
		case ci := <-h.register:
			// Drain any pending unregisters before adding the new client.
			// Without this, a simultaneous reconnect (page refresh, brief disconnect)
			// causes Go's select to randomly pick register first, inflating the count.
		drainLoop:
			for {
				select {
				case conn := <-h.unregister:
					h.mu.Lock()
					info := h.clients[conn]
					if info.ip != "" {
						delete(h.clients, conn)
						conn.Close()
					}
					h.mu.Unlock()
					log.Printf("Client disconnected: %s@%s", info.name, info.ip)
				default:
					break drainLoop
				}
			}
			h.mu.Lock()
			h.clients[ci.conn] = clientInfo{ip: ci.ip, name: ci.name}
			count := uniqueDeviceCount(h.clients)
			h.mu.Unlock()
			log.Printf("Client connected: %s@%s. Total devices: %d", ci.name, ci.ip, count)
			h.sendConfigToConn(ci.conn)
			h.sendWelcomeToConn(ci.conn, ci.name)
			h.sendHistoryToConn(ci.conn)
			h.broadcastToAll(Message{Type: "clients", Count: count})

		case conn := <-h.unregister:
			h.mu.Lock()
			info := h.clients[conn]
			if info.ip != "" {
				delete(h.clients, conn)
				conn.Close()
			}
			count := uniqueDeviceCount(h.clients)
			h.mu.Unlock()
			log.Printf("Client disconnected: %s@%s. Total devices: %d", info.name, info.ip, count)
			h.broadcastToAll(Message{Type: "clients", Count: count})

		case bm := <-h.broadcast:
			message := bm.msg
			// Set sender IP and name
			h.mu.Lock()
			senderInfo := h.clients[bm.sender]
			message.SenderIP = senderInfo.ip
			message.SenderName = senderInfo.name
			h.mu.Unlock()

			// Validate and sanitize file metadata before broadcast.
			// Actual file content is stored on disk by the /upload endpoint.
			if message.File != nil && message.ID != "" {
				// Use file ID if provided, otherwise use message ID
				fileID := message.File.ID
				if fileID == "" {
					fileID = message.ID
				}

				if existingFile, exists := h.fileStore.get(fileID); exists {
					log.Printf("Broadcasting file metadata: ID=%s, Name=%s, Size=%d", fileID, existingFile.Name, existingFile.Size)
					message.File = &FileData{
						ID:   fileID,
						Name: existingFile.Name,
						Size: existingFile.Size,
						Type: existingFile.Type,
					}
				} else {
					log.Printf("Warning: File ID %s not found in store, dropping file from broadcast", fileID)
					message.File = nil
				}
			}

			// Persist text/file messages so late-joining clients can catch up.
			if message.Text != "" || message.File != nil {
				h.messageStore.add(message)
			}

			h.mu.Lock()
			for conn := range h.clients {
				err := conn.WriteJSON(message)
				if err != nil {
					log.Printf("Error writing message: %v", err)
					delete(h.clients, conn)
					conn.Close()
				}
			}
			h.mu.Unlock()

		case <-timerChan:
			h.fileStore.clear()
			h.messageStore.clear()
			log.Printf("Auto-clear triggered (%d min)", h.clearConfig.IntervalMin)
			h.broadcastToAll(Message{Type: "clear"})
			next := time.Now().Add(time.Duration(h.clearConfig.IntervalMin) * time.Minute)
			h.clearConfig.NextClearTime = next
			timerChan = time.After(time.Duration(h.clearConfig.IntervalMin) * time.Minute)
			h.broadcastToAll(Message{Type: "config", Config: &ClearConfig{
				IntervalMin: h.clearConfig.IntervalMin, Paused: false, NextClearTime: next,
			}})

		case <-h.clearNowCh:
			h.fileStore.clear()
			h.messageStore.clear()
			log.Printf("Manual clear triggered")
			h.broadcastToAll(Message{Type: "clear"})
			if h.clearConfig.IntervalMin > 0 && !h.clearConfig.Paused {
				next := time.Now().Add(time.Duration(h.clearConfig.IntervalMin) * time.Minute)
				h.clearConfig.NextClearTime = next
				timerChan = time.After(time.Duration(h.clearConfig.IntervalMin) * time.Minute)
				h.broadcastToAll(Message{Type: "config", Config: &ClearConfig{
					IntervalMin: h.clearConfig.IntervalMin, Paused: false, NextClearTime: next,
				}})
			}

		case intervalMin := <-h.setIntervalCh:
			h.clearConfig.IntervalMin = intervalMin
			h.clearConfig.Paused = false
			var next time.Time
			if intervalMin > 0 {
				next = time.Now().Add(time.Duration(intervalMin) * time.Minute)
				timerChan = time.After(time.Duration(intervalMin) * time.Minute)
			} else {
				timerChan = nil
			}
			h.clearConfig.NextClearTime = next
			h.broadcastToAll(Message{Type: "config", Config: &ClearConfig{
				IntervalMin: intervalMin, Paused: false, NextClearTime: next,
			}})

		case <-h.togglePauseCh:
			if h.clearConfig.IntervalMin <= 0 {
				continue
			}
			h.clearConfig.Paused = !h.clearConfig.Paused
			var next time.Time
			if h.clearConfig.Paused {
				timerChan = nil
			} else {
				next = time.Now().Add(time.Duration(h.clearConfig.IntervalMin) * time.Minute)
				timerChan = time.After(time.Duration(h.clearConfig.IntervalMin) * time.Minute)
			}
			h.clearConfig.NextClearTime = next
			h.broadcastToAll(Message{Type: "config", Config: &ClearConfig{
				IntervalMin:   h.clearConfig.IntervalMin,
				Paused:        h.clearConfig.Paused,
				NextClearTime: next,
			}})
		}
	}
}

func (h *Hub) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	ip := realIP(r)
	name := generateChineseName()
	h.register <- connInfo{conn: conn, ip: ip, name: name}

	defer func() {
		h.unregister <- conn
	}()

	for {
		var msg Message
		err := conn.ReadJSON(&msg)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		if msg.Text != "" || msg.File != nil {
			if msg.ID == "" {
				msg.ID = fmt.Sprintf("%d", time.Now().UnixNano())
			}
			h.broadcast <- broadcastMsg{msg: msg, sender: conn}
		}
	}
}

func realIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// X-Forwarded-For can be a comma-separated list; the first entry is the client
		if before, _, ok := strings.Cut(xff, ","); ok {
			return strings.TrimSpace(before)
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

// chineseNamePrefixes and chineseNameSuffixes provide a large pool of words
// for Blizzard-style random names such as "愤怒之狼" or "沉默的猎手".
var chineseNamePrefixes = []string{
	"愤怒", "沉默", "狡猾", "勇敢", "孤独", "狂野", "神秘", "暗影", "光明", "寒冰",
	"烈焰", "雷霆", "迅捷", "坚韧", "狂暴", "幽灵", "钢铁", "血腥", "神圣", "黑暗",
	"贪婪", "傲慢", "嫉妒", "懒惰", "暴食", "色欲", "暴怒", "虚荣", "虚伪", "狂热",
	"冷酷", "无情", "温柔", "慈祥", "威严", "高贵", "卑微", "渺小", "伟大", "永恒",
	"破碎", "完整", "迷失", "觉醒", "沉睡", "苏醒", "凋零", "绽放", "腐朽", "新生",
	"疾风", "骤雨", "惊雷", "闪电", "霜雪", "熔岩", "深渊", "苍穹", "星辰", "死亡",
	"生命", "毁灭", "创造", "秩序", "混乱", "正义", "邪恶", "真理", "谎言", "寂寞",
	"欢愉", "悲伤", "痛苦", "快乐", "恐惧", "希望", "绝望", "信念",
}

var chineseNameSuffixes = []string{
	"野猪", "战狼", "猎手", "刺客", "骑士", "法师", "巨龙", "猛虎", "雄狮", "幽灵",
	"幻影", "风暴", "之刃", "行者", "游侠", "守护者", "毁灭者", "追猎者", "先知", "领主",
	"天使", "恶魔", "亡灵", "兽人", "精灵", "矮人", "侏儒", "巨人", "元素", "树人",
	"武士", "忍者", "剑客", "枪手", "狙击手", "工程师", "炼金术士", "德鲁伊", "萨满", "牧师",
	"凤凰", "朱雀", "青龙", "白虎", "玄武", "麒麟", "饕餮", "穷奇", "混沌", "梼杌",
	"玫瑰", "荆棘", "蔷薇", "百合", "罂粟", "曼陀罗", "彼岸花", "樱花", "梅花", "莲花",
	"雷霆", "烈焰", "冰霜", "暗影", "圣光", "自然", "奥术", "邪能", "鲜血", "死亡",
	"皇帝", "国王", "女王", "王子", "公主", "伯爵", "公爵", "勋爵",
}

var chineseNameJoiners = []string{"的", "之"}

// generateChineseName returns a random Blizzard-style Chinese name
// in the form "prefix+joiner+suffix" (e.g. "愤怒之狼").
func generateChineseName() string {
	prefix := chineseNamePrefixes[rand.Intn(len(chineseNamePrefixes))]
	suffix := chineseNameSuffixes[rand.Intn(len(chineseNameSuffixes))]
	joiner := chineseNameJoiners[rand.Intn(len(chineseNameJoiners))]
	return prefix + joiner + suffix
}

// isImageContentType reports whether a file should be displayed inline as an
// image. It checks the MIME type first, then falls back to the file extension.
func isImageContentType(contentType, fileName string) bool {
	ct := strings.ToLower(contentType)
	imageTypes := []string{
		"image/png", "image/jpeg", "image/jpg", "image/webp", "image/gif",
		"image/svg+xml", "image/bmp", "image/avif", "image/tiff", "image/x-icon",
	}
	for _, t := range imageTypes {
		if ct == t || strings.HasPrefix(ct, t+";") {
			return true
		}
	}

	ext := strings.ToLower(filepath.Ext(fileName))
	imageExts := map[string]bool{
		".png": true, ".jpg": true, ".jpeg": true, ".webp": true,
		".gif": true, ".svg": true, ".bmp": true, ".avif": true,
		".tiff": true, ".tif": true, ".ico": true,
	}
	return imageExts[ext]
}

func getLocalIP() string {
	ifaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	// Iterate through network interfaces
	for _, iface := range ifaces {
		// Skip interfaces that are down or loopback
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok {
				ip := ipnet.IP.To4()
				if ip != nil {
					if ip.IsLoopback() {
						continue
					}
					// Skip APIPA addresses (169.254.0.0/16) - these are auto-assigned when DHCP fails
					if ip[0] == 169 && ip[1] == 254 {
						continue
					}
					// Return the first valid private IP address
					return ip.String()
				}
			}
		}
	}
	return ""
}

// advertisedHost returns the host to advertise in the QR code and startup logs.
// It prefers the LOCAL_CLIPBOARD_HOST environment variable, falling back to the
// first local private IP address. This allows running inside Docker or behind NAT
// while still pointing clients at a reachable address.
func advertisedHost() string {
	if host := os.Getenv("LOCAL_CLIPBOARD_HOST"); host != "" {
		return host
	}
	return getLocalIP()
}

func main() {
	port := flag.String("port", "8080", "Port to run the server on")
	maxFileSize := flag.Int64("max-file-size", 2*1024*1024*1024, "Maximum file upload size in bytes (default 2GB)")
	flag.Parse()

	tempDir := filepath.Join(os.TempDir(), "local-clipboard-uploads")
	fileStore := newFileStore(tempDir)
	messageStore := newMessageStore()
	hub := newHub(fileStore, messageStore)
	go hub.run()

	// Serve static files from embedded filesystem
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")
		switch r.URL.Path {
		case "/":
			data, err := webFS.ReadFile("web/index.html")
			if err != nil {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html")
			w.Write(data)
		case "/styles.css":
			w.Header().Set("Content-Type", "text/css")
			data, err := webFS.ReadFile("web/styles.css")
			if err != nil {
				http.NotFound(w, r)
				return
			}
			w.Write(data)
		case "/script.js":
			w.Header().Set("Content-Type", "application/javascript")
			data, err := webFS.ReadFile("web/script.js")
			if err != nil {
				http.NotFound(w, r)
				return
			}
			w.Write(data)
		default:
			http.NotFound(w, r)
		}
	})

	// Version endpoint
	http.HandleFunc("/api/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(Version))
	})

	// QR code endpoint
	http.HandleFunc("/qr", func(w http.ResponseWriter, r *http.Request) {
		host := advertisedHost()
		if host == "" {
			http.Error(w, "Unable to determine local IP", http.StatusInternalServerError)
			return
		}

		url := fmt.Sprintf("http://%s:%s", host, *port)
		png, err := qrcode.Encode(url, qrcode.Medium, 256)
		if err != nil {
			http.Error(w, "Error generating QR code", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Write(png)
	})

	http.HandleFunc("/ws", hub.handleWebSocket)

	// Clear all messages and files
	http.HandleFunc("/clear", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		select {
		case hub.clearNowCh <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// Set auto-clear interval
	http.HandleFunc("/set-interval", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Interval int `json:"interval"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		if req.Interval < 0 {
			http.Error(w, "Interval must be >= 0", http.StatusBadRequest)
			return
		}
		select {
		case hub.setIntervalCh <- req.Interval:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// Toggle pause/resume for auto-clear timer
	http.HandleFunc("/toggle-pause", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		select {
		case hub.togglePauseCh <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// File upload endpoint
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Limit upload size and stream the body to disk instead of loading it into memory.
		r.Body = http.MaxBytesReader(w, r.Body, *maxFileSize)

		file, header, err := r.FormFile("file")
		if err != nil {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				http.Error(w, "File too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "Error reading file", http.StatusBadRequest)
			return
		}
		defer file.Close()

		fileID := fmt.Sprintf("%d", time.Now().UnixNano())
		tempPath := fileStore.filePath(fileID)

		dst, err := os.Create(tempPath)
		if err != nil {
			http.Error(w, "Error creating temp file", http.StatusInternalServerError)
			return
		}

		written, err := io.Copy(dst, file)
		dst.Close()
		if err != nil {
			os.Remove(tempPath)
			http.Error(w, "Error saving file", http.StatusInternalServerError)
			return
		}

		fileData := &FileData{
			Name: header.Filename,
			Size: written,
			Type: header.Header.Get("Content-Type"),
		}
		fileStore.set(fileID, fileData)
		log.Printf("File uploaded: ID=%s, Name=%s, Size=%d", fileID, fileData.Name, fileData.Size)

		// Return file ID
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"id":"%s","name":"%s","size":%d,"type":"%s"}`, fileID, fileData.Name, fileData.Size, fileData.Type)
	})

	// File download endpoint
	http.HandleFunc("/file/", func(w http.ResponseWriter, r *http.Request) {
		fileID := r.URL.Path[len("/file/"):]
		if fileID == "" {
			http.NotFound(w, r)
			return
		}

		fileMeta, ok := fileStore.get(fileID)
		if !ok {
			log.Printf("File not found: %s", fileID)
			http.NotFound(w, r)
			return
		}

		tempPath := fileStore.filePath(fileID)
		f, err := os.Open(tempPath)
		if err != nil {
			log.Printf("Error opening file %s: %v", fileID, err)
			http.Error(w, "File not available", http.StatusInternalServerError)
			return
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			log.Printf("Error stating file %s: %v", fileID, err)
			http.Error(w, "File not available", http.StatusInternalServerError)
			return
		}

		// Set content type, default to application/octet-stream if empty
		contentType := fileMeta.Type
		if contentType == "" {
			contentType = "application/octet-stream"
		}

		// Display images inline in the chat by default; force download when
		// ?download=1 is provided or the file is not a recognized image.
		if r.URL.Query().Get("download") == "1" || !isImageContentType(contentType, fileMeta.Name) {
			w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, fileMeta.Name))
		}

		// Stream the file from disk; supports range requests and keeps memory usage low.
		http.ServeContent(w, r, fileMeta.Name, fi.ModTime(), f)
		log.Printf("Successfully served file %s (%s, %d bytes)", fileID, fileMeta.Name, fileMeta.Size)
	})

	addr := "0.0.0.0:" + *port

	// Get advertised host for QR code and logs
	advertised := advertisedHost()
	log.Printf("Server starting on %s", addr)
	log.Printf("Open http://localhost:%s on your laptop", *port)
	if advertised != "" {
		log.Printf("Open http://%s:%s on your phone", advertised, *port)
		log.Printf("Or scan the QR code in the web interface")
		fmt.Fprintln(os.Stdout)
		qrterminal.GenerateWithConfig(fmt.Sprintf("http://%s:%s", advertised, *port), qrterminal.Config{
			Level:          qrterminal.L,
			Writer:         os.Stdout,
			HalfBlocks:     true,
			BlackChar:      "  ",
			WhiteChar:      "██",
			BlackWhiteChar: "▄▄",
			WhiteBlackChar: "▀▀",
			QuietZone:      1,
		})
		fmt.Fprintln(os.Stdout)
	} else {
		log.Printf("Open http://<your-laptop-ip>:%s on your phone", *port)
	}
	log.Println("Press Ctrl+C to stop the server")

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal("Server failed to start: ", err)
	}
}
