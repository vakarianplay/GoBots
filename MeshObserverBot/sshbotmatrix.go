package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

var matrixHomeserver string
var matrixUsername string
var matrixPassword string
var matrixDeviceID string
var targetRoomID string
var allowedSenderID string

const (
	maxReplyLen   = 3500
	maxHTTPRead   = 1 << 20
	dbFile        = "meshcoretel.sqlite3"
	meshAPIBase   = "https://meshcoretel.ru/api"
	lineSeparator = "----------------------"
)

type meshRecord struct {
	PK         int64
	MeshID     string
	DeviceType string
	Name       string
}

type observerResp struct {
	ObserverID      string `json:"observer_id"`
	RegionCode      string `json:"region_code"`
	Status          string `json:"status"`
	IsOnline        bool   `json:"is_online"`
	Observer        string `json:"observer"`
	Model           string `json:"model"`
	FirmwareVersion string `json:"firmware_version"`
	BatteryMV       int64  `json:"battery_mv"`
	UptimeSecs      int64  `json:"uptime_secs"`
	Errors          int64  `json:"errors"`
	QueueLen        int64  `json:"queue_len"`
	LastMessageAt   string `json:"last_message_at"`
	CreatedAt       string `json:"created_at"`
	StatusUpdatedAt string `json:"status_updated_at"`
	Detail          string `json:"detail"`
}

type repeaterDashboardResp struct {
	Repeater struct {
		ID           int64   `json:"id"`
		PublicKeyHex string  `json:"public_key_hex"`
		Name         string  `json:"name"`
		IsRepeater   bool    `json:"is_repeater"`
		Lat          float64 `json:"lat"`
		Lon          float64 `json:"lon"`
		FirstSeenAt  string  `json:"first_seen_at"`
		LastSeenAt   string  `json:"last_seen_at"`
		CreatedAt    string  `json:"created_at"`
	} `json:"repeater"`
	ResolvedRegionCode string `json:"resolved_region_code"`
	ResolvedBy         string `json:"resolved_by"`
	Detail             string `json:"detail"`
}

func configBot() {
	cfg := readCfg()
	if len(cfg) < 6 {
		log.Fatal("config error: readCfg returned less than 6 values")
	}

	matrixHomeserver = strings.TrimSpace(cfg[0])
	matrixUsername = strings.TrimSpace(cfg[1])
	matrixPassword = strings.TrimSpace(cfg[2])
	matrixDeviceID = strings.TrimSpace(cfg[3])
	targetRoomID = strings.TrimSpace(cfg[4])
	allowedSenderID = strings.TrimSpace(cfg[5])

	if matrixHomeserver == "" ||
		matrixUsername == "" ||
		matrixPassword == "" ||
		matrixDeviceID == "" ||
		targetRoomID == "" ||
		allowedSenderID == "" {
		log.Fatal("config error: one or more required fields are empty")
	}
}

func main() {
	rootCtx := context.Background()

	configBot()

	db, err := initDB(dbFile)
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer db.Close()

	homeDir, err := os.UserHomeDir()
	if err != nil {
		log.Fatalf("cannot resolve home dir: %v", err)
	}
	wledScript := filepath.Join(homeDir, ".wled.sh")
	logScript := filepath.Join(homeDir, ".log.sh")

	client, err := mautrix.NewClient(matrixHomeserver, "", "")
	if err != nil {
		log.Fatalf("new client: %v", err)
	}

	loginResp, err := client.Login(rootCtx, &mautrix.ReqLogin{
		Type: "m.login.password",
		Identifier: mautrix.UserIdentifier{
			Type: "m.id.user",
			User: matrixUsername,
		},
		Password:                 matrixPassword,
		DeviceID:                 id.DeviceID(matrixDeviceID),
		InitialDeviceDisplayName: "Matrix Admin Bot",
	})
	if err != nil {
		log.Fatalf("login failed: %v", err)
	}
	client.SetCredentials(loginResp.UserID, loginResp.AccessToken)

	syncer, ok := client.Syncer.(*mautrix.DefaultSyncer)
	if !ok {
		log.Fatal("client syncer is not *mautrix.DefaultSyncer")
	}

	syncer.OnEventType(event.EventMessage, func(
		ctx context.Context,
		evt *event.Event,
	) {
		if evt == nil || evt.Sender == client.UserID {
			return
		}
		if evt.RoomID != id.RoomID(targetRoomID) {
			return
		}
		if evt.Sender != id.UserID(allowedSenderID) {
			return
		}

		msg := evt.Content.AsMessage()
		if msg == nil || msg.MsgType != event.MsgText {
			return
		}

		raw := strings.TrimSpace(msg.Body)
		if raw == "" {
			return
		}

		reply, err := handleCommand(ctx, raw, wledScript, logScript, db)
		if err != nil {
			reply = "Invalid input"
		}
		reply = trimReply(reply, maxReplyLen)

		if err := sendText(ctx, client, evt.RoomID, reply); err != nil {
			log.Printf("send error: %v", err)
		}
	})

	log.Printf("logged in as %s", client.UserID)
	log.Printf("allowed sender: %s", allowedSenderID)
	log.Printf("room: %s", targetRoomID)

	for {
		if err := client.Sync(); err != nil {
			log.Printf("sync error: %v (retry in 5s)", err)
			time.Sleep(5 * time.Second)
		}
	}
}

func handleCommand(
	ctx context.Context,
	raw string,
	wledScript string,
	logScript string,
	db *sql.DB,
) (string, error) {
	cmd, arg := splitCommand(raw)

	switch cmd {
	case "sensors":
		return runCmd(ctx, 8*time.Second, "sensors")
	case "uptime":
		return runCmd(ctx, 5*time.Second, "uptime")
	case "free":
		if strings.TrimSpace(arg) == "-h" {
			return runCmd(ctx, 5*time.Second, "free", "-h")
		}
		return "", errors.New("invalid free args")
	case "ls":
		return runCmd(ctx, 5*time.Second, "ls")
	case "mpc":
		a := strings.TrimSpace(arg)
		if a == "" {
			return runCmd(ctx, 5*time.Second, "mpc")
		}
		if a == "next" {
			return runCmd(ctx, 5*time.Second, "mpc", "next")
		}
		return "", errors.New("invalid mpc args")
	case "wled":
		return runCmd(ctx, 20*time.Second, wledScript)
	case "log":
		return runCmd(ctx, 20*time.Second, logScript)
	case "curl":
		return curlURL(ctx, strings.TrimSpace(arg))
	case "ping":
		return pingReport(ctx)
	case "add_meshcoretel":
		meshID := strings.TrimSpace(arg)
		if meshID == "" {
			return "Использование: /add_meshcoretel <ID>", nil
		}
		return addMeshcoretel(ctx, db, meshID)
	case "list_meshcoretel":
		return listMeshcoretel(db)
	case "delete_meshcoretel":
		pkText := strings.TrimSpace(arg)
		if pkText == "" {
			return "Использование: /delete_meshcoretel <PRIMARY_KEY_ID>", nil
		}
		pk, err := strconv.ParseInt(pkText, 10, 64)
		if err != nil || pk <= 0 {
			return "Ошибка: PRIMARY_KEY_ID должен быть положительным числом", nil
		}
		return deleteMeshcoretel(db, pk)
	case "show_meshcoretel":
		return showMeshcoretel(ctx, db)
	case "help":
		return strings.Join([]string{
			"Available commands:",
			"sensors",
			"uptime",
			"free -h",
			"ls",
			"mpc",
			"mpc next",
			"curl <url>",
			"wled",
			"log",
			"ping",
			"/add_meshcoretel <ID>",
			"/list_meshcoretel",
			"/delete_meshcoretel <PRIMARY_KEY_ID>",
			"/show_meshcoretel",
		}, "\n"), nil
	default:
		return "", errors.New("unknown command")
	}
}

func initDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	schema := `
CREATE TABLE IF NOT EXISTS meshcoretel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mesh_id TEXT NOT NULL UNIQUE,
    device_type TEXT NOT NULL,
    name TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func addMeshcoretel(ctx context.Context, db *sql.DB, meshID string) (string, error) {
	deviceType, name, err := detectMeshcoretelType(ctx, meshID)
	if err != nil {
		return "ID не найден", nil
	}

	res, err := db.Exec(
		`INSERT INTO meshcoretel(mesh_id, device_type, name) VALUES(?,?,?)`,
		meshID, deviceType, name,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			var pk int64
			_ = db.QueryRow(
				`SELECT id FROM meshcoretel WHERE mesh_id = ?`,
				meshID,
			).Scan(&pk)
			if pk > 0 {
				return fmt.Sprintf("ID уже есть в базе (primary key=%d)", pk), nil
			}
			return "ID уже есть в базе", nil
		}
		return "", err
	}

	pk, _ := res.LastInsertId()
	if deviceType == "observer" {
		return fmt.Sprintf(
			"Наблюдатель %s добавлен в базу (primary key=%d)",
			name, pk,
		), nil
	}
	return fmt.Sprintf(
		"Повторитель %s добавлен в базу (primary key=%d)",
		name, pk,
	), nil
}

func listMeshcoretel(db *sql.DB) (string, error) {
	rows, err := db.Query(
		`SELECT id, mesh_id, device_type, name FROM meshcoretel ORDER BY id`,
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var out []string
	out = append(out, "Список meshcoretel:")
	hasRows := false

	for rows.Next() {
		hasRows = true
		var r meshRecord
		if err := rows.Scan(&r.PK, &r.MeshID, &r.DeviceType, &r.Name); err != nil {
			return "", err
		}
		out = append(out, fmt.Sprintf(
			"%d) %s (%s), mesh_id=%s",
			r.PK, r.Name, r.DeviceType, r.MeshID,
		))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	if !hasRows {
		return "Таблица meshcoretel пуста", nil
	}
	return strings.Join(out, "\n"), nil
}

func deleteMeshcoretel(db *sql.DB, pk int64) (string, error) {
	res, err := db.Exec(`DELETE FROM meshcoretel WHERE id = ?`, pk)
	if err != nil {
		return "", err
	}
	aff, _ := res.RowsAffected()
	if aff == 0 {
		return fmt.Sprintf("Запись с primary key=%d не найдена", pk), nil
	}
	return fmt.Sprintf("Запись с primary key=%d удалена", pk), nil
}

func showMeshcoretel(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.Query(
		`SELECT id, mesh_id, device_type, name FROM meshcoretel ORDER BY id`,
	)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var blocks []string
	for rows.Next() {
		var r meshRecord
		if err := rows.Scan(&r.PK, &r.MeshID, &r.DeviceType, &r.Name); err != nil {
			return "", err
		}

		switch r.DeviceType {
		case "observer":
			obs, err := fetchObserver(ctx, r.MeshID)
			if err != nil {
				blocks = append(blocks, fmt.Sprintf(
					"[%d] observer %s\nmesh_id: %s\nОшибка получения данных: %v",
					r.PK, r.Name, r.MeshID, err,
				))
				continue
			}
			blocks = append(blocks, strings.Join([]string{
				fmt.Sprintf("[%d] Наблюдатель: %s", r.PK, safe(obs.Observer, r.Name)),
				fmt.Sprintf("mesh_id: %s", r.MeshID),
				fmt.Sprintf("status: %s", obs.Status),
				fmt.Sprintf("is_online: %t", obs.IsOnline),
				fmt.Sprintf("battery_mv: %d", obs.BatteryMV),
				fmt.Sprintf("uptime_secs: %d", obs.UptimeSecs),
				fmt.Sprintf("errors: %d", obs.Errors),
				fmt.Sprintf("queue_len: %d", obs.QueueLen),
				fmt.Sprintf("last_message_at: %s", obs.LastMessageAt),
			}, "\n"))
		case "repeater":
			rep, err := fetchRepeaterDashboard(ctx, r.MeshID)
			if err != nil {
				blocks = append(blocks, fmt.Sprintf(
					"[%d] repeater %s\nmesh_id: %s\nОшибка получения данных: %v",
					r.PK, r.Name, r.MeshID, err,
				))
				continue
			}
			blocks = append(blocks, strings.Join([]string{
				fmt.Sprintf("[%d] Повторитель: %s",
					r.PK, safe(rep.Repeater.Name, r.Name)),
				fmt.Sprintf("mesh_id: %s", r.MeshID),
				fmt.Sprintf("public_key_hex: %s", rep.Repeater.PublicKeyHex),
				fmt.Sprintf("lat/lon: %.5f, %.5f",
					rep.Repeater.Lat, rep.Repeater.Lon),
				fmt.Sprintf("first_seen_at: %s", rep.Repeater.FirstSeenAt),
				fmt.Sprintf("last_seen_at: %s", rep.Repeater.LastSeenAt),
				fmt.Sprintf("resolved_region_code: %s", rep.ResolvedRegionCode),
			}, "\n"))
		default:
			blocks = append(blocks, fmt.Sprintf(
				"[%d] %s\nmesh_id: %s\nНеизвестный тип устройства: %s",
				r.PK, r.Name, r.MeshID, r.DeviceType,
			))
		}
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	if len(blocks) == 0 {
		return "Таблица meshcoretel пуста", nil
	}
	return strings.Join(blocks, "\n"+lineSeparator+"\n"), nil
}

func detectMeshcoretelType(
	ctx context.Context,
	meshID string,
) (deviceType string, name string, err error) {
	obs, err := fetchObserver(ctx, meshID)
	if err == nil && strings.TrimSpace(obs.Observer) != "" {
		return "observer", obs.Observer, nil
	}

	rep, err := fetchRepeaterDashboard(ctx, meshID)
	if err == nil && strings.TrimSpace(rep.Repeater.Name) != "" {
		return "repeater", rep.Repeater.Name, nil
	}

	return "", "", errors.New("id not found")
}

func fetchObserver(ctx context.Context, meshID string) (*observerResp, error) {
	url := fmt.Sprintf("%s/observers/%s", meshAPIBase, meshID)
	status, body, err := doGET(ctx, url)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		var det observerResp
		if json.Unmarshal(body, &det) == nil &&
			strings.Contains(strings.ToLower(det.Detail), "not found") {
			return nil, errors.New("observer not found")
		}
		return nil, fmt.Errorf("observer http status: %d", status)
	}

	var out observerResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.ObserverID) == "" &&
		strings.TrimSpace(out.Observer) == "" {
		return nil, errors.New("invalid observer payload")
	}
	return &out, nil
}

func fetchRepeaterDashboard(
	ctx context.Context,
	meshID string,
) (*repeaterDashboardResp, error) {
	url := fmt.Sprintf("%s/nodes/%s/repeater-dashboard", meshAPIBase, meshID)
	status, body, err := doGET(ctx, url)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		var det repeaterDashboardResp
		if json.Unmarshal(body, &det) == nil &&
			strings.Contains(strings.ToLower(det.Detail), "not found") {
			return nil, errors.New("repeater not found")
		}
		return nil, fmt.Errorf("repeater http status: %d", status)
	}

	var out repeaterDashboardResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.Repeater.Name) == "" {
		return nil, errors.New("invalid repeater payload")
	}
	return &out, nil
}

func doGET(ctx context.Context, rawURL string) (int, []byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPRead))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, body, nil
}

func safe(v string, fallback string) string {
	if strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func pingReport(ctx context.Context) (string, error) {
	sensorsOut, _ := runCmd(ctx, 8*time.Second, "sensors")
	load1, load5, load15 := readLoadAvg()
	memUsed, memTotal, memPct := readMemUsage()
	uptimeText := readUptimeHuman()

	if strings.TrimSpace(sensorsOut) == "" {
		sensorsOut = "(no output)"
	}

	return fmt.Sprintf(
		"pong\n\nCPU load: %s %s %s\nRAM: %s / %s (%.1f%%)\nUptime: %s\n\nSensors:\n%s",
		load1, load5, load15, memUsed, memTotal, memPct, uptimeText, sensorsOut,
	), nil
}

func runCmd(
	parent context.Context,
	timeout time.Duration,
	name string,
	args ...string,
) (string, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if ctx.Err() == context.DeadlineExceeded {
		return "Command timeout", nil
	}
	if err != nil && len(out) == 0 {
		return "", err
	}
	if len(out) == 0 {
		return "(no output)", nil
	}
	return string(out), nil
}

func curlURL(parent context.Context, rawURL string) (string, error) {
	if rawURL == "" {
		return "", errors.New("empty url")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("only http/https allowed")
	}
	if u.Host == "" {
		return "", errors.New("invalid host")
	}

	ctx, cancel := context.WithTimeout(parent, 12*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Sprintf("curl error: %v", err), nil
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxHTTPRead))
	if err != nil {
		return fmt.Sprintf("read error: %v", err), nil
	}

	text := strings.TrimSpace(string(body))
	if text == "" {
		text = "(no output)"
	}

	return fmt.Sprintf("HTTP %d\n%s", resp.StatusCode, text), nil
}

func splitCommand(raw string) (string, string) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", ""
	}
	parts := strings.SplitN(s, " ", 2)
	cmd := strings.ToLower(strings.TrimSpace(parts[0]))
	cmd = strings.TrimPrefix(cmd, "/")
	if len(parts) == 1 {
		return cmd, ""
	}
	return cmd, parts[1]
}

func readLoadAvg() (string, string, string) {
	b, err := os.ReadFile("/proc/loadavg")
	if err != nil {
		return "n/a", "n/a", "n/a"
	}
	fields := strings.Fields(string(b))
	if len(fields) < 3 {
		return "n/a", "n/a", "n/a"
	}
	return fields[0], fields[1], fields[2]
}

func readMemUsage() (used string, total string, pct float64) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return "n/a", "n/a", 0
	}
	defer f.Close()

	var memTotalKB float64
	var memAvailKB float64

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "MemTotal:") {
			memTotalKB = parseMeminfoKB(line)
		}
		if strings.HasPrefix(line, "MemAvailable:") {
			memAvailKB = parseMeminfoKB(line)
		}
	}

	if memTotalKB <= 0 {
		return "n/a", "n/a", 0
	}

	usedKB := memTotalKB - memAvailKB
	if usedKB < 0 {
		usedKB = 0
	}

	pct = (usedKB / memTotalKB) * 100.0
	return formatKB(usedKB), formatKB(memTotalKB), pct
}

func parseMeminfoKB(line string) float64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	v, err := strconv.ParseFloat(fields[1], 64)
	if err != nil {
		return 0
	}
	return v
}

func formatKB(kb float64) string {
	const kbInGiB = 1024 * 1024
	const kbInMiB = 1024

	if kb >= kbInGiB {
		return fmt.Sprintf("%.2f GiB", kb/kbInGiB)
	}
	return fmt.Sprintf("%.0f MiB", kb/kbInMiB)
}

func readUptimeHuman() string {
	b, err := os.ReadFile("/proc/uptime")
	if err != nil {
		return "n/a"
	}
	fields := strings.Fields(string(b))
	if len(fields) == 0 {
		return "n/a"
	}
	seconds, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return "n/a"
	}
	return humanDuration(time.Duration(seconds) * time.Second)
}

func humanDuration(d time.Duration) string {
	totalSec := int(d.Seconds())
	days := totalSec / 86400
	totalSec %= 86400
	hours := totalSec / 3600
	totalSec %= 3600
	mins := totalSec / 60
	secs := totalSec % 60

	if days > 0 {
		return fmt.Sprintf("%dd %02dh %02dm %02ds", days, hours, mins, secs)
	}
	return fmt.Sprintf("%02dh %02dm %02ds", hours, mins, secs)
}

func sendText(
	ctx context.Context,
	client *mautrix.Client,
	roomID id.RoomID,
	text string,
) error {
	_, err := client.SendMessageEvent(
		ctx,
		roomID,
		event.EventMessage,
		&event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    text,
		},
	)
	return err
}

func trimReply(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n...output truncated..."
}
