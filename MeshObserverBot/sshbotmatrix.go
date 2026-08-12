package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
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
var sqliteDBPath string

const (
	maxReplyLen    = 3500
	maxHTTPRead    = 1 << 20
	meshAPIBase    = "https://meshcoretel.ru/api"
	oneMeshAPIBase = "https://map.onemesh.ru/api/v1"
	lineSeparator  = "----------------------"
)

type meshRecord struct {
	PK         int64
	MeshID     string
	DeviceType string
	Name       string
}

type oneMeshRecord struct {
	PK     int64
	NodeID string
	Name   string
}

type observerResp struct {
	ObserverID    string `json:"observer_id"`
	Observer      string `json:"observer"`
	Status        string `json:"status"`
	IsOnline      bool   `json:"is_online"`
	BatteryMV     int64  `json:"battery_mv"`
	UptimeSecs    int64  `json:"uptime_secs"`
	Errors        int64  `json:"errors"`
	QueueLen      int64  `json:"queue_len"`
	LastMessageAt string `json:"last_message_at"`
}

type repeaterDashboardResp struct {
	Repeater struct {
		ID           int64   `json:"id"`
		PublicKeyHex string  `json:"public_key_hex"`
		Name         string  `json:"name"`
		Lat          float64 `json:"lat"`
		Lon          float64 `json:"lon"`
		FirstSeenAt  string  `json:"first_seen_at"`
		LastSeenAt   string  `json:"last_seen_at"`
	} `json:"repeater"`
	ResolvedRegionCode string `json:"resolved_region_code"`
}

type oneMeshNodeResp struct {
	Node *oneMeshNode `json:"node"`
}

type oneMeshNode struct {
	ID                 string  `json:"id"`
	NodeID             string  `json:"node_id"`
	LongName           string  `json:"long_name"`
	ShortName          string  `json:"short_name"`
	BatteryLevel       int     `json:"battery_level"`
	Voltage            *string `json:"voltage"`
	UptimeSeconds      *string `json:"uptime_seconds"`
	Temperature        *string `json:"temperature"`
	RelativeHumidity   *string `json:"relative_humidity"`
	BarometricPressure *string `json:"barometric_pressure"`
	Radiation          *string `json:"radiation"`
	UpdatedAt          string  `json:"updated_at"`
}

type detailResp struct {
	Detail string `json:"detail"`
}

func configBot() {
	cfg := readCfg()
	if len(cfg) < 7 {
		log.Fatal("config error: readCfg returned less than 7 values")
	}

	matrixHomeserver = strings.TrimSpace(cfg[0])
	matrixUsername = strings.TrimSpace(cfg[1])
	matrixPassword = strings.TrimSpace(cfg[2])
	matrixDeviceID = strings.TrimSpace(cfg[3])
	targetRoomID = strings.TrimSpace(cfg[4])
	allowedSenderID = strings.TrimSpace(cfg[5])
	sqliteDBPath = strings.TrimSpace(cfg[6])

	if matrixHomeserver == "" ||
		matrixUsername == "" ||
		matrixPassword == "" ||
		matrixDeviceID == "" ||
		targetRoomID == "" ||
		allowedSenderID == "" ||
		sqliteDBPath == "" {
		log.Fatal("config error: one or more required fields are empty")
	}
}

func main() {
	rootCtx := context.Background()
	configBot()

	db, err := initDB(sqliteDBPath)
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer db.Close()

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

		reply, err := handleCommand(ctx, raw, db)
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
	log.Printf("db: %s", sqliteDBPath)

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
	db *sql.DB,
) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("empty command")
	}

	if strings.HasPrefix(raw, "!") {
		return handleBangCommand(ctx, raw, db)
	}

	switch strings.ToLower(raw) {
	case "ping", "/ping":
		return pingReport(ctx)
	case "help", "/help":
		return strings.Join([]string{
			"Команды:",
			"ping",
			"!add meshcoretel <ID>",
			"!add onemesh <ID>",
			"!list meshcoretel",
			"!list onemesh",
			"!delete meshcoretel <PRIMARY_KEY_ID>",
			"!delete onemesh <PRIMARY_KEY_ID>",
			"!show meshcoretel",
			"!show onemesh",
			"!show all",
		}, "\n"), nil
	default:
		return "", errors.New("unknown command")
	}
}

func handleBangCommand(
	ctx context.Context,
	raw string,
	db *sql.DB,
) (string, error) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) == 0 {
		return "Пустая команда", nil
	}

	cmd := strings.ToLower(strings.TrimPrefix(parts[0], "!"))

	switch cmd {
	case "add":
		if len(parts) < 3 {
			return "Использование: !add <meshcoretel|onemesh> <ID>", nil
		}
		target := strings.ToLower(parts[1])
		idArg := strings.TrimSpace(parts[2])

		switch target {
		case "meshcoretel":
			return addMeshcoretel(ctx, db, idArg)
		case "onemesh":
			return addOnemesh(ctx, db, idArg)
		default:
			return "Неизвестная таблица. Доступно: meshcoretel, onemesh", nil
		}

	case "list":
		if len(parts) < 2 {
			return "Использование: !list <meshcoretel|onemesh>", nil
		}
		target := strings.ToLower(parts[1])

		switch target {
		case "meshcoretel":
			return listMeshcoretel(db)
		case "onemesh":
			return listOnemesh(db)
		default:
			return "Неизвестная таблица. Доступно: meshcoretel, onemesh", nil
		}

	case "delete":
		if len(parts) < 3 {
			return "Использование: !delete <meshcoretel|onemesh> <PRIMARY_KEY_ID>", nil
		}
		target := strings.ToLower(parts[1])
		pk, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil || pk <= 0 {
			return "PRIMARY_KEY_ID должен быть положительным числом", nil
		}

		switch target {
		case "meshcoretel":
			return deleteMeshcoretel(db, pk)
		case "onemesh":
			return deleteOnemesh(db, pk)
		default:
			return "Неизвестная таблица. Доступно: meshcoretel, onemesh", nil
		}

	case "show":
		if len(parts) < 2 {
			return "Использование: !show <meshcoretel|onemesh|all>", nil
		}
		target := strings.ToLower(parts[1])

		switch target {
		case "meshcoretel":
			return showMeshcoretel(ctx, db)
		case "onemesh":
			return showOnemesh(ctx, db)
		case "all":
			return showAll(ctx, db)
		default:
			return "Неизвестная цель. Доступно: meshcoretel, onemesh, all", nil
		}
	default:
		return "Неизвестная команда. Используй: !add, !list, !delete, !show", nil
	}
}

func pingReport(_ context.Context) (string, error) {
	return "pong", nil
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

CREATE TABLE IF NOT EXISTS onemesh (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func addMeshcoretel(ctx context.Context, db *sql.DB, meshID string) (string, error) {
	if meshID == "" {
		return "Использование: !add meshcoretel <ID>", nil
	}

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
		return fmt.Sprintf("Запись meshcoretel с primary key=%d не найдена", pk), nil
	}
	return fmt.Sprintf("Запись meshcoretel с primary key=%d удалена", pk), nil
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
					"[%d] Наблюдатель: %s\nmesh_id: %s\nОшибка получения данных: %v",
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
				fmt.Sprintf("uptime_secs: %s", formatUptimeSecondsObserver(obs.UptimeSecs)),
				fmt.Sprintf("errors: %d", obs.Errors),
				fmt.Sprintf("queue_len: %d", obs.QueueLen),
				fmt.Sprintf("last_message_at: %s",
					formatAPITimeLocal(obs.LastMessageAt)),
			}, "\n"))
		case "repeater":
			rep, err := fetchRepeaterDashboard(ctx, r.MeshID)
			if err != nil {
				blocks = append(blocks, fmt.Sprintf(
					"[%d] Повторитель: %s\nmesh_id: %s\nОшибка получения данных: %v",
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
				fmt.Sprintf("first_seen_at: %s",
					formatAPITimeLocal(rep.Repeater.FirstSeenAt)),
				fmt.Sprintf("last_seen_at: %s",
					formatAPITimeLocal(rep.Repeater.LastSeenAt)),
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

func addOnemesh(ctx context.Context, db *sql.DB, nodeID string) (string, error) {
	if nodeID == "" {
		return "Использование: !add onemesh <ID>", nil
	}

	node, err := fetchOnemeshNode(ctx, nodeID)
	if err != nil {
		return "ID не найден", nil
	}

	name := strings.TrimSpace(node.ShortName + " " + node.LongName)
	if name == "" {
		name = node.NodeID
	}

	res, err := db.Exec(
		`INSERT INTO onemesh(node_id, name) VALUES(?, ?)`,
		node.NodeID, name,
	)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return "ID уже есть в базе", nil
		}
		return "", err
	}

	pk, _ := res.LastInsertId()
	return fmt.Sprintf(
		"Нода %s %s добавлена в базу (primary key=%d)",
		node.ShortName, node.LongName, pk,
	), nil
}

func listOnemesh(db *sql.DB) (string, error) {
	rows, err := db.Query(`SELECT id, node_id, name FROM onemesh ORDER BY id`)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var out []string
	out = append(out, "Список onemesh:")
	hasRows := false

	for rows.Next() {
		hasRows = true
		var r oneMeshRecord
		if err := rows.Scan(&r.PK, &r.NodeID, &r.Name); err != nil {
			return "", err
		}
		out = append(out, fmt.Sprintf("%d) %s, node_id=%s", r.PK, r.Name, r.NodeID))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	if !hasRows {
		return "Таблица onemesh пуста", nil
	}
	return strings.Join(out, "\n"), nil
}

func deleteOnemesh(db *sql.DB, pk int64) (string, error) {
	res, err := db.Exec(`DELETE FROM onemesh WHERE id = ?`, pk)
	if err != nil {
		return "", err
	}
	aff, _ := res.RowsAffected()
	if aff == 0 {
		return fmt.Sprintf("Запись onemesh с primary key=%d не найдена", pk), nil
	}
	return fmt.Sprintf("Запись onemesh с primary key=%d удалена", pk), nil
}

func showOnemesh(ctx context.Context, db *sql.DB) (string, error) {
	rows, err := db.Query(`SELECT id, node_id, name FROM onemesh ORDER BY id`)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var blocks []string

	for rows.Next() {
		var r oneMeshRecord
		if err := rows.Scan(&r.PK, &r.NodeID, &r.Name); err != nil {
			return "", err
		}

		n, err := fetchOnemeshNode(ctx, r.NodeID)
		if err != nil {
			blocks = append(blocks, fmt.Sprintf(
				"[%d] Нода: %s\nnode_id: %s\nОшибка получения данных: %v",
				r.PK, r.Name, r.NodeID, err,
			))
			continue
		}

		barMMHG := "n/a"
		if v, ok := parseFloatPtr(n.BarometricPressure); ok {
			barMMHG = fmt.Sprintf("%.1f мм.рт.ст", v*0.75006156)
		}

		voltage := "n/a"
		if v, ok := parseFloatPtr(n.Voltage); ok {
			voltage = fmt.Sprintf("%.3f V", v)
		}

		temp := "n/a"
		if v, ok := parseFloatPtr(n.Temperature); ok {
			temp = fmt.Sprintf("%.2f °C", v)
		}

		hum := "n/a"
		if v, ok := parseFloatPtr(n.RelativeHumidity); ok {
			hum = fmt.Sprintf("%.2f %%", v)
		}

		rad := "n/a"
		if n.Radiation != nil && strings.TrimSpace(*n.Radiation) != "" {
			rad = strings.TrimSpace(*n.Radiation)
		}

		uptime := formatUptimeSeconds(n.UptimeSeconds)

		blocks = append(blocks, strings.Join([]string{
			fmt.Sprintf("[%d] Нода: %s %s", r.PK, n.ShortName, n.LongName),
			fmt.Sprintf("id: %s", n.ID),
			fmt.Sprintf("node_id: %s", n.NodeID),
			fmt.Sprintf("long_name: %s", n.LongName),
			fmt.Sprintf("battery_level: %d%%", n.BatteryLevel),
			fmt.Sprintf("voltage: %s", voltage),
			fmt.Sprintf("uptime_seconds: %s", uptime),
			fmt.Sprintf("temperature: %s", temp),
			fmt.Sprintf("relative_humidity: %s", hum),
			fmt.Sprintf("barometric_pressure: %s", barMMHG),
			fmt.Sprintf("radiation: %s", rad),
			fmt.Sprintf("updated_at: %s", formatAPITimeLocal(n.UpdatedAt)),
		}, "\n"))
	}

	if err := rows.Err(); err != nil {
		return "", err
	}

	if len(blocks) == 0 {
		return "Таблица onemesh пуста", nil
	}
	return strings.Join(blocks, "\n"+lineSeparator+"\n"), nil
}

func showAll(ctx context.Context, db *sql.DB) (string, error) {
	meshTxt, err := showMeshcoretel(ctx, db)
	if err != nil {
		return "", err
	}
	oneTxt, err := showOnemesh(ctx, db)
	if err != nil {
		return "", err
	}

	var parts []string
	if meshTxt != "Таблица meshcoretel пуста" {
		parts = append(parts, "meshcoretel:\n"+meshTxt)
	}
	if oneTxt != "Таблица onemesh пуста" {
		parts = append(parts, "onemesh:\n"+oneTxt)
	}

	if len(parts) == 0 {
		return "Обе таблицы пусты", nil
	}
	return strings.Join(parts, "\n"+lineSeparator+"\n"), nil
}

func detectMeshcoretelType(
	ctx context.Context,
	meshID string,
) (deviceType string, name string, err error) {
	obsURL := fmt.Sprintf("%s/observers/%s", meshAPIBase, meshID)
	status, body, err := doGET(ctx, obsURL)
	if err != nil {
		return "", "", err
	}

	if status == http.StatusOK {
		var obs observerResp
		if err := json.Unmarshal(body, &obs); err == nil &&
			strings.TrimSpace(obs.Observer) != "" {
			return "observer", obs.Observer, nil
		}
		return "", "", errors.New("invalid observer payload")
	}

	if !isNotFoundPayload(body) {
		return "", "", errors.New("observer response is not expected")
	}

	repURL := fmt.Sprintf("%s/nodes/%s/repeater-dashboard", meshAPIBase, meshID)
	status, body, err = doGET(ctx, repURL)
	if err != nil {
		return "", "", err
	}
	if status == http.StatusOK {
		var rep repeaterDashboardResp
		if err := json.Unmarshal(body, &rep); err == nil &&
			strings.TrimSpace(rep.Repeater.Name) != "" {
			return "repeater", rep.Repeater.Name, nil
		}
		return "", "", errors.New("invalid repeater payload")
	}

	return "", "", errors.New("id not found")
}

func fetchObserver(ctx context.Context, meshID string) (*observerResp, error) {
	u := fmt.Sprintf("%s/observers/%s", meshAPIBase, meshID)
	status, body, err := doGET(ctx, u)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("observer status: %d", status)
	}

	var out observerResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.Observer) == "" {
		return nil, errors.New("empty observer name")
	}
	return &out, nil
}

func fetchRepeaterDashboard(
	ctx context.Context,
	meshID string,
) (*repeaterDashboardResp, error) {
	u := fmt.Sprintf("%s/nodes/%s/repeater-dashboard", meshAPIBase, meshID)
	status, body, err := doGET(ctx, u)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("repeater status: %d", status)
	}

	var out repeaterDashboardResp
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if strings.TrimSpace(out.Repeater.Name) == "" {
		return nil, errors.New("empty repeater name")
	}
	return &out, nil
}

func fetchOnemeshNode(ctx context.Context, nodeID string) (*oneMeshNode, error) {
	u := fmt.Sprintf("%s/nodes/%s", oneMeshAPIBase, nodeID)
	status, body, err := doGET(ctx, u)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, errors.New("not found")
	}

	var resp oneMeshNodeResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, err
	}
	if resp.Node == nil || strings.TrimSpace(resp.Node.NodeID) == "" {
		return nil, errors.New("invalid payload")
	}
	return resp.Node, nil
}

func doGET(ctx context.Context, rawURL string) (int, []byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "matrix-admin-bot/1.0")

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

func isNotFoundPayload(body []byte) bool {
	var d detailResp
	if err := json.Unmarshal(body, &d); err != nil {
		return false
	}
	return strings.Contains(strings.ToLower(d.Detail), "not found")
}

func parseFloatPtr(s *string) (float64, bool) {
	if s == nil {
		return 0, false
	}
	v := strings.TrimSpace(*s)
	if v == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

func formatUptimeSecondsObserver(sec int64) string {
	if sec < 0 {
		sec = 0
	}
	h := sec / 3600
	m := (sec % 3600) / 60
	s := sec % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func formatUptimeSeconds(s *string) string {
	if s == nil {
		return "n/a"
	}
	raw := strings.TrimSpace(*s)
	if raw == "" {
		return "n/a"
	}
	sec, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return raw
	}
	if sec <= 60 {
		return fmt.Sprintf("%d", sec)
	}
	h := sec / 3600
	m := (sec % 3600) / 60
	ss := sec % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, ss)
}

func formatAPITimeLocal(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "n/a"
	}

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999Z07:00",
		"2006-01-02T15:04:05Z07:00",
	}

	for _, layout := range layouts {
		t, err := time.Parse(layout, raw)
		if err == nil {
			return t.In(time.Local).Format("2006-01-02 15:04:05 MST")
		}
	}
	return raw
}

func safe(v string, fallback string) string {
	if strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
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
