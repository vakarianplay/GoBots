// main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

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
)

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

type progressTracker struct {
	ctx    context.Context
	client *mautrix.Client
	roomID id.RoomID
	msgID  id.EventID
	total  int
	done   int
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
		sqliteDBPath == "" {
		log.Fatal("config error: required fields are empty")
	}
}

func main() {
	rootCtx := context.Background()
	configBot()

	store, err := NewDBStore(sqliteDBPath)
	if err != nil {
		log.Fatalf("init db: %v", err)
	}
	defer store.Close()

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
		if allowedSenderID != "" &&
			allowedSenderID != "*" &&
			evt.Sender != id.UserID(allowedSenderID) {
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

		owner := ownerKey(evt.Sender)

		if target, ok := parseShowTarget(raw); ok {
			err := handleShowWithProgress(ctx, client, evt.RoomID, store, owner, target)
			if err != nil {
				_, _ = sendText(ctx, client, evt.RoomID, "❌ Ошибка формирования отчёта")
			}
			return
		}

		reply, err := handleCommand(ctx, raw, store, owner)
		if err != nil {
			reply = "Invalid input"
		}
		reply = trimReply(reply, maxReplyLen)

		if _, err := sendText(ctx, client, evt.RoomID, reply); err != nil {
			log.Printf("send error: %v", err)
		}
	})

	log.Printf("logged in as %s", client.UserID)
	log.Printf("room: %s", targetRoomID)
	log.Printf("db: %s", sqliteDBPath)
	log.Printf("allowed user: %s", safe(allowedSenderID, "*"))

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
	store *DBStore,
	owner string,
) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", errors.New("empty command")
	}

	if strings.HasPrefix(raw, "!") {
		return handleBangCommand(ctx, raw, store, owner)
	}

	switch strings.ToLower(raw) {
	case "ping", "/ping":
		meshStatus := serviceAvailability(ctx, meshAPIBase)
		oneStatus := serviceAvailability(ctx, oneMeshAPIBase)
		return strings.Join([]string{
			"pong",
			fmt.Sprintf("meshcoretel: %s", meshStatus),
			fmt.Sprintf("onemesh: %s", oneStatus),
		}, "\n"), nil

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
	store *DBStore,
	owner string,
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
			return addMeshcoretel(ctx, store, idArg, owner)
		case "onemesh":
			return addOnemesh(ctx, store, idArg, owner)
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
			return listMeshcoretel(store, owner)
		case "onemesh":
			return listOnemesh(store, owner)
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
			deleted, err := store.DeleteMeshNode(owner, pk)
			if err != nil {
				return "", err
			}
			if !deleted {
				return fmt.Sprintf("Запись meshcoretel id=%d не найдена среди ваших", pk), nil
			}
			return fmt.Sprintf("Запись meshcoretel id=%d удалена", pk), nil

		case "onemesh":
			deleted, err := store.DeleteOneMeshNode(owner, pk)
			if err != nil {
				return "", err
			}
			if !deleted {
				return fmt.Sprintf("Запись onemesh id=%d не найдена среди ваших", pk), nil
			}
			return fmt.Sprintf("Запись onemesh id=%d удалена", pk), nil

		default:
			return "Неизвестная таблица. Доступно: meshcoretel, onemesh", nil
		}

	case "show":
		if len(parts) < 2 {
			return "Использование: !show <meshcoretel|onemesh|all>", nil
		}
		return "⏳ Формирование отчёта выполняется...", nil

	default:
		return "Неизвестная команда. Используй: !add, !list, !delete, !show", nil
	}
}

func parseShowTarget(raw string) (string, bool) {
	parts := strings.Fields(strings.TrimSpace(raw))
	if len(parts) < 2 {
		return "", false
	}
	if strings.ToLower(strings.TrimPrefix(parts[0], "!")) != "show" {
		return "", false
	}
	target := strings.ToLower(parts[1])
	switch target {
	case "meshcoretel", "onemesh", "all":
		return target, true
	default:
		return "", false
	}
}

func handleShowWithProgress(
	ctx context.Context,
	client *mautrix.Client,
	roomID id.RoomID,
	store *DBStore,
	owner string,
	target string,
) error {
	total, err := countForTarget(store, owner, target)
	if err != nil {
		return err
	}

	if total == 0 {
		switch target {
		case "meshcoretel":
			_, _ = sendText(ctx, client, roomID, "📭 У вас нет записей в meshcoretel")
		case "onemesh":
			_, _ = sendText(ctx, client, roomID, "📭 У вас нет записей в onemesh")
		case "all":
			_, _ = sendText(ctx, client, roomID, "📭 У вас нет записей ни в одной таблице")
		}
		return nil
	}

	msgID, err := sendText(
		ctx,
		client,
		roomID,
		fmt.Sprintf("⏳ Формируется отчёт, пожалуйста подождите (1/%d)", total),
	)
	if err != nil {
		return err
	}

	tr := &progressTracker{
		ctx:    ctx,
		client: client,
		roomID: roomID,
		msgID:  msgID,
		total:  total,
		done:   0,
	}
	firstReq := true

	var cards []string
	switch target {
	case "meshcoretel":
		cards, err = buildMeshcoretelCards(ctx, store, owner, tr, &firstReq)
	case "onemesh":
		cards, err = buildOnemeshCards(ctx, store, owner, tr, &firstReq)
	case "all":
		cards, err = buildAllCards(ctx, store, owner, tr, &firstReq)
	default:
		err = errors.New("unknown show target")
	}
	if err != nil {
		_ = editText(ctx, client, roomID, msgID, "❌ Ошибка формирования отчёта")
		return err
	}

	_ = editText(ctx, client, roomID, msgID, "✅ Отчёт сформирован")

	for _, card := range cards {
		_, _ = sendText(ctx, client, roomID, trimReply(card, maxReplyLen))
	}

	return nil
}

func (p *progressTracker) step() {
	p.done++
	current := p.done
	if current < 1 {
		current = 1
	}
	if current > p.total {
		current = p.total
	}
	_ = editText(
		p.ctx,
		p.client,
		p.roomID,
		p.msgID,
		fmt.Sprintf("⏳ Формируется отчёт, пожалуйста подождите (%d/%d)", current, p.total),
	)
}

func waitRateLimit(ctx context.Context, first *bool) error {
	if *first {
		*first = false
		return nil
	}
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func buildAllCards(
	ctx context.Context,
	store *DBStore,
	owner string,
	tr *progressTracker,
	firstReq *bool,
) ([]string, error) {
	meshCards, err := buildMeshcoretelCards(ctx, store, owner, tr, firstReq)
	if err != nil {
		return nil, err
	}
	oneCards, err := buildOnemeshCards(ctx, store, owner, tr, firstReq)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(meshCards)+len(oneCards))
	out = append(out, meshCards...)
	out = append(out, oneCards...)
	return out, nil
}

func buildMeshcoretelCards(
	ctx context.Context,
	store *DBStore,
	owner string,
	tr *progressTracker,
	firstReq *bool,
) ([]string, error) {
	records, err := store.ListMeshNodes(owner)
	if err != nil {
		return nil, err
	}

	var cards []string
	for _, r := range records {
		if err := waitRateLimit(ctx, firstReq); err != nil {
			return nil, err
		}

		switch r.DeviceType {
		case "observer":
			obs, err := fetchObserver(ctx, r.MeshID)
			if err != nil {
				cards = append(cards, strings.Join([]string{
					"🛰️ MeshCoreTel • Наблюдатель",
					// fmt.Sprintf("┌ ID в БД: %d", r.ID),
					fmt.Sprintf("┌ Имя: %s", r.Name),
					fmt.Sprintf("├ Mesh ID: %s", r.MeshID),
					fmt.Sprintf("└ ❌ Ошибка получения данных: %v", err),
				}, "\n"))
			} else {
				cards = append(cards, strings.Join([]string{
					"🛰️ MeshCoreTel • Наблюдатель",
					// fmt.Sprintf("┌ ID в БД: %d", r.ID),
					fmt.Sprintf("┌ Имя: %s", safe(obs.Observer, r.Name)),
					fmt.Sprintf("├ Mesh ID: %s", r.MeshID),
					fmt.Sprintf("├ Статус: %s", obs.Status),
					fmt.Sprintf("├ Онлайн: %t", obs.IsOnline),
					fmt.Sprintf("├ 🔋 Батарея: %d mV", obs.BatteryMV),
					fmt.Sprintf("├ ⏱ Uptime: %s", formatSecondsHHMMSS(obs.UptimeSecs)),
					fmt.Sprintf("├ ⚠ Ошибки: %d", obs.Errors),
					fmt.Sprintf("├ 📬 Очередь: %d", obs.QueueLen),
					fmt.Sprintf("└ 🕒 Последнее сообщение: %s",
						formatAPITimeLocal(obs.LastMessageAt)),
				}, "\n"))
			}

		case "repeater":
			rep, err := fetchRepeaterDashboard(ctx, r.MeshID)
			if err != nil {
				cards = append(cards, strings.Join([]string{
					"🔁 MeshCoreTel • Повторитель",
					fmt.Sprintf("┌ ID в БД: %d", r.ID),
					fmt.Sprintf("├ Имя: %s", r.Name),
					fmt.Sprintf("├ Mesh ID: %s", r.MeshID),
					fmt.Sprintf("└ ❌ Ошибка получения данных: %v", err),
				}, "\n"))
			} else {
				cards = append(cards, strings.Join([]string{
					"🔁 MeshCoreTel • Повторитель",
					// fmt.Sprintf("┌ ID в БД: %d", r.ID),
					fmt.Sprintf("┌ Имя: %s", safe(rep.Repeater.Name, r.Name)),
					fmt.Sprintf("├ Mesh ID: %s", r.MeshID),
					fmt.Sprintf("├ 🔑 Public key: %s", rep.Repeater.PublicKeyHex),
					fmt.Sprintf("├ 📍 Координаты: %.5f, %.5f",
						rep.Repeater.Lat, rep.Repeater.Lon),
					fmt.Sprintf("├ 🕒 First seen: %s",
						formatAPITimeLocal(rep.Repeater.FirstSeenAt)),
					fmt.Sprintf("├ 🕒 Last seen: %s",
						formatAPITimeLocal(rep.Repeater.LastSeenAt)),
					fmt.Sprintf("└ 🌍 Region: %s", rep.ResolvedRegionCode),
				}, "\n"))
			}

		default:
			cards = append(cards, strings.Join([]string{
				"❓ MeshCoreTel • Неизвестный тип",
				// fmt.Sprintf("┌ ID в БД: %d", r.ID),
				fmt.Sprintf("┌ Имя: %s", r.Name),
				fmt.Sprintf("├ Mesh ID: %s", r.MeshID),
				fmt.Sprintf("└ Тип: %s", r.DeviceType),
			}, "\n"))
		}

		if tr != nil {
			tr.step()
		}
	}

	return cards, nil
}

func buildOnemeshCards(
	ctx context.Context,
	store *DBStore,
	owner string,
	tr *progressTracker,
	firstReq *bool,
) ([]string, error) {
	records, err := store.ListOneMeshNodes(owner)
	if err != nil {
		return nil, err
	}

	var cards []string
	for _, r := range records {
		if err := waitRateLimit(ctx, firstReq); err != nil {
			return nil, err
		}

		n, err := fetchOnemeshNode(ctx, r.NodeID)
		if err != nil {
			cards = append(cards, strings.Join([]string{
				"📡 OneMesh • Нода",
				// fmt.Sprintf("┌ ID в БД: %d", r.ID),
				fmt.Sprintf("┌ Имя: %s %s", r.ShortName, r.LongName),
				fmt.Sprintf("├ Node ID: %s", r.NodeID),
				fmt.Sprintf("└ ❌ Ошибка получения данных: %v", err),
			}, "\n"))
			if tr != nil {
				tr.step()
			}
			continue
		}

		barMMHG := "n/a"
		if v, ok := parseFloatPtr(n.BarometricPressure); ok {
			barMMHG = fmt.Sprintf("%.1f мм рт. ст.", v*0.75006156)
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

		cards = append(cards, strings.Join([]string{
			"📡 OneMesh • Нода",
			// fmt.Sprintf("┌ ID в БД: %d", r.ID),
			fmt.Sprintf("┌ ID: %s", n.ID),
			fmt.Sprintf("├ Node ID: %s", n.NodeID),
			fmt.Sprintf("├ Имя: %s %s", n.ShortName, n.LongName),
			fmt.Sprintf("├ 🔋 Батарея: %d%%", n.BatteryLevel),
			fmt.Sprintf("├ 🔌 Напряжение: %s", voltage),
			fmt.Sprintf("├ ⏱ Uptime: %s", uptime),
			fmt.Sprintf("├ 🌡 Температура: %s", temp),
			fmt.Sprintf("├ 💧 Влажность: %s", hum),
			fmt.Sprintf("├ 🧭 Давление: %s", barMMHG),
			fmt.Sprintf("├ ☢ Радиация: %s", rad),
			fmt.Sprintf("└ 🕒 Обновлено: %s", formatAPITimeLocal(n.UpdatedAt)),
		}, "\n"))

		if tr != nil {
			tr.step()
		}
	}

	return cards, nil
}

func countForTarget(store *DBStore, owner string, target string) (int, error) {
	switch target {
	case "meshcoretel":
		return store.CountMeshNodes(owner)
	case "onemesh":
		return store.CountOneMeshNodes(owner)
	case "all":
		a, err := store.CountMeshNodes(owner)
		if err != nil {
			return 0, err
		}
		b, err := store.CountOneMeshNodes(owner)
		if err != nil {
			return 0, err
		}
		return a + b, nil
	default:
		return 0, errors.New("unknown target")
	}
}

func addMeshcoretel(
	ctx context.Context,
	store *DBStore,
	meshID string,
	owner string,
) (string, error) {
	if meshID == "" {
		return "Использование: !add meshcoretel <ID>", nil
	}

	deviceType, name, err := detectMeshcoretelType(ctx, meshID)
	if err != nil {
		return "ID не найден", nil
	}

	_, duplicate, err := store.AddMeshNode(owner, meshID, deviceType, name)
	if err != nil {
		return "", err
	}
	if duplicate {
		return "Эта нода уже есть у вас в базе", nil
	}

	if deviceType == "observer" {
		return fmt.Sprintf(
			"Наблюдатель %s добавлен в базу",
			name,
		), nil
	}
	return fmt.Sprintf(
		"Повторитель %s добавлен в базу",
		name,
	), nil
}

func addOnemesh(
	ctx context.Context,
	store *DBStore,
	nodeID string,
	owner string,
) (string, error) {
	if nodeID == "" {
		return "Использование: !add onemesh <ID>", nil
	}

	node, err := fetchOnemeshNode(ctx, nodeID)
	if err != nil {
		return "ID не найден", nil
	}

	pk, duplicate, err := store.AddOneMeshNode(owner, node.NodeID, node.ShortName, node.LongName)
	if err != nil {
		return "", err
	}
	if duplicate {
		return "Эта нода уже есть у вас в базе", nil
	}

	return fmt.Sprintf(
		"Нода %s %s добавлена в базу (primary key=%d)",
		node.ShortName, node.LongName, pk,
	), nil
}

func listMeshcoretel(store *DBStore, owner string) (string, error) {
	records, err := store.ListMeshNodes(owner)
	if err != nil {
		return "", err
	}
	if len(records) == 0 {
		return "У вас нет записей в meshcoretel", nil
	}

	out := []string{"Список ваших meshcoretel:"}
	for _, r := range records {
		out = append(out, fmt.Sprintf(
			"%d) %s (%s), mesh_id=%s",
			r.ID, r.Name, r.DeviceType, r.MeshID,
		))
	}
	return strings.Join(out, "\n"), nil
}

func listOnemesh(store *DBStore, owner string) (string, error) {
	records, err := store.ListOneMeshNodes(owner)
	if err != nil {
		return "", err
	}
	if len(records) == 0 {
		return "У вас нет записей в onemesh", nil
	}

	out := []string{"Список ваших onemesh:"}
	for _, r := range records {
		out = append(out, fmt.Sprintf(
			"%d) %s %s, node_id=%s",
			r.ID, strings.TrimSpace(r.ShortName), strings.TrimSpace(r.LongName), r.NodeID,
		))
	}
	return strings.Join(out, "\n"), nil
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

func serviceAvailability(parent context.Context, rawURL string) string {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "недоступен"
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "недоступен"
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 500 {
		return "недоступен"
	}
	return "доступен"
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

func formatSecondsHHMMSS(sec int64) string {
	if sec < 0 {
		sec = 0
	}
	h := sec / 3600
	m := (sec % 3600) / 60
	s := sec % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
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

func ownerKey(userID id.UserID) string {
	s := strings.TrimSpace(string(userID))
	if s == "" {
		return "unknown"
	}
	return s
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
) (id.EventID, error) {
	resp, err := client.SendMessageEvent(
		ctx,
		roomID,
		event.EventMessage,
		&event.MessageEventContent{
			MsgType: event.MsgText,
			Body:    text,
		},
	)
	if err != nil {
		return "", err
	}
	return resp.EventID, nil
}

func editText(
	ctx context.Context,
	client *mautrix.Client,
	roomID id.RoomID,
	targetEventID id.EventID,
	text string,
) error {
	content := &event.MessageEventContent{
		MsgType: event.MsgText,
		Body:    text,
	}
	content.SetEdit(targetEventID)

	_, err := client.SendMessageEvent(
		ctx,
		roomID,
		event.EventMessage,
		content,
	)
	return err
}

func trimReply(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "\n...output truncated..."
}
