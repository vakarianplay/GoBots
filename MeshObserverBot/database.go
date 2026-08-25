// database.go
package main

import (
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

type MeshNodeRecord struct {
	ID         int64
	MeshID     string
	DeviceType string
	Name       string
	AddedBy    string
	CreatedAt  string
}

type OneMeshNodeRecord struct {
	ID        int64
	NodeID    string
	ShortName string
	LongName  string
	AddedBy   string
	CreatedAt string
}

type DBStore struct {
	db *sql.DB
}

func NewDBStore(path string) (*DBStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	if err := createTables(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := migrateLegacyUniqueIfNeeded(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := ensureColumn(
		db,
		"meshcoretel",
		"added_by",
		"ALTER TABLE meshcoretel ADD COLUMN added_by TEXT NOT NULL DEFAULT ''",
	); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := ensureColumn(
		db,
		"onemesh",
		"short_name",
		"ALTER TABLE onemesh ADD COLUMN short_name TEXT NOT NULL DEFAULT ''",
	); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureColumn(
		db,
		"onemesh",
		"long_name",
		"ALTER TABLE onemesh ADD COLUMN long_name TEXT NOT NULL DEFAULT ''",
	); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureColumn(
		db,
		"onemesh",
		"added_by",
		"ALTER TABLE onemesh ADD COLUMN added_by TEXT NOT NULL DEFAULT ''",
	); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := createIndexes(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &DBStore{db: db}, nil
}

func (s *DBStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *DBStore) AddMeshNode(
	owner string,
	meshID string,
	deviceType string,
	name string,
) (pk int64, duplicate bool, err error) {
	res, err := s.db.Exec(
		`INSERT INTO meshcoretel(mesh_id, device_type, name, added_by)
		 VALUES(?,?,?,?)`,
		meshID, deviceType, name, owner,
	)
	if err != nil {
		if isUniqueErr(err) {
			return 0, true, nil
		}
		return 0, false, err
	}
	id, _ := res.LastInsertId()
	return id, false, nil
}

func (s *DBStore) AddOneMeshNode(
	owner string,
	nodeID string,
	shortName string,
	longName string,
) (pk int64, duplicate bool, err error) {
	res, err := s.db.Exec(
		`INSERT INTO onemesh(node_id, short_name, long_name, added_by)
		 VALUES(?,?,?,?)`,
		nodeID, shortName, longName, owner,
	)
	if err != nil {
		if isUniqueErr(err) {
			return 0, true, nil
		}
		return 0, false, err
	}
	id, _ := res.LastInsertId()
	return id, false, nil
}

func (s *DBStore) ListMeshNodes(owner string) ([]MeshNodeRecord, error) {
	rows, err := s.db.Query(
		`SELECT id, mesh_id, device_type, name, added_by, created_at
		 FROM meshcoretel
		 WHERE added_by = ?
		 ORDER BY id`,
		owner,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []MeshNodeRecord
	for rows.Next() {
		var r MeshNodeRecord
		if err := rows.Scan(
			&r.ID, &r.MeshID, &r.DeviceType, &r.Name, &r.AddedBy, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DBStore) ListOneMeshNodes(owner string) ([]OneMeshNodeRecord, error) {
	rows, err := s.db.Query(
		`SELECT id, node_id, short_name, long_name, added_by, created_at
		 FROM onemesh
		 WHERE added_by = ?
		 ORDER BY id`,
		owner,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []OneMeshNodeRecord
	for rows.Next() {
		var r OneMeshNodeRecord
		if err := rows.Scan(
			&r.ID, &r.NodeID, &r.ShortName, &r.LongName, &r.AddedBy, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *DBStore) DeleteMeshNode(owner string, pk int64) (bool, error) {
	res, err := s.db.Exec(
		`DELETE FROM meshcoretel WHERE id = ? AND added_by = ?`,
		pk, owner,
	)
	if err != nil {
		return false, err
	}
	aff, _ := res.RowsAffected()
	return aff > 0, nil
}

func (s *DBStore) DeleteOneMeshNode(owner string, pk int64) (bool, error) {
	res, err := s.db.Exec(
		`DELETE FROM onemesh WHERE id = ? AND added_by = ?`,
		pk, owner,
	)
	if err != nil {
		return false, err
	}
	aff, _ := res.RowsAffected()
	return aff > 0, nil
}

func (s *DBStore) CountMeshNodes(owner string) (int, error) {
	var n int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM meshcoretel WHERE added_by = ?`,
		owner,
	).Scan(&n)
	return n, err
}

func (s *DBStore) CountOneMeshNodes(owner string) (int, error) {
	var n int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM onemesh WHERE added_by = ?`,
		owner,
	).Scan(&n)
	return n, err
}

func createTables(db *sql.DB) error {
	schema := `
CREATE TABLE IF NOT EXISTS meshcoretel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mesh_id TEXT NOT NULL,
    device_type TEXT NOT NULL,
    name TEXT NOT NULL,
    added_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS onemesh (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    short_name TEXT NOT NULL DEFAULT '',
    long_name TEXT NOT NULL DEFAULT '',
    added_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`
	_, err := db.Exec(schema)
	return err
}

func createIndexes(db *sql.DB) error {
	indexes := `
CREATE UNIQUE INDEX IF NOT EXISTS idx_meshcoretel_owner_mesh
ON meshcoretel(added_by, mesh_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_onemesh_owner_node
ON onemesh(added_by, node_id);
`
	_, err := db.Exec(indexes)
	return err
}

func ensureColumn(db *sql.DB, table string, column string, alterSQL string) error {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if strings.EqualFold(name, column) {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	_, err = db.Exec(alterSQL)
	return err
}

func migrateLegacyUniqueIfNeeded(db *sql.DB) error {
	meshSQL, _ := getTableSQL(db, "meshcoretel")
	oneSQL, _ := getTableSQL(db, "onemesh")

	meshLegacy := strings.Contains(strings.ToLower(meshSQL), "mesh_id text not null unique")
	oneLegacy := strings.Contains(strings.ToLower(oneSQL), "node_id text not null unique")

	if !meshLegacy && !oneLegacy {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if meshLegacy {
		if err = migrateMeshcoretelLegacy(tx); err != nil {
			return err
		}
	}
	if oneLegacy {
		if err = migrateOnemeshLegacy(tx); err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

func migrateMeshcoretelLegacy(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE meshcoretel RENAME TO meshcoretel_legacy`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
CREATE TABLE meshcoretel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mesh_id TEXT NOT NULL,
    device_type TEXT NOT NULL,
    name TEXT NOT NULL,
    added_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`)
	if err != nil {
		return err
	}

	hasAddedBy, _ := txHasColumn(tx, "meshcoretel_legacy", "added_by")
	hasCreatedAt, _ := txHasColumn(tx, "meshcoretel_legacy", "created_at")
	hasDeviceType, _ := txHasColumn(tx, "meshcoretel_legacy", "device_type")
	hasName, _ := txHasColumn(tx, "meshcoretel_legacy", "name")

	addedByExpr := "''"
	if hasAddedBy {
		addedByExpr = "added_by"
	}

	createdAtExpr := "CURRENT_TIMESTAMP"
	if hasCreatedAt {
		createdAtExpr = "created_at"
	}

	deviceTypeExpr := "''"
	if hasDeviceType {
		deviceTypeExpr = "device_type"
	}

	nameExpr := "''"
	if hasName {
		nameExpr = "name"
	}

	q := fmt.Sprintf(`
INSERT INTO meshcoretel(id, mesh_id, device_type, name, added_by, created_at)
SELECT id, mesh_id, %s, %s, %s, %s
FROM meshcoretel_legacy;
`, deviceTypeExpr, nameExpr, addedByExpr, createdAtExpr)

	_, err = tx.Exec(q)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DROP TABLE meshcoretel_legacy`)
	return err
}

func migrateOnemeshLegacy(tx *sql.Tx) error {
	_, err := tx.Exec(`ALTER TABLE onemesh RENAME TO onemesh_legacy`)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
CREATE TABLE onemesh (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    short_name TEXT NOT NULL DEFAULT '',
    long_name TEXT NOT NULL DEFAULT '',
    added_by TEXT NOT NULL DEFAULT '',
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
`)
	if err != nil {
		return err
	}

	hasShort, _ := txHasColumn(tx, "onemesh_legacy", "short_name")
	hasLong, _ := txHasColumn(tx, "onemesh_legacy", "long_name")
	hasName, _ := txHasColumn(tx, "onemesh_legacy", "name")
	hasAddedBy, _ := txHasColumn(tx, "onemesh_legacy", "added_by")
	hasCreatedAt, _ := txHasColumn(tx, "onemesh_legacy", "created_at")

	shortExpr := "''"
	if hasShort {
		shortExpr = "short_name"
	} else if hasName {
		shortExpr = "name"
	}

	longExpr := "''"
	if hasLong {
		longExpr = "long_name"
	}

	addedByExpr := "''"
	if hasAddedBy {
		addedByExpr = "added_by"
	}

	createdAtExpr := "CURRENT_TIMESTAMP"
	if hasCreatedAt {
		createdAtExpr = "created_at"
	}

	q := fmt.Sprintf(`
INSERT INTO onemesh(id, node_id, short_name, long_name, added_by, created_at)
SELECT id, node_id, %s, %s, %s, %s
FROM onemesh_legacy;
`, shortExpr, longExpr, addedByExpr, createdAtExpr)

	_, err = tx.Exec(q)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`DROP TABLE onemesh_legacy`)
	return err
}

func txHasColumn(tx *sql.Tx, table string, column string) (bool, error) {
	rows, err := tx.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	return false, rows.Err()
}

func getTableSQL(db *sql.DB, table string) (string, error) {
	var sqlText sql.NullString
	err := db.QueryRow(
		`SELECT sql FROM sqlite_master WHERE type='table' AND name=?`,
		table,
	).Scan(&sqlText)
	if err != nil {
		return "", err
	}
	return sqlText.String, nil
}

func isUniqueErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "unique") ||
		strings.Contains(s, "constraint failed")
}
