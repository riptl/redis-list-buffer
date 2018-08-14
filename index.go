package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

func connectIndex() (err error) {
	db, err = sql.Open("sqlite3", config.SIndex)
	if err != nil {
		return
	}

	_, err = db.Exec(tables)
	if err != nil {
		return
	}

	return
}

func disconnectIndex() {
	db.Close()
}

func newIndex() error {
	_, err := db.Exec("INSERT INTO chunks (size) VALUES (0);")
	return err
}

func deleteIndex(id int64) error {
	_, err := db.Exec("DELETE FROM chunks WHERE id = ?", id)
	return err
}

func rowIndex(query string) (id int64, size int64, err error) {
	row := db.QueryRow(query)
	err = row.Scan(&id, &size)
	if err == sql.ErrNoRows {
		return -1, 0, nil
	}
	return
}

func setSizeIndex(id int64, size int64) (err error) {
	_, err = db.Exec("UPDATE chunks SET size=? WHERE id=?", size, id)
	return
}

func minIndex() (id int64, size int64, err error) {
	return rowIndex("SELECT id, size FROM chunks ORDER BY id ASC;")
}

func maxIndex() (id int64, size int64, err error) {
	return rowIndex("SELECT id, size FROM chunks ORDER BY id DESC")
}

const tables = `
CREATE TABLE IF NOT EXISTS chunks(
	id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
	size INTEGER NOT NULL
);
`
