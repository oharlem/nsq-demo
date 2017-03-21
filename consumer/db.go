package main

import (
	"fmt"

	"github.com/jmoiron/sqlx"
)

func connect(dbName, host, port string) *sqlx.DB {
	connectionString := "root:root@tcp(" + host + ":" + port + ")/" + dbName
	db, err := sqlx.Open("mysql", connectionString)
	if err != nil {
		fmt.Printf("openind db: error: %s\n", err)
		panic(err)
	}

	db.SetMaxOpenConns(4)

	err = db.Ping()
	if err != nil {
		fmt.Printf("pinging db: error: %s\n", err)
		panic(err)
	}

	return db
}

func initDB() {
	_, err := db.Exec(`DROP DATABASE IF EXISTS stream`)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`CREATE DATABASE stream`)
	if err != nil {
		panic(err)
	}
}

func initStructure() {
	_, err := db.Exec(`CREATE TABLE users (
  user_id int(11) NOT NULL,
  country varchar(2) DEFAULT NULL,
  ip varchar(15) DEFAULT NULL,
  timestamp varchar(64) DEFAULT NULL,
  PRIMARY KEY (user_id),
  KEY country (country)
)`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`CREATE TABLE videos (
  video_id int(11) NOT NULL,
  timestamp varchar(64) DEFAULT NULL,
  user_id int(11) DEFAULT NULL,
  PRIMARY KEY (video_id),
  KEY user_id (user_id)
)`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`CREATE TABLE leaderboard (
  id int(11) NOT NULL DEFAULT 0,
  most_watched_video_id int(11) DEFAULT NULL,
  most_watched_video_count int(11) DEFAULT 0,
  PRIMARY KEY (id)
)`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`CREATE TABLE views (
  user_id int(11) NOT NULL,
  video_id int(11) NOT NULL DEFAULT 0,
  timestamp varchar(64) NOT NULL DEFAULT 0,
  PRIMARY KEY (timestamp, video_id),
  KEY video_id (video_id)
)`)
	if err != nil {
		panic(err)
	}

	_, err = db.Exec(`INSERT INTO leaderboard (id, most_watched_video_id, most_watched_video_count) VALUES (1, 0, 0)`)
	if err != nil {
		panic(err)
	}
}
