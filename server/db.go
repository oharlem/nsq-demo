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
