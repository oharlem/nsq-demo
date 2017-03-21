package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/nsqio/go-nsq"
)

const (
	// NSQ config.
	addr        = "127.0.0.1:4150"
	eventsTopic = "events"

	// Script progress output.
	outputInterval = 1000

	sessStartMsg = "SESS_START"
	sessEndMsg   = "SESS_END"
)

var (
	db *sqlx.DB

	mysqlHost *string
	mysqlPort *string

	errInvalidInput = errors.New("Invalid line input")

	// Event types.
	typeRegister = "REGISTER"
	typeUpload   = "UPLOAD"
	typeWatch    = "WATCH"

	// Service messages.
	svcMsgs = map[string]struct{}{
		"SESS_START": {},
		"SESS_END":   {},
	}
)

// Init initializes script arguments, connects to the database
func Init() {

	mysqlHost = flag.String("mysql_host", "127.0.0.1", "MySQL host")
	mysqlPort = flag.String("mysql_port", "3306", "MySQL port")
	flag.Parse()

	db = connect("", *mysqlHost, *mysqlPort)
	initDB()

	db = connect("stream", *mysqlHost, *mysqlPort)
	initStructure()
}

func main() {

	Init()

	config := nsq.NewConfig()

	consumer, _ := nsq.NewConsumer(eventsTopic, "ch", config)

	counter := 0
	h := nsq.HandlerFunc(func(msg *nsq.Message) error {

		// Process service messages.
		msgStr := string(msg.Body)
		if IsSvcMsg(msgStr) {

			switch msgStr {

			case sessStartMsg:
				fmt.Println("session STARTED")
			case sessEndMsg:
				// In the end of teach data loading session we recalculate most watched stats
				// to avoid this calculation on query.
				// Implemented here just to synchronize with each data update, however,
				// in a real-life situation, most likely will be happening as a result
				// of a separate scheduled job.
				fmt.Println(" - waiting for batch processing to finish...")
				err := recalculateMostWatched()
				if err != nil {
					// todo: error-handling
					fmt.Println(" - recalculateMostWatched FAILED: " + err.Error())
				}
				fmt.Println(" - recalculateMostWatched COMPLETED")

				fmt.Println("session COMPLETED")
				fmt.Println("waiting for input...")
			}

			return nil
		}

		// Process data messages.
		saveBatch(msg)
		counter++

		if counter != 0 && counter%outputInterval == 0 {
			fmt.Println(strconv.Itoa(counter/outputInterval) + "K batches received")
		}

		return nil
	})

	consumer.AddHandler(h)
	err := consumer.ConnectToNSQD(addr)
	if err != nil {
		panic("Could not connect")
	}

	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-termChan:
			stats := consumer.Stats()
			fmt.Printf("Consumer shut down.\n\nMessages received: %d\nMessages finished: %d\n\n",
				stats.MessagesReceived,
				stats.MessagesFinished)
			consumer.Stop()
		}
	}
}

// Check if message belongs to a group of service messages.
func IsSvcMsg(msg string) bool {
	_, ok := svcMsgs[msg]
	if !ok {
		return false
	}

	return true
}

// recalculateMostWatched identifies a most watched video based on accumulated history of views.
func recalculateMostWatched() error {
	mw := struct {
		MostWatchedVideoID    string `db:"video_id"`
		MostWatchedVideoCount int64  `db:"count"`
	}{}
	err := db.Get(&mw, `SELECT video_id, COUNT(*) AS count FROM views GROUP BY video_id ORDER BY count DESC LIMIT 1`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`UPDATE leaderboard SET most_watched_video_id = ?, most_watched_video_count = ? WHERE id = 1 LIMIT 1`, mw.MostWatchedVideoID, mw.MostWatchedVideoCount)
	if err != nil {
		return err
	}

	return nil
}

// saveBatch unwraps a batch message into separate records, prepares event-type-specific batches and persists them.
func saveBatch(msg *nsq.Message) {
	batch := strings.Split(string(msg.Body), "|")

	// Convert batch items into a MySQL insert statement.
	batchUsers := ""
	batchVideos := ""
	batchViews := ""

	for _, line := range batch {

		r := Record{}
		err := r.add(line)
		if err != nil {
			//e.Add(line, "record_parse", err)
		}

		switch r.RecordType {
		case typeRegister:
			batchUsers += `(` + r.UserID + `,'` + r.Country + `','` + r.IP + `','` + r.Timestamp + `'),`
		case typeUpload:
			batchVideos += `(` + r.VideoID + `,` + r.UserID + `,'` + r.Timestamp + `'),`
		case typeWatch:
			batchViews += `(` + r.VideoID + `,` + r.UserID + `,'` + r.Timestamp + `'),`
		}
	}

	go flushUsersBatch(batchUsers)
	go flushVideosBatch(batchVideos)
	go flushViewsBatch(batchViews)
}

// flushUsersBatch saves batch of REGISTER records.
func flushUsersBatch(b string) {
	b = strings.TrimSuffix(b, ",")
	_, err := db.Exec(`INSERT INTO users (user_id, country, ip, timestamp) VALUES ` + b)
	if err != nil {
		//
	}
}

// flushVideosBatch saves batch of UPLOAD records.
func flushVideosBatch(b string) {
	b = strings.TrimSuffix(b, ",")
	_, err := db.Exec(`INSERT INTO videos (video_id, user_id, timestamp) VALUES ` + b)
	if err != nil {
		//
	}
}

// flushUsersBatch saves batch of WATCH records.
func flushViewsBatch(b string) {
	b = strings.TrimSuffix(b, ",")
	_, err := db.Exec(`INSERT INTO views (video_id, user_id, timestamp) VALUES ` + b)
	if err != nil {
		//
	}
}
