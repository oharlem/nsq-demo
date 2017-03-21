package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/nsqio/go-nsq"
)

const (
	// Event types.
	typeLike     = "LIKE"
	typeRegister = "REGISTER"
	typeUpload   = "UPLOAD"
	typeWatch    = "WATCH"

	// Script progress output.
	outputInterval = 10000

	// Record set that will be sent to NSQ as one message.
	// Should be fine-tuned according to ens. specs.
	// Currently, to avoid additional processing on consumer side,
	// it is set to match optimal number for MySQL,
	batchSize = 100

	// Messages to signal beginning and end of a data load session.
	// They give additional control on the consuming end of the message queue.
	sessStartMsg = "SESS_START"
	sessEndMsg   = "SESS_END"

	// NSQ config.
	addr        = "127.0.0.1:4150"
	eventsTopic = "events"
)

var (
	// Allowed event types.
	types = map[string]struct{}{
		typeLike:     {},
		typeRegister: {},
		typeUpload:   {},
		typeWatch:    {},
	}

	// ErrParsingRecord means that the record is of unrecognized format.
	ErrParsingRecord = errors.New("Error parsing record")

	fileLoc *string
)

// Init initializes script arguments.
func Init() {
	fileLoc = flag.String("file", "data/data.dump", "Data dump file location.")
	flag.Parse()
}

func main() {

	Init()

	fmt.Println(" ")
	fmt.Println("START")
	fmt.Println(" ")

	// Connect to NSQ.
	conf := nsq.NewConfig()
	p, err := nsq.NewProducer(addr, conf)
	if err != nil {
		panic("Could not connect to NSQ: " + err.Error())
	}
	defer p.Stop()

	// Read the data dump
	f, err := os.Open(*fileLoc)
	if err != nil {
		panic("Could not read the data source: " + err.Error())
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)

	// Ops stats
	likes := 0
	registrations := 0
	uploads := 0
	views := 0
	total := 0
	errQty := 0

	// Init batch.
	batch := ""
	batchQty := 0

	// Signal session start.
	p.Publish(eventsTopic, []byte(sessStartMsg))

	for scanner.Scan() {
		line := scanner.Text()

		rType, err := recordValidator(line)
		if err != nil {
			errQty++
			fmt.Printf("%v", err.Error())
		}

		switch rType {
		case typeLike:
			likes++
			total++
			continue
		case typeWatch:
			views++
		case typeRegister:
			registrations++
		case typeUpload:
			uploads++
		}

		batch += line + "|"
		if batch != "" && total%batchSize == 0 {
			p.Publish(eventsTopic, []byte(batch))
			batch = ""
			batchQty++
		}

		if total != 0 && total%outputInterval == 0 {
			fmt.Println(strconv.Itoa(total/1000) + "K records processed")
		}

		total++
	}

	// Signal session end.
	p.Publish(eventsTopic, []byte(sessEndMsg))

	fmt.Println(strconv.Itoa(total/1000) + "K records processed in total")

	fmt.Print("\nSTATS:\n")
	fmt.Printf("Likes: %d (skipped)\n", likes)
	fmt.Printf("Registrations: %d\n", registrations)
	fmt.Printf("Uploads: %d\n", uploads)
	fmt.Printf("Views: %d\n", views)
	fmt.Printf("Errors: %d\n", errQty)
	fmt.Printf("Batches sent: %d\n", batchQty)

	fmt.Println(" ")
	fmt.Println("COMPLETE")
	fmt.Println(" ")
}

// recordValidator validates properties of an event record and returns its type on success or an error.
func recordValidator(line string) (string, error) {

	vals := strings.Split(line, " ")

	// Lowest # of fields is 4, 5 is used by REGISTER events.
	if len(vals) < 4 {
		return "", ErrParsingRecord
	}

	recType := vals[1]
	_, ok := types[recType]
	if !ok {
		return "", ErrParsingRecord
	}

	return recType, nil
}
