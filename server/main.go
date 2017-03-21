package main

import (
	"errors"
	"flag"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"gopkg.in/tylerb/graceful.v1"
)

var (
	db *sqlx.DB

	mysqlHost  *string
	mysqlPort  *string
	serverPort *string
)

// Init initializes script arguments and connects to the database.
func Init() {

	mysqlHost = flag.String("mysql_host", "127.0.0.1", "MySQL host")
	mysqlPort = flag.String("mysql_port", "3306", "MySQL port")
	serverPort = flag.String("server_port", "9999", "API Server port")
	flag.Parse()

	db = connect("stream", *mysqlHost, *mysqlPort)
}

func main() {

	Init()

	r := gin.Default()
	r.LoadHTMLGlob("server/templates/*")

	r.GET("/", indexHandler)
	r.GET("/stats", statsHandler)
	r.GET("/users", usersHandler)
	r.GET("/viewers", viewersHandler)

	graceful.Run(":"+*serverPort, 10*time.Second, r)
}

func indexHandler(c *gin.Context) {
	c.HTML(http.StatusOK, "index.tmpl", gin.H{})
}

func statsHandler(c *gin.Context) {

	mw := struct {
		VideoID    int   `db:"most_watched_video_id" json:"most_watched_video_id"`
		ViewsCount int64 `db:"most_watched_video_count" json:"most_watched_video_count"`
	}{}
	err := db.Get(&mw, `SELECT most_watched_video_id, most_watched_video_count FROM leaderboard WHERE id=1 LIMIT 1`)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "ERROR",
			"data":   nil,
			"err":    err,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "OK",
		"data":   mw,
		"err":    nil,
	})
}

func usersHandler(c *gin.Context) {

	if c.Query("country") == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "ERROR",
			"data":   nil,
			"err":    http.StatusText(http.StatusBadRequest),
		})
		return
	}

	videoIDs := []struct {
		UserID   int    `db:"user_id"`
		VideoIDs string `db:"video_ids"`
	}{}

	err := db.Select(&videoIDs, `
		SELECT
			u.user_id, GROUP_CONCAT(v.video_id) AS video_ids
		FROM
			videos v INNER JOIN users u
			ON v.user_id = u.user_id
		WHERE u.country = ?
		GROUP BY u.user_id;`,
		c.Query("country"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "ERROR",
			"data":   nil,
			"err":    err,
		})
		return
	}

	// Unwrap video ids
	resp := []struct {
		UserID   int   `json:"user_id"`
		VideoIDs []int `json:"video_ids"`
	}{}
	for _, v := range videoIDs {

		ids, err := convertStrIDs(v.VideoIDs)
		if err != nil {
			// todo: error-handling
			continue
		}

		resp = append(resp, struct {
			UserID   int   `json:"user_id"`
			VideoIDs []int `json:"video_ids"`
		}{
			UserID:   v.UserID,
			VideoIDs: ids,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "OK",
		"data":   resp,
		"err":    nil,
	})
}

// convertStrIDs converts an string of video ids into a slice of ints to preserve data types.
func convertStrIDs(ids string) ([]int, error) {
	ints := []int{}
	for _, s := range strings.Split(ids, ",") {
		i, err := strconv.Atoi(s)
		if err != nil {
			return ints, errors.New("Wrong argument")
		}
		ints = append(ints, i)
	}

	return ints, nil
}

func viewersHandler(c *gin.Context) {

	if c.Query("video_id") == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": "ERROR",
			"data":   nil,
			"err":    http.StatusText(http.StatusBadRequest),
		})
		return
	}

	views := []struct {
		Country    string `db:"country" json:"country"`
		ViewsCount int64  `db:"viewsQty" json:"viewsQty"`
	}{}
	err := db.Select(&views, `
		SELECT
			u.country, count(v.user_id) AS viewsQty
		FROM
			views v INNER JOIN users u
			ON v.user_id = u.user_id
		WHERE v.video_id = ?
		GROUP BY country;`,
		c.Query("video_id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"status": "ERROR",
			"data":   nil,
			"err":    err,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status": "OK",
		"data":   views,
		"err":    nil,
	})
}
