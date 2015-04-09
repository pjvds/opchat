package main

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pjvds/tidy"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	log = tidy.GetLogger()
)

const (
	OpInsert   = "i"
	OpUpdate   = "u"
	OpDelete   = "d"
	OpCommand  = "c"
	OpDatabase = "db"
	OpNoop     = "n"
)

type Oplog struct {
	Timestamp   time.Time `bson:"ts"`
	HistoryID   int64     `bson:"h"`
	Version     int       `bson:"v"`
	Operation   string    `bson:"op"`
	Namespace   string    `bson:"ns"`
	Object      bson.M    `bson:"o"`
	QueryObject bson.M    `bson:"o2"`
}

func tailOplog(session *mgo.Session) chan Oplog {
	logs := make(chan Oplog)

	cursor := session.DB("local").C("oplog.$main").Find(nil).Tail(-1)
	defer cursor.Close()

	go func() {
		defer close(logs)

		entry := Oplog{}
		for cursor.Next(&entry) {
			logs <- entry
		}

		if cursor.Err() != nil {
			log.With("error", cursor.Err()).Fatal("tail curos failed")
		}
	}()

	return logs
}

func main() {
	dbUrl := "localhost"
	session, err := mgo.Dial(dbUrl)

	if err != nil {
		log.Withs(tidy.Fields{
			"url":   dbUrl,
			"error": err}).Fatal("failed to dail server")
	}
	if err := session.Ping(); err != nil {
		log.Withs(tidy.Fields{
			"url":   dbUrl,
			"error": err}).Fatal("failed to ping server")
	}

	db := session.DB("opchat")
	router := gin.Default()
	log := tidy.GetLogger()
	oplog := tailOplog(session)

	go func() {
		for op := range oplog {
			if op.Namespace == "opchat.conversations" {
				selector := bson.M{"_id": op.Object["last_message_id"]}
				update := bson.M{
					"id":         op.Object["last_message_id"],
					"by_user_id": op.Object["by_user_id"],
					"to_user_id": op.Object["to_user_id"],
					"text":       op.Object["last_message_text"],
					"created_at": op.Object["last_message_at"],
				}

				if _, err := db.C("messages").Upsert(selector, update); err != nil {
					log.Withs(tidy.Fields{
						"selector": selector,
						"update":   update,
						"error":    err}).Error("failed to upsert message")
				} else {
					log.Withs(tidy.Fields{
						"selector": selector,
						"update":   update}).Error("message upsert success")
				}
			}
		}

		log.Fatal("oplog tail ended")
	}()

	router.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	router.POST("/conversations/", func(c *gin.Context) {
		body := struct {
			Id       string `json:"id"`
			Message  string `json:"message" binding:"required"`
			ByUserId string `json:"by_user_id" binding:"required"`
			ToUserId string `json:"to_user_id" binding:"required"`
		}{}
		c.Bind(&body)

		if len(body.Id) == 0 {
			body.Id = bson.NewObjectId().String()
		}

		if len(body.Message) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "missing message",
			})
			return
		}
		if len(body.ToUserId) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "missing to_user_id",
			})
			return
		}
		if len(body.ByUserId) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "missing by_user_id",
			})
			return
		}

		id := body.ByUserId + "-" + body.ToUserId
		selector := bson.M{
			"_id": id}
		update := bson.M{
			"by_user_id":        body.ByUserId,
			"to_user_id":        body.ToUserId,
			"last_message_id":   body.Id,
			"last_message_text": body.Message,
			"last_message_at":   time.Now().UTC(),
			"create_at":         time.Now().UTC()}

		if _, err := db.C("conversations").Upsert(selector, update); err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}

		c.String(http.StatusCreated, "created")
	})

	address := ":8080"

	log.With("address", address).Info("started")
	if err := router.Run(":8080"); err != nil {
		log.With("error", err).Fatal("http server failed")
	}
}
