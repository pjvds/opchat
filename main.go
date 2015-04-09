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

type OplogRecord struct {
}

func main() {
	dbUrl := "localhost"
	session, err := mgo.Dial(dbUrl)

	if err != nil {
		log.With("url", dbUrl).Fatal(err.Error())
	}

	db := session.DB("opchat")
	router := gin.Default()
	log := tidy.GetLogger()

	router.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK, "pong")
	})

	router.POST("/conversations/", func(c *gin.Context) {
		body := struct {
			Message  string `json:"message" binding:"required"`
			ByUserId string `json:"by_user_id" binding:"required"`
			ToUserId string `json:"to_user_id" binding:"required"`
		}{}
		c.Bind(&body)

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
			"by_user_id":      body.ByUserId,
			"to_user_id":      body.ToUserId,
			"last_message":    body.Message,
			"last_message_at": time.Now().UTC(),
			"create_at":       time.Now().UTC()}

		if _, err := db.C("conversations").Upsert(selector, update); err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}

		c.String(http.StatusCreated, "created")
	})

	address := ":8080"

	log.With("address", address).Info("started")
	router.Run(":8080")
}
