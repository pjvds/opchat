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

	router.POST("/thread/:thread_id", func(c *gin.Context) {
		request := struct {
			Message  string `json:"message" binding:"required"`
			ByUserId string `json:"by_user_id" binding:"required"`
			ToUserId string `json:"to_user_id" binding:"required"`
			ThreadId string
		}{
			ThreadId: c.Params.ByName("thread_id"),
		}
		c.Bind(&request)

		if len(request.ThreadId) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "missing thread id",
			})
			return
		}
		if len(request.Message) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "missing message",
			})
			return
		}
		if len(request.ToUserId) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "missing to_user_id",
			})
			return
		}
		if len(request.ByUserId) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "missing by_user_id",
			})
			return
		}

		if err := db.C("threads").Insert(bson.M{
			"thread_id":  request.ThreadId,
			"message":    request.Message,
			"by_user_id": request.ByUserId,
			"to_user_id": request.ToUserId,
			"create_at":  time.Now().UTC(),
		}); err != nil {
			c.String(http.StatusInternalServerError, err.Error())
			return
		}

		c.String(http.StatusCreated, "created")
	})

	address := ":8080"

	log.With("address", address).Info("started")
	router.Run(":8080")
}
