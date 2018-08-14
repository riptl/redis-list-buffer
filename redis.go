package main

import (
	"github.com/go-redis/redis"
	"errors"
)

var queue *redis.Client

func connectQueue() error {
	queue = redis.NewClient(&redis.Options{
		Network: config.RNetworkMode,
		Addr: config.RHost,
		Password: config.RPass,
		DB: int(config.RNumberDB),
	})
	if queue == nil { return errors.New("could not connect to Redis") }

	return nil
}

func queueSizes() (inLen int64, outLen int64, err error) {
	pipe := queue.Pipeline()
	defer pipe.Close()
	inCmd := pipe.LLen(config.RInKey)
	outCmd := pipe.LLen(config.ROutKey)
	pipe.Exec()

	inLen, err = inCmd.Result()
	if err != nil { return }
	outLen, err = outCmd.Result()
	return
}

func popChunk() ([]string, error) {
	multi := queue.TxPipeline()
	defer multi.Close()

	// Get chunk at start of list
	itemsCmd := multi.LRange(config.RInKey, 0, redisChunk)
	// Drop chunk (trim to everything except chunk)
	multi.LTrim(config.RInKey, redisChunk + 1, -1)

	_, err := multi.Exec()
	if err == nil {
		return itemsCmd.Val(), nil
	} else {
		return nil, err
	}
}

func pushChunk(chunk []string) (err error) {

}

func transferChunk() error {
	itemsStr, err := popChunk()
	if err != nil { return err }

	// Convert result to []interface{}
	items := make([]interface{}, len(itemsStr))
	for i, item := range itemsStr {
		items[i] = item
	}

	// Add to output queue
	err = queue.LPush(config.ROutKey, items...).Err()
	return err
}
