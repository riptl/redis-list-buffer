package main

import (
	"errors"
	"github.com/go-redis/redis"
)

var queue *redis.Client

func connectQueue() error {
	queue = redis.NewClient(&redis.Options{
		Network:  config.RNetworkMode,
		Addr:     config.RHost,
		Password: config.RPass,
		DB:       int(config.RNumberDB),
	})
	if queue == nil {
		return errors.New("could not connect to Redis")
	}

	return nil
}

func queueSizes() (inLen int64, outLen int64, err error) {
	pipe := queue.Pipeline()
	defer pipe.Close()
	inCmd := pipe.LLen(config.RInKey)
	outCmd := pipe.LLen(config.ROutKey)
	pipe.Exec()

	inLen, err = inCmd.Result()
	if err != nil {
		return
	}
	outLen, err = outCmd.Result()
	return
}

func popChunk() ([]string, error) {
	multi := queue.TxPipeline()
	defer multi.Close()

	// Get chunk at start of list
	itemsCmd := multi.LRange(config.RInKey, 0, redisChunk)
	// Drop chunk (trim to everything except chunk)
	multi.LTrim(config.RInKey, redisChunk+1, -1)

	_, err := multi.Exec()
	if err == nil {
		return itemsCmd.Val(), nil
	} else {
		return nil, err
	}
}

func popAll() ([]string, error) {
	multi := queue.TxPipeline()
	defer multi.Close()

	itemsCmd := multi.LRange(config.RInKey, 0, -1)
	multi.LTrim(config.RInKey, 1, 0)

	_, err := multi.Exec()
	if err == nil {
		return itemsCmd.Val(), nil
	} else {
		return nil, err
	}
}

func pushChunk(chunk []string) (err error) {
	// Convert result to []interface{}
	items := make([]interface{}, len(chunk))
	for i, item := range chunk {
		items[i] = item
	}

	// Add to output queue
	err = queue.LPush(config.ROutKey, items...).Err()
	return err
}

func transferChunk() (err error) {
	var chunk []string
	chunk, err = popChunk()
	if err != nil {
		return
	}
	err = pushChunk(chunk)
	return
}

func transferAll() (err error) {
	var chunk []string
	chunk, err = popAll()
	if err != nil {
		return
	}
	err = pushChunk(chunk)
	return
}
