package main

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/terorie/viperstruct"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var config struct {
	RNetworkMode string `viper:"redis.network,optional"`
	RHost        string `viper:"redis.host,optional"`
	RPass        string `viper:"redis.pass,optional"`
	RNumberDB    uint8  `viper:"redis.db,optional"`
	RInKey       string `viper:"redis.in,optional"`
	ROutKey      string `viper:"redis.out,optional"`
	SIndex       string `viper:"index_file,optional"`
	SDir         string `viper:"data_dir,optional"`
	Tick         uint   `viper:"tick"`
	TargetIn     uint   `viper:"target_in"`
	TargetOut    uint   `viper:"target_out"`
	RedisChunk   uint   `viper:"redis_chunk"`
	DataChunk    uint   `viper:"data_chunk"`
}

var exitRequested = uint32(0)
var tickDuration time.Duration
var targetIn, targetOut int64
var redisChunk, dataChunk int64
var empty bool

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "Usage: redis-list-buffer <config.yaml>")
		os.Exit(1)
	} else {
		viper.SetConfigFile(os.Args[1])
	}

	err := exec()
	if err != nil {
		logrus.WithError(err).Fatal("Fatal error")
		os.Exit(1)
	}
}

func exec() (err error) {
	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	viper.SetDefault("redis.network", "tcp")
	viper.SetDefault("redis.host", "localhost:6379")
	viper.SetDefault("redis.pass", "")
	viper.SetDefault("redis.db", 0)
	viper.SetDefault("redis.in", "QUEUE_IN")
	viper.SetDefault("redis.out", "QUEUE_OUT")
	viper.SetDefault("index_file", "./index.db")
	viper.SetDefault("data_dir", "./data/")

	err = viperstruct.ReadConfig(&config)
	if err != nil {
		return
	}

	if config.RNumberDB >= 16 {
		return fmt.Errorf("invalid db number: %d\n", config.RNumberDB)
	}

	tickDuration = time.Duration(config.Tick) * time.Millisecond
	targetIn = int64(config.TargetIn)
	targetOut = int64(config.TargetOut)
	redisChunk = int64(config.RedisChunk)
	dataChunk = int64(config.DataChunk)

	if err := connectQueue(); err != nil {
		return err
	}

	if err := connectStorage(); err != nil {
		return err
	}
	defer disconnectStorage()

	go watchExit()

	for {
		if atomic.LoadUint32(&exitRequested) != 0 {
			break
		}

		inLen, outLen, err := queueSizes()
		if err != nil {
			logrus.WithError(err).Error("Failed getting queue sizes.")
			time.Sleep(tickDuration)
			continue
		}

		inDelta := inLen - targetIn
		outDelta := targetOut - outLen

		// Number of chunks that need to be
		// popped from queueIn
		inChunks := inDelta / redisChunk

		// Number of chunks that need to be
		// pushed into queueOut
		outChunks := outDelta / redisChunk

		switch {
		// Direct copy possible
		case inChunks > 0 && outChunks > 0:
			logrus.Infof("Directly transferring %d items …", redisChunk)
			err = transferChunk()
			if err != nil {
				logrus.WithError(err).
					Error("Error transferring chunk.")
			}

		// Needs write
		case inChunks > 0:
			logrus.Infof("Storing %d items …", inChunks*redisChunk)
			err = storeChunks(inChunks)
			if err != nil {
				logrus.WithError(err).
					Error("Error storing chunk.")
			}

		// Needs read
		case outChunks > 0:
			if empty {
				if inLen > 0 {
					logrus.Infof("Directly transferring all (%d) items …", inLen)
					err = transferAll()
					if err != nil {
						logrus.WithError(err).
							Error("Error transferring chunk.")
					}
				} else {
					logrus.Info("Empty queue …")
				}
				time.Sleep(tickDuration)
				empty = false
				continue
			}
			logrus.Infof("Loading file of items …")
			err = loadChunk()
			if err != nil {
				logrus.WithError(err).
					Error("Error loading chunk.")
			}

		// Idle
		default:
			logrus.Info("Idle …")
			time.Sleep(tickDuration)
		}
	}

	return nil
}

func storeChunks(redisChunks int64) error {
	for ; redisChunks > 0; redisChunks-- {
		items, err := popChunk()
		if err != nil {
			return err
		}
		err = writeChunk(items)
		if err != nil {
			return err
		}
	}
	return nil
}

func loadChunk() (err error) {
	var remChunk []string
	remChunk, err = readChunk()
	if err != nil {
		return
	}

	// Read chunk and load to Redis
	for len(remChunk) != 0 {
		// Get small chunk from big chunk
		var chunk []string
		if int64(len(remChunk)) <= redisChunk {
			chunk = remChunk
			remChunk = nil
		} else {
			chunk = remChunk[:redisChunk]
			remChunk = remChunk[redisChunk:]
		}

		// Write to Redis
		err = pushChunk(chunk)
		if err != nil {
			return
		}
	}

	return
}

func watchExit() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	atomic.StoreUint32(&exitRequested, 1)
}
