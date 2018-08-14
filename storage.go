package main

import (
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"path"
)

var writeFile *os.File
var writeFileBuf *bufio.Writer
var writeId int64 = -1
var writeCount int64 = 0

func connectStorage() (err error) {
	err = os.MkdirAll(config.SDir, 0755)
	if err != nil {
		return
	}
	return connectIndex()
}

func disconnectStorage() {
	if writeId >= 0 {
		setSizeIndex(writeId, writeCount)
	}
	disconnectIndex()
	if writeFileBuf != nil {
		writeFileBuf.Flush()
	}
	if writeFile != nil {
		writeFile.Close()
	}
}

func fileIdToPath(id int64) string {
	idStr := fmt.Sprintf("%016x.txt", id)
	return path.Join(config.SDir, idStr)
}

func nextWriteFile() (err error) {
	if writeFile != nil {
		writeFileBuf.Flush()
		writeFile.Close()
	}

	writeId, writeCount, err = maxIndex()
	if err != nil {
		return
	}
	if writeId < 0 {
		newIndex()
		writeId, writeCount, err = maxIndex()
	}

	filePath := fileIdToPath(writeId)
	flags := os.O_WRONLY | os.O_APPEND | os.O_CREATE
	writeFile, err = os.OpenFile(filePath, flags, 0644)
	if err != nil {
		return
	}
	writeFileBuf = bufio.NewWriter(writeFile)

	return
}

func writeChunk(remChunk []string) (err error) {
	if writeId < 0 {
		err = nextWriteFile()
		if err != nil {
			return
		}
	}

	for len(remChunk) > 0 {
		available := dataChunk - writeCount

		// Switch to new file
		if available <= 0 {
			err = setSizeIndex(writeId, writeCount)
			if err != nil {
				return
			}
			err = newIndex()
			if err != nil {
				return
			}
			err = nextWriteFile()
			if err != nil {
				return
			}
			available = dataChunk
		}

		// Get current chunk at set remaining chunk
		var chunk []string
		if int64(len(remChunk)) <= available {
			chunk = remChunk
			remChunk = nil
		} else {
			chunk = remChunk[:available]
			remChunk = remChunk[available:]
		}

		// First write
		if writeFile == nil {
			err = nextWriteFile()
			if err != nil {
				return
			}
		}

		// Do actual write
		for _, id := range chunk {
			_, err = writeFileBuf.WriteString(id)
			if err != nil {
				return
			}
			err = writeFileBuf.WriteByte('\n')
			if err != nil {
				return
			}
		}
		writeCount += int64(len(chunk))
	}
	return
}

func readChunk() (chunk []string, err error) {
	var id int64
	id, _, err = minIndex()
	if id < 0 {
		empty = true
		return
	}
	if err != nil {
		return
	}

	// Check if ID collides with write
	if id == writeId {
		// Close writer
		err = writeFileBuf.Flush()
		if err != nil {
			return
		}
		err = writeFile.Close()
		if err != nil {
			return
		}
		writeId = -1
		writeCount = 0
	}

	filePath := fileIdToPath(id)

	var file *os.File
	var scanner *bufio.Scanner
	file, err = os.Open(filePath)
	if os.IsNotExist(err) {
		logrus.Warnf("File of %016x disappeared.", id)
		err = deleteIndex(id)
		return
	}
	if err != nil {
		return
	}
	defer file.Close()

	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		chunk = append(chunk, line)
	}
	err = scanner.Err()
	if err != nil {
		return
	}

	err = deleteChunk(id)
	if err != nil {
		return
	}

	return
}

func deleteChunk(id int64) (err error) {
	filePath := fileIdToPath(id)
	err = os.Remove(filePath)
	if err != nil {
		return
	}
	err = deleteIndex(id)
	return
}
