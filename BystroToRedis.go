package main

import (
	"compress/gzip"
	"encoding/csv"
	// "fmt"
	"io"
	"os"
	"time"
)

import (
	"github.com/go-redis/redis"
	"log"
)

//BystroToRedis takes bystro output and indexes in redis
func BystroToRedis() {

	redisClient := redis.NewClient(&redis.Options{
		Addr:        "54.175.56.31:6379",
		Password:    "", // no password set
		DB:          0,  // use default DB
		ReadTimeout: time.Minute * 10,
	})

	ping, err := redisClient.Ping().Result()

	if err != nil {
		panic(err)
	}

	log.Println(ping)

	redisClient.FlushDB()

	fh, err := os.Open("bystro/AmpAD.chr1.tsv.gz")
	if err != nil {
		panic(err)
	}

	fi, _ := fh.Stat()

	log.Println(fi.Name())
	defer fh.Close()

	gz, err := gzip.NewReader(fh)
	if err != nil {
		panic(err)
	}

	reader := csv.NewReader(gz)

	reader.Comma = '\t'

	header, err := reader.Read()
	if err != nil {
		panic(err)
	}

	i := 0
	j := 0
	pipe := redisClient.Pipeline()

	for {
		line := map[string]interface{}{}

		cols, error := reader.Read()
		if error == io.EOF {
			log.Println("EOF")
			break
		} else if error != nil {
			panic(error)
		}

		for index, data := range cols {
			line[header[index]] = data
		}

		// fmt.Println("chrom", "\t: ", line["chrom"])
		// fmt.Println("pos", "\t: ", line["pos"])
		// fmt.Println("")

		pipe.HMSet(line["chrom"].(string)+":"+line["pos"].(string), line)

		if err != nil {
			panic(err)
		}

		if i > 9999 {
			cmdList, err := pipe.Exec()
			if err != nil {
				panic(err)
			}
			i = 0
			j++
			log.Println("Pipeline sent: ", j)
			log.Println("Pipeline size: ", len(cmdList))
		} else {
			// log.Println(i)
			i++
		}
	}

}
