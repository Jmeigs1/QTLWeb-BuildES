package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

import (
	"github.com/go-redis/redis"
	"log"
)

//BystroToRedis takes bystro output and indexes in redis
func BystroToRedis() {

	redisClient := redis.NewClient(&redis.Options{
		Addr:     "54.92.183.250:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	redisClient.Ping()

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

	for {

		cols, error := reader.Read()
		if error == io.EOF {
			log.Println("EOF")
			break
		} else if error != nil {
			panic(error)
		}

		for i, data := range cols {
			fmt.Println(header[i], ": ", data)
		}
		break
	}

}
