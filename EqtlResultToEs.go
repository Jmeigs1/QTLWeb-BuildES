package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	// "strings"
)

import (
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
)

type searchResult struct {
	chr   string
	pos   string
	gene  string
	rsNum string
}

type resultsSet struct {
	dir  string
	name string
}

var resultsSets = []resultsSet{
	resultsSet{
		dir:  "results/pqtl/",
		name: "pqtl",
	},
	resultsSet{
		dir:  "results/pqtl-overlap/",
		name: "pqtlOverlap",
	},
	resultsSet{
		dir:  "results/eqtl-overlap/",
		name: "eqtlOverlap",
	},
}

//ResultToEs sends eqtl results to ElasticSearch
func ResultToEs(chr int, wantedFields []string, bystroMapRef *map[string][]string) {

	for _, set := range resultsSets {

		bystroMap := *bystroMapRef

		es7, _ := elasticsearch7.NewDefaultClient()

		res, err := es7.Info()
		if err != nil {
			log.Fatalf("Error getting response: %s", err)
		}

		log.Println(res)

		log.Println(elasticsearch7.Version)

		headerMap := map[string]int{}

		headerFh, err := os.Open(fmt.Sprintf("%sHeader.csv", set.dir))
		if err != nil {
			panic(err)
		}

		defer headerFh.Close()

		csvReaderHeader := csv.NewReader(headerFh)

		csvReaderHeader.Comma = ','

		header, err := csvReaderHeader.Read()

		for i, col := range header {
			headerMap[col] = i
			log.Println(col)
		}

		fh, err := os.Open(fmt.Sprintf("%schr%d_fixed.csv", set.dir, chr))
		if err != nil {
			panic(err)
		}

		defer fh.Close()

		csvReader := csv.NewReader(fh)

		csvReader.Comma = ','

		raw := map[string]interface{}{}
		j := 0
		var buffer bytes.Buffer

		for {

			cols, readerr := csvReader.Read()
			if readerr == io.EOF {

			} else if readerr != nil {
				panic(readerr)
			} else {

				bystroData, ok := bystroMap[fmt.Sprintf(
					"%s:%s", cols[headerMap["Chromosome"]],
					cols[headerMap["SNPGenomicPosition"]],
				)]
				if !ok {
					log.Panicf(
						"Value not found in bystroMap: chr [%d] pos [%d]",
						headerMap["Chromosome"],
						headerMap["SNPGenomicPosition"],
					)
				}

				bystroDataMap := map[string]interface{}{}

				for i, o := range bystroData {
					bystroDataMap[wantedFields[i]] = o
				}

				sr := searchResult{
					chr:   cols[headerMap["Chromosome"]],
					pos:   cols[headerMap["SNPGenomicPosition"]],
					gene:  cols[headerMap["GeneSymbol"]],
					rsNum: bystroDataMap["dbSNP.name"].(string),
				}

				meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s" } }%s`, "searchresults", "\n"))

				extraData := map[string]interface{}{}

				for index, colData := range cols {
					extraData[header[index]] = colData
				}

				extraDataString, err := json.Marshal(extraData)
				if err != nil {
					panic(err)
				}

				bystroDataString, err := json.Marshal(bystroDataMap)
				if err != nil {
					panic(err)
				}

				//Index ES
				payload := fmt.Sprintf(`{ `+
					`"Coordinate":   		"%s:%s",`+
					`"Chr":   				"%s",`+
					`"Site":   				"%s",`+
					`"Dataset":         	"%s",`+
					`"RsNum":         		"%s",`+
					`"NonIndexedData": 		%s,`+
					`"BystroData": 			%s`+
					`}%s`,
					sr.chr,
					sr.pos,
					sr.chr,
					sr.pos,
					set.name,
					sr.rsNum,
					string(extraDataString),
					string(bystroDataString),
					"\n",
				)

				payloadCast := []byte(payload)

				buffer.Grow(len(payloadCast) + len(meta))
				buffer.Write(meta)
				buffer.Write(payloadCast)
			}

			if buffer.Len() > 15*1024*1024 || readerr == io.EOF {
				res, err = es7.Bulk(
					bytes.NewReader(buffer.Bytes()),
					es7.Bulk.WithIndex("searchresults"),
				)
				if err != nil {
					panic(err)
				} else if res.IsError() {
					if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
						log.Fatalf("Failure to to parse response body: %s", err)
					} else {
						log.Printf("  Error: [%d] %s: %s",
							res.StatusCode,
							raw["error"].(map[string]interface{})["type"],
							raw["error"].(map[string]interface{})["reason"],
						)
					}
				}
				buffer.Reset()
				j++
				log.Println("Payload sent", j)

				if readerr == io.EOF {
					log.Println("EOF", "chr:", chr, "dir:", set.dir)
					break
				}
			}
		}
	}
}
