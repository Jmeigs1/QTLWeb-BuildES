package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	// "strings"

	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
)

var columnNames = map[string]string{
	"Chromosome":         "Chr",
	"SNPGenomicPosition": "Rel_SNP_Name",
	"GeneSymbol":         "GeneSymbol",
}

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
	{
		dir:  "results/rosmap/",
		name: "rosmap",
	},
	{
		dir:  "results/rosmap_control/",
		name: "rosmap_control",
	},
	{
		dir:  "results/banner/",
		name: "banner",
	},
}

//ResultToEs sends eqtl results to ElasticSearch
func ResultToEs(chr int, wantedFields []string, bystroMapRef *map[string][]string) {

	f, err := os.OpenFile("missing.log",
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println(err)
	}
	defer f.Close()

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

		fh, err := os.Open(fmt.Sprintf("%schr%d.csv", set.dir, chr))
		if err != nil {
			panic(err)
		}

		defer fh.Close()

		csvReader := csv.NewReader(fh)

		csvReader.Comma = ','

		raw := map[string]interface{}{}
		j := 0
		counter := 0
		var buffer bytes.Buffer

		for {
			counter++
			cols, readerr := csvReader.Read()
			if readerr == io.EOF {

			} else if readerr != nil {
				panic(readerr)
			} else {

				bystroData, ok := bystroMap[fmt.Sprintf(
					"%s",
					cols[headerMap[columnNames["SNPGenomicPosition"]]],
				)]
				if !ok {
					f.WriteString(
						fmt.Sprintf(
							"Value not found in bystroMap: set [%s] chr [%s] pos [%s] line [%d]\n",
							set.name,
							cols[headerMap[columnNames["Chromosome"]]],
							cols[headerMap[columnNames["SNPGenomicPosition"]]],
							counter,
						),
					)

					continue
					// log.Panicf(
					// 	"Value not found in bystroMap: chr [%s] pos [%s] line [%d]",
					// 	cols[headerMap[columnNames["Chromosome"]]],
					// 	cols[headerMap[columnNames["SNPGenomicPosition"]]],
					// 	counter,
					// )
				}

				bystroDataMap := map[string]interface{}{}

				for i, o := range bystroData {
					bystroDataMap[wantedFields[i]] = o
				}

				chromosome := cols[headerMap[columnNames["Chromosome"]]]

				if !strings.HasPrefix(chromosome, "chr") {
					chromosome = "chr" + chromosome
				}

				sr := searchResult{
					chr:   chromosome,
					pos:   cols[headerMap[columnNames["SNPGenomicPosition"]]],
					gene:  cols[headerMap[columnNames["GeneSymbol"]]],
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
