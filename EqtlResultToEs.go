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

//EqtlResultToEs sends eqtl results to ElasticSearch
func EqtlResultToEs(chr int, wantedFields []string, bystroMapRef *map[string][]string) {

	bystroMap := *bystroMapRef

	es7, _ := elasticsearch7.NewDefaultClient()

	res, err := es7.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	log.Println(res)

	log.Println(elasticsearch7.Version)

	// //Recreate index gene
	// if res, err = es7.Indices.Delete([]string{"searchresults"}); err != nil {
	// 	log.Fatalf("Cannot delete index: %s", err)
	// }

	// res, err = es7.Indices.Create("searchresults")
	// if err != nil {
	// 	log.Fatalf("Cannot create index: %s", err)
	// }
	// if res.IsError() {
	// 	log.Fatalf("Cannot create index: %s", res)
	// }

	headerMap := map[string]int{}

	headerFh, err := os.Open("results/eqtl/eQTLresults_for_brainqtl_Header.csv")
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

	fh, err := os.Open(fmt.Sprintf("results/eqtl/eQTLresults_for_brainqtl_chr%d.csv", chr))
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

			bystroData := bystroMap[fmt.Sprintf("%s:%s", cols[headerMap["Chromosome"]], cols[headerMap["SNPGenomicPosition"]])]

			bystroDataMap := map[string]interface{}{}

			for i, o := range bystroData {
				bystroDataMap[wantedFields[i]] = o
			}

			sr := searchResult{
				chr:   cols[headerMap["Chromosome"]],
				pos:   cols[headerMap["SNPGenomicPosition"]],
				gene:  cols[headerMap["EnsemblGeneID"]],
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
				`"Track":         		"%s",`+
				`"RsNum":         		"%s",`+
				`"NonIndexedData": 		%s,`+
				`"BystroData": 			%s`+
				`}%s`,
				sr.chr,
				sr.pos,
				sr.chr,
				sr.pos,
				"ensGene",
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
				log.Println("EOF", "chr:", chr)
				break
			}
		}
	}
}
