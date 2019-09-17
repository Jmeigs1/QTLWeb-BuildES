package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"strings"
)
import (
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	//Driver for database/sql
	_ "github.com/go-sql-driver/mysql"
)

var indexMapping = `{
	"settings":{
		"index":{
			"number_of_shards": "1",
            "number_of_replicas": "0"
		},
		"analysis": {
			"analyzer": {
				"lowercasespaceanalyzer": {
					"type": "custom",
					"tokenizer": "whitespace",
					"filter": "lowercase"
				}
			}
		}
	},
	"mappings": {
		"properties": {
			"NonIndexedData": {
				"type": "object",
				"enabled": false
			},
			"BystroData": {
				"type": "object",
				"enabled": false
			},
			"Coordinate": {
				"type": "text",
				"analyzer" : "lowercasespaceanalyzer"
			},
			"GeneSymbol": {
				"type": "text",
				"analyzer" : "lowercasespaceanalyzer"
			},
			"ProteinName": {
				"type": "text",
				"analyzer" : "lowercasespaceanalyzer"
			},
			"UniprotID": {
				"type": "text",
				"analyzer" : "lowercasespaceanalyzer"
			},
			"Site": {
				"type": "integer"
			}
		}
	}
}`

type geneRelations struct {
	EnsIDs       map[string]bool
	UniprotIDs   map[string]bool
	ProteinNames map[string]bool
	Chr          string
}

func mysqlToEs() {

	es7, _ := elasticsearch7.NewDefaultClient()

	res, err := es7.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	log.Println(res)

	log.Println(elasticsearch7.Version)

	//Recreate index searchresults
	if res, err = es7.Indices.Delete([]string{"searchresults"}); err != nil {
		log.Fatalf("Cannot delete index: %s", err)
	}

	icr := esapi.IndicesCreateRequest{
		Index: "searchresults",
		Body:  strings.NewReader(indexMapping),
	}

	res, err = icr.Do(context.Background(), es7)

	if err != nil {
		log.Fatalf("Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Cannot create index: %s", res)
	}

	db, err := sql.Open("mysql", "genome@tcp(genome-mysql.soe.ucsc.edu:3306)/hg19")

	defer db.Close()

	query := `SELECT
e.name2 "ens_id",
kxr.spID "uniprot_id",
kxr.genesymbol "genesymbol",
kg.name "protein_name",
kg.chrom "chr"
FROM hg19.knownGene AS kg
JOIN hg19.knownToEnsembl AS kte ON kte.name = kg.name
JOIN hg19.kgXref AS kxr ON kxr.kgID = kg.name
LEFT JOIN hg19.ensGene AS e on e.name = kte.value
where kg.chrom != "chrX" and kg.chrom != "chrY"
and kg.chrom not like "%#_%" ESCAPE "#"
`

	rows, err := db.Query(query)

	defer rows.Close()

	if err != nil {
		panic(err)
	}

	// geneSymbol ->
	geneSymbolRelationMap := map[string]geneRelations{}

	for rows.Next() {

		var (
			EnsID       sql.NullString
			UniprotID   sql.NullString
			GeneSymbol  sql.NullString
			ProteinName sql.NullString
			Chr         string
		)

		//Parse MySql results
		err = rows.Scan(
			&EnsID,
			&UniprotID,
			&GeneSymbol,
			&ProteinName,
			&Chr,
		)
		if err != nil {
			panic(err)
		}

		if GeneSymbol.Valid && !strings.Contains(GeneSymbol.String, " ") {
			if _, ok := geneSymbolRelationMap[GeneSymbol.String]; !ok {
				geneSymbolRelationMap[GeneSymbol.String] = geneRelations{
					EnsIDs:       map[string]bool{},
					UniprotIDs:   map[string]bool{},
					ProteinNames: map[string]bool{},
					Chr:          Chr,
				}
			}
			if EnsID.Valid {
				if _, ok := geneSymbolRelationMap[GeneSymbol.String].EnsIDs[EnsID.String]; !ok {
					geneSymbolRelationMap[GeneSymbol.String].EnsIDs[EnsID.String] = true
				}
			}
			if UniprotID.Valid {
				if _, ok := geneSymbolRelationMap[GeneSymbol.String].UniprotIDs[UniprotID.String]; !ok {
					geneSymbolRelationMap[GeneSymbol.String].UniprotIDs[UniprotID.String] = true
				}
			}
			if ProteinName.Valid {
				if _, ok := geneSymbolRelationMap[GeneSymbol.String].ProteinNames[ProteinName.String]; !ok {
					geneSymbolRelationMap[GeneSymbol.String].ProteinNames[ProteinName.String] = true
				}
			}
		}
	}

	batchsize := 100
	i := 0
	var buffer bytes.Buffer

	for geneSymbol, geneSymbolRelation := range geneSymbolRelationMap {

		addToESBuffer(&buffer, geneSymbol, "GeneSymbol", geneSymbol, geneSymbolRelation.Chr, &i, batchsize, es7)

		for data := range geneSymbolRelation.EnsIDs {
			addToESBuffer(&buffer, data, "EnsID", geneSymbol, geneSymbolRelation.Chr, &i, batchsize, es7)
		}
		for data := range geneSymbolRelation.UniprotIDs {
			addToESBuffer(&buffer, data, "UniprotID", geneSymbol, geneSymbolRelation.Chr, &i, batchsize, es7)
		}
		for data := range geneSymbolRelation.ProteinNames {
			addToESBuffer(&buffer, data, "ProteinName", geneSymbol, geneSymbolRelation.Chr, &i, batchsize, es7)
		}
	}

	sendESPayload(es7, &buffer)

}

func addToESBuffer(
	buffer *bytes.Buffer,
	data string,
	dataLabel string,
	geneSymbol string,
	chr string,
	i *int,
	batchsize int,
	es7 *elasticsearch7.Client,
) {
	meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s" } }%s`, "searchresults", "\n"))

	payloadMap := map[string]interface{}{
		dataLabel: data,
		"Chr":     chr,
		"NonIndexedData": map[string]interface{}{
			"GeneSymbol": geneSymbol,
		},
	}

	payload, err := json.Marshal(payloadMap)
	if err != nil {
		panic(err)
	}

	payloadCast := []byte(string(payload) + "\n")

	buffer.Grow(len(payloadCast) + len(meta))
	buffer.Write(meta)
	buffer.Write(payloadCast)

	if *i == batchsize {
		sendESPayload(es7, buffer)
		*i = 0
	} else {
		*i++
	}
}

func sendESPayload(es7 *elasticsearch7.Client, buffer *bytes.Buffer) {
	var raw map[string]interface{}

	res, err := es7.Bulk(
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

}

func queryAndPrint(query string, db *sql.DB) {

	rows, err := db.Query(query)

	if err != nil {
		panic(err)
	}

	defer rows.Close()

	cols, err := rows.Columns() // Remember to check err afterwards
	vals := make([]interface{}, len(cols))
	for i := range cols {
		vals[i] = new([]byte)
	}
	index := 0

	for rows.Next() {
		fmt.Println(index + 1)
		err = rows.Scan(vals...)

		for i, obj := range vals {
			if obj != nil {
				test := obj.(*[]byte)
				fmt.Printf("%s: %s\n", cols[i], string(*test))
			}
		}
		fmt.Println("")
		index++
	}

}
