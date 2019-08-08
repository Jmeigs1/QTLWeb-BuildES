package main

import (
	// "context"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	// "strings"
)
import (
	elasticsearch7 "github.com/elastic/go-elasticsearch/v7"
	// esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	_ "github.com/go-sql-driver/mysql"
)

func mysqlToEs() {

	db, err := sql.Open("mysql", "genome@tcp(genome-mysql.soe.ucsc.edu:3306)/hg19")

	// queryAndPrint(`SELECT
	// count(*)
	// FROM hg19.ensGene AS e
	// JOIN hg19.knownToEnsembl AS kte ON kte.value = e.name
	// JOIN hg19.kgXref AS kxr ON kxr.kgID = kte.name
	// JOIN hg19.knownGene AS kg on kg.name = kte.name
	// JOIN hg19.knownCanonical as kc on kc.transcript = kxr.kgID
	// where e.chrom != "chr1" and e.chrom != "chrX" and e.chrom != "chrY"
	// limit 1`, db)

	// return

	es7, _ := elasticsearch7.NewDefaultClient()

	res, err := es7.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	log.Println(res)

	log.Println(elasticsearch7.Version)

	//Recreate index gene
	if res, err = es7.Indices.Delete([]string{"gene"}); err != nil {
		log.Fatalf("Cannot delete index: %s", err)
	}

	res, err = es7.Indices.Create("gene")
	if err != nil {
		log.Fatalf("Cannot create index: %s", err)
	}
	if res.IsError() {
		log.Fatalf("Cannot create index: %s", res)
	}

	defer db.Close()

	rows, err := db.Query(`SELECT
    e.name2 "ens_id",
    kxr.spDisplayID "uniprot_id",
    kxr.genesymbol "genesymbol",
    kg.name "protein_name",
    e.txStart "start",
	e.txEnd "end",
	e.chrom "chr"
    FROM hg19.ensGene AS e
    JOIN hg19.knownToEnsembl AS kte ON kte.value = e.name
    JOIN hg19.kgXref AS kxr ON kxr.kgID = kte.name
    JOIN hg19.knownGene AS kg on kg.name = kte.name
	JOIN hg19.knownCanonical as kc on kc.transcript = kxr.kgID
	where e.chrom != "chr1" and e.chrom != "chrX" and e.chrom != "chrY"
	order by e.name2 ASC`)

	defer rows.Close()

	if err != nil {
		panic(err)
	}

	var (
		GeneSymbol  string
		UniprotID   string
		EnsID       string
		ProteinName string
		Start       int
		End         int
		Chr         string
	)

	var raw map[string]interface{}
	batchsize := 100
	i := 0
	var buffer bytes.Buffer

	for rows.Next() {

		//Parse MySql results
		err = rows.Scan(
			&EnsID,
			&UniprotID,
			&GeneSymbol,
			&ProteinName,
			&Start,
			&End,
			&Chr,
		)

		if err != nil {
			panic(err)
		}

		meta := []byte(fmt.Sprintf(`{ "index" : { "_index" : "%s" } }%s`, "gene", "\n"))

		//Index ES
		payload := fmt.Sprintf(`{ `+
			`"GeneSymbol":   "%s",`+
			`"UniprotID":    "%s",`+
			`"EnsID":        "%s",`+
			`"ProteinName":  "%s",`+
			`"Start":        "%d",`+
			`"End":          "%d",`+
			`"Chr":          "%s" `+
			`}%s`,
			GeneSymbol,
			UniprotID,
			EnsID,
			ProteinName,
			Start,
			End,
			Chr,
			"\n",
		)

		payloadCast := []byte(payload)

		buffer.Grow(len(payloadCast) + len(meta))
		buffer.Write(meta)
		buffer.Write(payloadCast)

		if i == batchsize {
			res, err = es7.Bulk(
				bytes.NewReader(buffer.Bytes()),
				es7.Bulk.WithIndex("gene"),
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
			i = 0
		} else {
			i++
		}

	}
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
		fmt.Println(index)
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
