package main

import "fmt"
import "log"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import elasticsearch7 "github.com/elastic/go-elasticsearch/v7"

func main() {

	es7, _ := elasticsearch7.NewDefaultClient()

	res, err := es7.Info()
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}

	log.Println(res)

	db, err := sql.Open("mysql", "genome@tcp(genome-mysql.soe.ucsc.edu:3306)/hg19")

	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	rows, err := db.Query(`SELECT
    e.name2 "ens_id",
    e.txStart "start",
    e.txEnd "end",
	kxr.spDisplayID "uniprot_id",
	kxr.genesymbol "genesymbol",
    kg.name "protein_name"
    FROM hg19.ensGene AS e
    JOIN hg19.knownToEnsembl AS kte ON kte.value = e.name
    JOIN hg19.kgXref AS kxr ON kxr.kgID = kte.name
	JOIN hg19.knownGene AS kg on kg.name = kte.name
	JOIN hg19.knownCanonical as kc on kc.transcript = kxr.kgID
	where e.chrom = "chr1"
	Limit 1`)

	defer rows.Close()

	cols, err := rows.Columns()

	if err != nil {

	}

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
