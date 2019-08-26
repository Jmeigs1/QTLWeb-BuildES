package main

import (
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

import (
	//Driver for database/sql
	_ "github.com/go-sql-driver/mysql"
)

type resultsSet struct {
	dirs        []string
	startingKey string
}

var resultsSets = []resultsSet{
	resultsSet{
		dirs: []string{
			"../results/pqtl/",
			"../results/pqtl-overlap/",
		},
		startingKey: "UniprotID",
	},
}

var ensResultsSets = []resultsSet{
	resultsSet{
		dirs: []string{
			"../results/eqtl-overlap/",
		},
		startingKey: "EnsemblGeneID",
	},
}

func main() {

	for _, set := range resultsSets {
		relationMap := getRelationMap(set.startingKey)

		for _, dir := range set.dirs {

			for chr := 1; chr < 23; chr++ {

				output := []byte{}

				fh, err := os.Open(fmt.Sprintf("%schr%d.csv", dir, chr))
				if err != nil {
					panic(err)
				}

				defer fh.Close()

				reader := csv.NewReader(fh)

				reader.Comma = ','

				for {
					cols, error := reader.Read()
					if error == io.EOF {
						log.Printf("EOF: chr%d %s", chr, dir)
						break
					} else if error != nil {
						panic(error)
					}

					if val, ok := relationMap[cols[0]]; ok {
						output = append(output, []byte(val+",")...)
						output = append(output, []byte(strings.Join(cols, ",")+"\n")...)

					} else {
						log.Panicf("Value not found in relation map - val [%s] - chr[%d] - dir[%s]",
							cols[0],
							chr,
							dir,
						)
					}
				}

				err = ioutil.WriteFile(fmt.Sprintf("%schr%d_fixed.csv", dir, chr), output, 0644)
				if err != nil {
					log.Fatalln(err)
				}
			}
		}

	}

	for _, set := range ensResultsSets {

		for _, dir := range set.dirs {

			for chr := 1; chr < 23; chr++ {
				output := []byte{}
				ids := []string{}
				idsMap := map[string]bool{}

				fh, err := os.Open(fmt.Sprintf("%schr%d.csv", dir, chr))
				if err != nil {
					panic(err)
				}

				defer fh.Close()

				reader := csv.NewReader(fh)

				reader.Comma = ','

				rows, err := reader.ReadAll()
				if err != nil {
					panic(err)
				}

				for _, row := range rows {
					if _, ok := idsMap[row[0]]; !ok {
						ids = append(ids, row[0])
						idsMap[row[0]] = true
					}
				}

				reqBody, err := json.Marshal(
					map[string][]string{
						"ids": ids,
					},
				)
				if err != nil {
					panic(err)
				}

				resp, err := http.Post("http://grch37.rest.ensembl.org/lookup/id", "application/json", bytes.NewBuffer(reqBody))
				if err != nil {
					panic(err)
				}

				// body, err := ioutil.ReadAll(resp.Body)
				// if err != nil {
				// 	panic(err)
				// }
				// log.Println(string(body))

				resBodyMap := map[string]interface{}{}

				json.NewDecoder(resp.Body).Decode(&resBodyMap)

				for _, row := range rows {
					if _, ok := resBodyMap[row[0]]; ok {

						val, typeCheck := resBodyMap[row[0]].(map[string]interface{})
						if !typeCheck {
							log.Panicf("Typecheck on response body failed for val [%s] - chr[%d] - dir[%s]",
								row[0],
								chr,
								dir,
							)
						}

						output = append(output, []byte(val["display_name"].(string)+",")...)
						output = append(output, []byte(strings.Join(row, ",")+"\n")...)

					} else {
						log.Panicf("Ensembl rest request failed for val [%s] - chr[%d] - dir[%s]",
							row[0],
							chr,
							dir,
						)
					}
				}

				err = ioutil.WriteFile(fmt.Sprintf("%schr%d_fixed.csv", dir, chr), output, 0644)
				if err != nil {
					log.Fatalln(err)
				}

			}
		}
	}

}

func getRelationMap(startingKey string) map[string]string {

	db, err := sql.Open("mysql", "genome@tcp(genome-mysql.soe.ucsc.edu:3306)/hg19")
	if err != nil {
		panic(err)
	}

	defer db.Close()

	var query string

	if startingKey == "UniprotID" {
		query = `SELECT
			kxr.spID,
			MAX(kxr.genesymbol)
			FROM hg19.knownGene AS kg
			JOIN hg19.kgXref AS kxr ON kxr.kgID = kg.name
			group by kxr.spID
			`
	} else if startingKey == "EnsemblGeneID" {
		query = `SELECT DISTINCT
			e.name2 "ens_id",
			kxr.genesymbol "genesymbol"
			FROM hg19.knownGene AS kg
			JOIN hg19.kgXref AS kxr ON kxr.kgID = kg.name
			JOIN hg19.knownToEnsembl AS kte ON kte.name = kg.name
			JOIN hg19.ensGene AS e on e.name = kte.value
			where kxr.spID != ''
			`
	} else {
		log.Panicln("Unknown starting key in getRelationMap()")
	}

	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	retMap := map[string]string{}

	for rows.Next() {

		var (
			Key        string
			GeneSymbol string
		)

		err = rows.Scan(
			&Key,
			&GeneSymbol,
		)
		if err != nil {
			panic(err)
		}

		if _, ok := retMap[Key]; !ok {
			retMap[Key] = GeneSymbol
		} else {
			log.Printf("getRelationMap() key previously exists in map key: [%s] val: [%s]", Key, GeneSymbol)
			//log.Panicf("getRelationMap() key previously exists in map key: [%s] val: [%s]", Key, GeneSymbol)
		}

	}

	return retMap
}
