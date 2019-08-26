package main

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	// "time"
)

import (
	yaml "gopkg.in/yaml.v2"
)

//BystroToMem takes bystro output and loads into memory
func BystroToMem(chr int, wantedFields []string, bystroMapRef *map[string][]string) {

	bystroMap := *bystroMapRef

	fh, err := os.Open(fmt.Sprintf("bystro/AmpAD.chr%d.tsv.gz", chr))
	if err != nil {
		panic(err)
	}

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

	log.Println("len(header)", len(header))

	headerMapIndices := []int{}
	chromIndex := -1
	posIndex := -1

	for _, o := range wantedFields {

		index := -1
		for j, o2 := range header {
			if o2 == o {
				index = j
				if o == "chrom" {
					chromIndex = j
				} else if o == "pos" {
					posIndex = j
				}
				break
			}
		}

		if index == -1 {
			log.Panicf("%s : Value not found in bystro header\n", o)
		} else {
			headerMapIndices = append(headerMapIndices, index)
		}

	}

	if chromIndex == -1 {
		log.Panicf("chrom not found in bystro header\n")
	}

	if posIndex == -1 {
		log.Panicf("pos not found in bystro header\n")
	}

	log.Println("len(headerMapIndices)", len(headerMapIndices))

	i := 0
	j := 0

	for {
		line := []string{}

		cols, error := reader.Read()
		if error == io.EOF {
			log.Println("EOF")
			break
		} else if error != nil {
			panic(error)
		}

		for _, index := range headerMapIndices {
			line = append(line, cols[index])
		}

		bystroMap[cols[chromIndex]+":"+cols[posIndex]] = line
		i++
		if i%11837 == 0 {
			j++
			log.Printf("%d%% complete", j)
		}
	}

	printMemUsage()
}

// readYAML reads from an io.Reader and populates an interface returning an
// an error from reading the io.Reader or unmarshalling, as needed.
func readYAML(r io.Reader, i interface{}) error {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, i)
}

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
