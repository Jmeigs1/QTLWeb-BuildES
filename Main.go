package main

import (
	"log"
	"os"
)

type config struct {
	WantedFields []string `json:"wanted_fields,omitempty" yaml:"wanted_fields,omitempty" `
}

func main() {

	mysqlToEs()

	f, err := os.Open("wantedFields.yaml")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	cfg := &config{}

	err = readYAML(f, cfg)

	log.Println("len(cfg.WantedFields)", len(cfg.WantedFields))

	for chr := 1; chr < 23; chr++ {

		bystroData := make(map[string][]string, 1114167)
		log.Println("chr:", chr)
		BystroToMem(chr, cfg.WantedFields, &bystroData)
		log.Println("len(bystroData)", len(bystroData))

		ResultToEs(chr, cfg.WantedFields, &bystroData)
	}
}
