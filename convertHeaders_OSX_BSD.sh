#!/bin/bash
sed -i '' '1s/\.//g' results/eqtl-overlap/Header.csv
sed -i '' '1s/\.//g' results/pqtl-overlap/Header.csv
sed -i '' '1s/\.//g' results/pqtl/Header.csv

sed -i '' '1s/^/\"GeneSymbol\",/g' results/eqtl-overlap/Header.csv
sed -i '' '1s/^/\"GeneSymbol\",/g' results/pqtl-overlap/Header.csv
sed -i '' '1s/^/\"GeneSymbol\",/g' results/pqtl/Header.csv