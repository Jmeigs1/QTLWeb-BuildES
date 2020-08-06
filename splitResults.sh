#!/bin/bash

# awk -F "\"*,\"*" \
# '{print >> ("results/eqtl-overlap/"$2".csv");
# close("results/eqtl-overlap/"$2".csv")}' \
# results/eqtl-overlap/eQTLoverlappQTL_for_brainqtl_Aug21.csv
# mv results/eqtl-overlap/Chromosome.csv results/eqtl-overlap/Header.csv

# awk -F "\"*,\"*" \
# '{print >> ("results/pqtl-overlap/"$2".csv");
# close("results/pqtl-overlap/"$2".csv")}' \
# results/pqtl-overlap/pQTLoverlapeQTL_for_brainqtl_Aug21.csv
# mv results/pqtl-overlap/Chromosome.csv results/pqtl-overlap/Header.csv

sed -i 's/^/,/' results/pqtl/brainQTL_July2020.csv
awk -F "\"*,\"*" \
'{print >> ("results/pqtl/chr"$2".csv");
close("results/pqtl/chr"$2".csv")}' \
results/pqtl/brainQTL_July2020.csv
mv results/pqtl/chrChr.csv results/pqtl/Header.csv
sed -i 's/^.//' results/pqtl/*.csv

# Assumes chr is the first column
# TODO: Parse column headers for "chr" or "Chromosome"