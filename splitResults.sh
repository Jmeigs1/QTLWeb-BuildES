#!/bin/bash

results=("results/banner/BannerBBDP_DLPFC_pQTLs_fixed.csv" "results/rosmap/ROSMAP_DLPFC_pQTLs_fixed.csv" "results/rosmap_control/ROSMAP_NCI_DLPFC_pQTLs_fixed.csv")

for r in ${results[@]}; do
    awk -v dir=$(dirname $r) -F "\"*,\"*" \
    '{print >> (dir"/chr"$1".csv");
    close(dir"/chr"$1".csv")}' \
    $r

    mv $(dirname $r)/chrChr.csv $(dirname $r)/Header.csv
done

# Assumes chr is the first column
# TODO: Parse column headers for "chr" or "Chromosome"