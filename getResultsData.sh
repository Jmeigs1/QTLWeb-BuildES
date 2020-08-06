#!/bin/bash

# mkdir -p results/eqtl-overlap
# mkdir -p results/pqtl-overlap
mkdir -p results/pqtl

# aws s3 cp s3://final-eqtl-overlap-results/eQTLoverlappQTL_for_brainqtl_Aug21.csv results/eqtl-overlap/
# aws s3 cp s3://final-pqtl-overlap-results/pQTLoverlapeQTL_for_brainqtl_Aug21.csv results/pqtl-overlap/
aws s3 cp s3://brainqtl-data/july2020/brainQTL_July2020_2.csv  results/pqtl/

