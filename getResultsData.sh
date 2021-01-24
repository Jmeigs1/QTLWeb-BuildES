#!/bin/bash

mkdir -p results

aws s3 cp s3://brainqtl-data/2020-01-19/BannerBBDP_DLPFC_pQTLs_fixed.csv results/
aws s3 cp s3://brainqtl-data/2020-01-19/ROSMAP_DLPFC_pQTLs_fixed.csv results/
aws s3 cp s3://brainqtl-data/2020-01-19/ROSMAP_NCI_DLPFC_pQTLs_fixed.csv results/

