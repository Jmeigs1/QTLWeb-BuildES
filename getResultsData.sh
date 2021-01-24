#!/bin/bash

mkdir -p results/banner/
mkdir -p results/rosmap/
mkdir -p results/rosmap_control/

aws s3 cp s3://brainqtl-data/2020-01-19/BannerBBDP_DLPFC_pQTLs_fixed.csv results/banner/
aws s3 cp s3://brainqtl-data/2020-01-19/ROSMAP_DLPFC_pQTLs_fixed.csv results/rosmap/
aws s3 cp s3://brainqtl-data/2020-01-19/ROSMAP_NCI_DLPFC_pQTLs_fixed.csv results/rosmap_control/
