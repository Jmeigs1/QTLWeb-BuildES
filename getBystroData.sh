#!/bin/bash

mkdir -p bystro
aws s3 sync s3://eicc-account-data/wgs-chunk-maf-01-working/ bystro/