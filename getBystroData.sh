#!/bin/bash

mkdir -p bystro
aws s3 sync s3://wgs-chunk-maf-01-working/ bystro/