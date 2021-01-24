# QTLWeb-BuildES
Index ElasticSearch data for QTLWeb

## Dependencies

Golang 1.12.4

AWS CLI

ElasticSearch 7.2.0

# Setup

## Run FixResults
This is a one time fix.  Upload to s3 once complete.

`cd FixResults && go build && runall.sh`

## Download results data
`./getResultsData.sh`

## Split results into separate chr files
`./splitResults.sh`

## Convert headers

`convertHeaders.sh`

or

`convertHeaders_OSX_BSD.sh`

## Download annotation data
`./getBystroData.sh`

## Run program
`go run *.go`
