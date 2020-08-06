# QTLWeb-BuildES
Index ElasticSearch data for QTLWeb

## Dependencies

Golang 1.12.4

AWS CLI

ElasticSearch 7.2.0

# Setup

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
