# FixResults

Maps Uniprot ID to HGNC Gene Symbol
Add's log10pvalue
Remove's bad elastic search characters from header names

Previously also 
- checked for missing, NA, or multiple values (containing semi colon) in any of the gene symbol fields (Uniprot, ens id, genesymbol)
- mapped ens id's to HGNC Gene Symbol for eQTL data set