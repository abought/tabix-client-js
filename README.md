This is a proof-of-concept for reading tabix-indexed files
in a web browser using only javascript.

Tabix files are used to store tab-separated data in a 
compressed format while still making it easy and quick
to pull out records by an indexed "position".
([Heng Li 2011](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC3042176/))

This demo reads both the bgzipped data file and the tabix
index to extract rows in a particular range. All file
access happens locally (no server involved).
