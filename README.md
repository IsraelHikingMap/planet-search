# Global Search

This repo allows both the creation of global POIs PMTiles using maptiler and also creating a global search using elasticsearch.
Make sure there's an elasticsearch running at http://localhost:9200.

To compile run:

`mvn package`

To run do:

`java -jar ./target/global-search-1.0-SNAPSHOT-jar-with-dependencies.jar`

To serve the PMTiles run:

`docker run -p 7777:8080 -v $(pwd)/data/:/data/ --rm protomaps/go-pmtiles serve /data/ --public-url=http://localhost:7777 --cors=\*`
