# Planet-Search

<img src="https://github.com/user-attachments/assets/ab2b8b66-5b0c-43ef-b330-543416c10f8a" height="256" /> <img src="https://github.com/user-attachments/assets/c3a9b1e9-34dc-45cf-a981-d60d80d961cf" height="256" />

A combination of planetiler and elasticsearch to create tiles for points for interest on one hand and a searchabale index on the other.

This repo allows both the creation of global POIs PMTiles using planetiler and also creating a global search using elasticsearch.

Additional arguments to this wrapper besides the Planetiler's arguments:
| Parameter Name | Description | Default Value |
|-|-|-|
| `languages` | A comma separated list of languages to add to the tiles and search | `en,he` |
| `es-address` | The address of the Elasticsearch database | `http://localhost:9200` |
| `es-index-alias` | The alias of the index to insert into, it will create "1" and "2" suffix for the relevant index before switching | `points` |

To run using docker:



To compile run:

`mvn package`

To run do:

`java -jar ./target/planet-search-1.0-SNAPSHOT-jar-with-dependencies.jar`

To serve the PMTiles run:

`docker run -p 7777:8080 -v $(pwd)/data/:/data/ --rm protomaps/go-pmtiles serve /data/ --public-url=http://localhost:7777 --cors=\*`

