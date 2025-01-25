# Planet-Search

<img src="https://github.com/user-attachments/assets/ab2b8b66-5b0c-43ef-b330-543416c10f8a" height="256" /> <img src="https://github.com/user-attachments/assets/c3a9b1e9-34dc-45cf-a981-d60d80d961cf" height="256" />

[![Codecov](https://img.shields.io/codecov/c/github/israelhikingmap/planet-search/main.svg)](https://codecov.io/gh/IsraelHikingMap/planet-search/)

A combination of planetiler and elasticsearch to create tiles for points for interest on one hand and a searchabale index on the other.

This repo allows both the creation of global POIs PMTiles using planetiler and also creating a global search using elasticsearch.

Additional arguments to this wrapper besides the Planetiler's arguments:
| Parameter Name | Description | Default Value |
|-|-|-|
| `languages` | A comma separated list of languages to add to the tiles and search | `en,he` |
| `es-address` | The address of the Elasticsearch database | `http://localhost:9200` |
| `es-points-index-alias` | The alias of the index to insert points into, it will create "1" and "2" suffix for the relevant index before switching | `points` |
| `es-bbox-index-alias` | The alias of the index to insert bounding boxes into, it will create "1" and "2" suffix for the relevant index before switching | `bbox` |

To run using docker (While having a local elasticsearch running at 9200):

`docker run --rm --network=host -e JAVA_OPTS="-Xmx4g -Xms4g" -v "$(pwd)/data":/app/data ghcr.io/israelhikingmap/planet-search --download`

To compile run:

`mvn package`

To run do:

`java -jar ./target/planet-search-1.0.jar`

To serve the PMTiles run:

`docker run -p 7777:8080 -v $(pwd)/data/:/data/ --rm protomaps/go-pmtiles serve /data/ --public-url=http://localhost:7777 --cors=\*`

