# Planet-Search

<img src="https://github.com/user-attachments/assets/ab2b8b66-5b0c-43ef-b330-543416c10f8a" height="256" /> <img src="https://github.com/user-attachments/assets/c3a9b1e9-34dc-45cf-a981-d60d80d961cf" height="256" />

[![Codecov](https://img.shields.io/codecov/c/github/israelhikingmap/planet-search/main.svg)](https://codecov.io/gh/IsraelHikingMap/planet-search/)

A combination of planetiler and elasticsearch to create tiles for points for interest on one hand and a searchabale index on the other.

This repo allows both the creation of global POIs PMTiles using planetiler and also creating a global search using elasticsearch.

Additional arguments to this wrapper besides the Planetiler's arguments:
| Parameter Name | Description | Default Value |
|-|-|-|
| `languages` | A comma separated list of languages to add to the tiles and search | `en,he,ru,ar,es` |
| `es-address` | The address of the Elasticsearch database | `http://localhost:9200` |
| `es-points-index-alias` | The alias of the index to insert points into, it will create "1" and "2" suffix for the relevant index before switching | `points` |
| `es-bbox-index-alias` | The alias of the index to insert bounding boxes into, it will create "1" and "2" suffix for the relevant index before switching | `bbox` |
| `external-file-path` | External geojson file path to allow adding non OSM features to the search and POIs. these features should have a specific format | "empty" |
| `skip-tiles` | Collapse the tile pyramid to z0 so the `.pmtiles` archive is a near-instant stub, to speed up an Elasticsearch-only reindex. The search index is built identically; only the map tiles degrade, so do not use it for a build whose map tiles are consumed. | `false` |
| `qrank-path` | Path to a gzipped `qrank.csv.gz` used to compute the `poiProminence` ranking signal. Optional — leave empty to build without it (every point still gets a base+metadata prominence; only the QRank signal is omitted). | "empty" |

The QRank data file comes from [https://qrank.toolforge.org](https://qrank.toolforge.org) (CC0): a gzipped CSV (`Entity,QRank`) ranking Wikidata entities by aggregated Wikimedia pageviews. `qrank-path` is optional and fully omittable — omit it and the build runs unchanged without the ~363 MB file.

## External features file format

The file pointed at by `external-file-path` is a GeoJSON `FeatureCollection`. Every feature is reduced to a single point (a `Point` is used as is, a `LineString` uses the first coordinate, a polygon uses its centroid) and is added both to the search index and to the POIs tiles.

The document id in Elasticsearch is `poiSource` + `_` + `identifier`, so both must be present and the pair must be unique across the file. The `name`, `description` and `alt_name` properties follow the OSM tagging convention, i.e. a `:<language>` suffix per language plus an unsuffixed fallback, and `alt_name` accepts several values separated by `;`.

| Property | Description |
|-|-|
| `identifier` | The id of the feature within its source, used to build the document id |
| `poiSource` | The name of the source this feature came from, for example `Nakeb` |
| `poiCategory` | The category of the point, for example `Hiking`, `Water`, `Bicycle`, `4x4` |
| `poiIcon` | The icon to show for this point, for example `icon-hike` |
| `poiIconColor` | The color of the above icon |
| `poiDifficulty` | Optional, the difficulty of a route |
| `poiLength` | Optional, the length of a route in meters |
| `poiUserId` | Optional, added to the tiles feature |
| `name`, `name:<language>` | The name of the point |
| `description`, `description:<language>` | The description of the point |
| `alt_name`, `alt_name:<language>` | Alternative names, `;` separated |
| `image`, `website`, `wikidata`, `wikimedia_commons` | Optional metadata, same meaning as the OSM tags |

An example of such a file:

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [35.729807, 33.250042],
                    [35.730100, 33.251000]
                ]
            },
            "properties": {
                "identifier": "1",
                "poiSource": "MySource",
                "poiCategory": "Hiking",
                "poiIcon": "icon-hike",
                "poiIconColor": "black",
                "poiDifficulty": "easy",
                "poiLength": 4200,
                "name": "Some name",
                "name:en": "Some name",
                "description:en": "Some description",
                "alt_name:en": "Some alt name",
                "image": "https://www.example.com/image.jpg",
                "website": "https://www.example.com/hike/1"
            }
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [35.729807, 33.250042]
            },
            "properties": {
                "identifier": "2",
                "poiSource": "AnotherSource",
                "poiCategory": "Water",
                "poiIcon": "icon-tint",
                "poiIconColor": "blue",
                "name:en": "Some Eye"
            }
        }
    ]
}
```

To run using docker (While having a local elasticsearch running at 9200):

`docker run --rm --network=host -e JAVA_OPTS="-Xmx4g -Xms4g" -v "$(pwd)/data":/app/data ghcr.io/israelhikingmap/planet-search --download`

To compile run:

`mvn clean package dependency:copy-dependencies`

To run do:

`java -cp "target/classes:target/dependency/*" il.org.osm.israelhiking.MainClass`

You can also do the same with Docker locally:

`docker compose build planet-search`
`docker compose up planet-search` 

To serve the PMTiles run:

`docker run --rm -p 7777:8080 -v $(pwd)/data/target/:/data/ --rm protomaps/go-pmtiles serve /data/ --public-url=http://localhost:7777 --cors=\*`

