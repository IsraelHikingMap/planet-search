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
| `update-templates-only` | Store the search templates of this build in Elasticsearch and exit, without building anything. Updates the queries of a live index without a reindex | `false` |

The QRank data file comes from [https://qrank.toolforge.org](https://qrank.toolforge.org) (CC0): a gzipped CSV (`Entity,QRank`) ranking Wikidata entities by aggregated Wikimedia pageviews. `qrank-path` is optional and fully omittable — omit it and the build runs unchanged without the ~363 MB file.

## Search templates

The queries themselves live here as well: they are stored in Elasticsearch as [mustache search templates](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-template.html) at the end of every build, right before the aliases are switched — so the live index and the queries that assume it are always swapped together. The query side does not build queries, it only sends parameters:

```sh
curl -s localhost:9200/points/_search/template -H 'Content-Type: application/json' -d '{
  "id": "points_search",
  "params": { "searchTerm": "חיפה", "hasCenter": true, "lat": 32.79, "lng": 34.99, "zoom": 12 }
}'
```

| Template | Used for | Parameters |
|-|-|-|
| `points_search` | The main search. Add `place` to limit it to a container — a "point, place" search that resolves in one query, since every point already carries its containers (see [Containers on points](#containers-on-points)) | `searchTerm`, `prefix`, `hasCenter`, `lat`, `lng`, `zoom`, `place` |
| `points_search_exact` | A quoted search, matches the whole name only | `searchTerm` |
| `bbox_contains` | Finds the container of a coordinate, i.e. which place a point is in | `shape` |

Each template is a self contained query, tuning included, with two kinds of placeholders:

- `[[#languages]]...[[lang]]...[[/languages]]` is expanded when the template is rendered, into one clause per supported language, since the languages are a build argument. This is the only build time placeholder.
- `{{...}}` is left as is and is expanded by Elasticsearch on every search, from the parameters the caller sends.

The relevance signals that depend on the zoom level (the strength of the map center, the gaussian decay and the viewport boost) are computed inside the query in painless, from the raw `zoom` parameter — so the caller never has to compute them, and the tuning stays here next to the tests that score it.

### Updating the queries of a live index

A query can be changed without a reindex: `update-templates-only` stores the templates of the build in Elasticsearch and exits, leaving the indices and their data exactly as they are. Searches keep being served while it runs, and the next one already uses the new query. This is how a production query is fixed in a minute instead of waiting for a planet build.

To change a query:

1. Edit the relevant file in [src/main/resources/search-templates](src/main/resources/search-templates).
2. Let the end to end test build a real index and search it, see below. It runs on every pull request.
3. Push the new query to the live Elasticsearch, using the image of the version that built the live index:

   ```sh
   docker run --rm ghcr.io/israelhikingmap/planet-search:latest \
     --update-templates-only --es-address=http://elasticsearch:9200
   ```

   Add the arguments the build was run with if they are not the default ones — `languages` matters, since it decides which per language clauses the templates get.
4. Verify what is now stored, and search with it:

   ```sh
   curl -s http://elasticsearch:9200/_scripts/points_search
   curl -s http://elasticsearch:9200/points/_search/template -H 'Content-Type: application/json' \
     -d '{ "id": "points_search", "params": { "searchTerm": "חיפה" } }'
   ```

Rolling back is the same command with the previous image tag. This works as long as the index shape and the analyzers did not change; a query that needs a new field or a different normalization needs a reindex, not a template update.

### Testing the queries

The templates are tested by [E2ETest.java](src/test/java/il/org/osm/israelhiking/E2ETest.java), which builds a real index out of a real OSM extract, through the profile, and then searches it with the templates - so a query is always checked against data that was really indexed, and not against documents a test made up. The cases are in [search-sanity-cases.json](src/test/resources/search-sanity-cases.json), in the same format as the relevance cases: a search term, an optional map center and zoom, and the place it is expected to find, i.e. a target coordinate, a radius around it and how deep in the results to look for it.

Adding a case is adding a line to that file. It runs on every pull request, and locally with:

`docker compose up -d elasticsearch && mvn test -Prun-all-tests -Dtest=E2ETest`

## Containers on points

Every point is tagged at build time with the places that contain it, so a "point, place" search — say "spring, Jerusalem" — is a single query: `points_search` filters on the point's containers with the `place` parameter. Each point carries:

- `poiParentNames` — every place it falls inside, per language; this is what `place` matches against.
- `poiContainer` — the tightest place around it, for display.
- `poiCountry` — the country, for display, shown next to the container when they differ.

Point-in-polygon can't run in the single streaming pass, because a point is read before the boundary that contains it is assembled. So containers are carried between builds through the bbox index — the same documents used to answer `bbox_contains`, no separate store: at the start of a build the live `bbox` alias still points at the previous build's containers, so the build loads them (admin boundaries up to level 8, settlements, parks and reserves) into an in-memory spatial index and tags its points from them, before swapping in its own bbox index at the end. Containers change rarely, so the one-build lag is by design.

The catch is that a fresh deployment needs **two build cycles** to fully populate: the first build has no previous bbox index to load, so its points go untagged, and the second tags its points from the first's containers. The end to end test exercises this by building twice.

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

