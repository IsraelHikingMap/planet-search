package il.org.osm.israelhiking;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;

import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * The main entry point
 */
public class MainClass {

    private static final Logger LOGGER = Logger.getLogger(MainClass.class.getName());

    /** Static utility class should not be instantiated. */
    private MainClass() {
    }

    /**
     * Main entry point for the application.
     * 
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        run(Arguments.fromArgsOrConfigFile(args));
    }

    static void run(Arguments args) throws Exception {
        // skip-tiles (default OFF): when set, collapse the vector-tile pyramid to a single
        // trivial z0 tile so the .pmtiles archive write — which on a whole-planet run is the
        // z6..z14 archive stage that takes ~30-60 min AFTER the ES ingest already finished —
        // becomes near-instant. The ES bulk-ingest is driven by PlanetSearchProfile.processFeature
        // during OSM pass2 and is completely independent of tile zoom (it does not read maxzoom),
        // so the search index (points/bbox) is built identically; only the map-tile output is
        // degraded to a stub. We layer maxzoom/render_maxzoom=0 OVER the caller's args (the user's
        // explicit args still win for everything else) BEFORE Planetiler.create reads the config.
        // Use for our own reindex turnarounds; leave OFF for the client / whole-planet map build
        // that actually needs the tiles. See ADR-0016.
        boolean skipTiles = args.getBoolean("skip-tiles",
                "Collapse tile output to z0 (near-instant archive) to speed up an ES-only reindex; "
                        + "map tiles become a stub. Default false — leave off when tiles are needed.",
                false);
        if (skipTiles) {
            LOGGER.warning("skip-tiles=true: collapsing tile output to z0 (maxzoom=0). The ES search "
                    + "index is built normally; the .pmtiles archive will be a stub. Do NOT use for a "
                    + "build whose map tiles are consumed (client / production map).");
            args = Arguments.of("maxzoom", "0", "render_maxzoom", "0").orElse(args);
        }

        // Planetiler is a convenience wrapper around the lower-level API for the most
        // common use-cases.
        // See ToiletsOverlayLowLevelApi for an example using the lower-level API
        Planetiler planetiler = Planetiler.create(args);

        var esAddress = args.getString("es-address", "Elasticsearch address", "http://localhost:9200");
        var esClient = ElasticsearchHelper.createElasticsearchClient(esAddress);
        try {
            var pointsIndexAlias = args.getString("es-points-index-alias", "Elasticsearch index to populate points",
                    "points");
            var bboxIndexAlias = args.getString("es-bbox-index-alias", "Elasticsearch index to populate bounding boxes",
                    "bbox");
            var supportedLanguages = args.getString("languages", "Languages to support", "en,he,ru,ar,es").split(",");
            var externalFilePath = args.getString("external-file-path", "External file path", "");
            var qrankPath = args.getString("qrank-path",
                    "Path to qrank.csv.gz for the prominence signal (empty = run without it)", "");
            var qrankIndex = QRankIndex.load(qrankPath.isEmpty() ? null : Path.of(qrankPath));

            // SAFETY GUARD (see ElasticsearchHelper.assertSafeToReindex): this indexer
            // DELETES+RECREATES its target index on every run, then swaps the alias. If a
            // populated, live index is already aliased (e.g. the whole-planet build), an
            // unintentional run — like a stray "docker compose up site" defaulting to
            // area=us/colorado — would destroy it. Before deleting anything, refuse to
            // proceed when the LIVE (currently-aliased) doc-count is at/above the
            // threshold unless an intentional reindex was requested via --force-reindex.
            var forceReindex = args.getBoolean("force-reindex",
                    "Allow reindexing OVER a populated live index (intentional rebuild, e.g. make prod)",
                    false);
            var minProtectDocs = args.getLong("min-protect-docs",
                    "Protect the live index from being reindexed when it has at least this many docs",
                    1_000_000L);
            ElasticsearchHelper.assertSafeToReindex(esClient, pointsIndexAlias, minProtectDocs, forceReindex);
            ElasticsearchHelper.assertSafeToReindex(esClient, bboxIndexAlias, minProtectDocs, forceReindex);

            var targetPointsIndex = ElasticsearchHelper.createPointsIndex(esClient, pointsIndexAlias,
                    supportedLanguages);
            var targetBBoxIndex = ElasticsearchHelper.createBBoxIndex(esClient, bboxIndexAlias, supportedLanguages);
            var profile = new PlanetSearchProfile(planetiler.config(), esClient, targetPointsIndex, targetBBoxIndex,
                    supportedLanguages, qrankIndex);

            String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
            planetiler.setProfile(profile);
            // Geofabrik publishes continents/countries/regions but NOT the whole planet, so
            // "geofabrik:planet" fails with "No matches for 'planet'". For the whole-planet
            // build (area=planet) use the "aws:latest" source — planetiler's Downloader maps
            // the aws: scheme to AwsOsm.OSM_PDS (the s3://osm-pds Registry-of-Open-Data mirror),
            // which carries the full planet.osm.pbf. Everything else keeps geofabrik:<area>.
            // (Valid Downloader schemes: geofabrik:, aws:, overture: — verified in planetiler 0.9.3.)
            // override this default with osm_path="path/to/data.osm.pbf"
            String osmSourceUrl = "planet".equals(area) ? "aws:latest" : "geofabrik:" + area;
            planetiler.addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), osmSourceUrl);
            if ("" != externalFilePath) {
                planetiler.addGeoJsonSource("external", Path.of(externalFilePath));
            }
            // override this default with mbtiles="path/to/output.mbtiles"
            planetiler.overwriteOutput(Path.of("data", "target", PlanetSearchProfile.POINTS_LAYER_NAME + ".pmtiles"));
            planetiler.run();

            // Flush every buffered document and wait for the in-flight bulk
            // requests to finish BEFORE the alias swap, so the new index is fully
            // populated when it goes live. Then refresh so docs are searchable.
            profile.flush();
            esClient.indices().refresh(r -> r.index(targetPointsIndex, targetBBoxIndex));

            // Restore normal refresh/replica settings (they were disabled during
            // the build to speed up bulk indexing) before the indexes go live.
            ElasticsearchHelper.restoreSearchSettings(esClient, targetPointsIndex);
            ElasticsearchHelper.restoreSearchSettings(esClient, targetBBoxIndex);

            ElasticsearchHelper.switchAlias(esClient, pointsIndexAlias, targetPointsIndex);
            ElasticsearchHelper.switchAlias(esClient, bboxIndexAlias, targetBBoxIndex);

            // bbox geo_shape rejects (a few degenerate OSM relation geometries ES
            // can't parse) are tolerated — they don't affect name search — but we
            // surface them so they're never silent.
            if (profile.getFailedBboxCount() > 0) {
                LOGGER.warning("Indexing dropped " + profile.getFailedBboxCount()
                        + " bbox document(s) (geo_shape rejects); tolerated — name search unaffected.");
            }
            // Transient whole-batch charges (after retries exhausted) are likely
            // phantom — ES may have committed them after the client timed out — so
            // they are surfaced but tolerated up to a generous threshold; the
            // reconcile gate below is the authoritative check on real loss.
            if (profile.getTransientCharges() > 0) {
                LOGGER.warning("Indexing had " + profile.getTransientCharges()
                        + " transient charge(s) after retries (" + profile.getTransientPointsCharges()
                        + " points, " + profile.getTransientBboxCharges()
                        + " bbox); likely phantom timeouts — verifying via reconcile gate.");
            }

            // Step D — two-bucket guard. Fail the process only when POINTS loss
            // exceeds the bucket thresholds (genuine per-item data failures held to a
            // strict bar; transient whole-batch charges tolerated far more generously).
            // The all-or-nothing `>0` predicate is gone: 161 phantom timeouts no
            // longer turn a fully-restored index into an exit-1.
            if (profile.hasIndexingFailures()) {
                throw new IllegalStateException("Indexing finished with " + profile.getFailedPointsCount()
                        + " genuine + " + profile.getTransientPointsCharges()
                        + " transient failed POINTS document(s) out of " + profile.getEmittedCount()
                        + " emitted (" + profile.getIndexedCount() + " indexed, "
                        + profile.getFailedBboxCount() + " bbox dropped). "
                        + "Refusing to treat a partial points index as success.");
            }

            // Step D — post-build reconcile gate (the robust backstop). Independent of
            // charge-counting: read the LIVE points alias doc-stats and compare against
            // emitted - genuine-data-failures. Re-emitting the same id overwrites in
            // place, so we compare against count+deleted (distinct ops landed), not the
            // bare live count, to avoid mistaking legitimate dedup for loss. Fail if the
            // index is short by more than 0.1%.
            var pointsStats = ElasticsearchHelper.getLiveAliasDocsStats(esClient, pointsIndexAlias);
            String reconcileFailure = ElasticsearchHelper.reconcileLivePoints(
                    profile.getEmittedPointsCount(), profile.getFailedPointsCount(), pointsStats, 0.001);
            if (reconcileFailure != null) {
                throw new IllegalStateException(reconcileFailure);
            }
        } finally {
            esClient.close();
        }
    }
}
