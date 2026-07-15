package il.org.osm.israelhiking;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The container side of a build, both halves in one object: it {@link #load}s
 * the previous build's containers into a queryable spatial index (used by
 * {@link #containing} to tag each point), and {@link #add}s the containers this
 * build sees so {@link #write} can persist them for the next build. Containers
 * change rarely, so the one-build lag is acceptable.
 *
 * <p>Built once (single-threaded), then both queried and appended concurrently
 * from the Planetiler worker threads: {@link STRtree} queries and
 * {@link PreparedGeometry#contains} are thread-safe once the tree is built, and
 * the append side is a {@link ConcurrentLinkedQueue}.
 */
final class ContainerIndex {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContainerIndex.class);
  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();

  /**
   * A place a point can fall inside — an admin boundary, a settlement polygon, a
   * park. Carries only what point enrichment needs: the localized names, the
   * admin level (2 == country, 0 when the container is not an admin boundary),
   * the area in m² (used to pick the tightest container), and a simplified
   * polygon for the containment test.
   */
  static final class ContainerRecord {

    static final int COUNTRY_ADMIN_LEVEL = 2;

    final Map<String, String> names;
    final int adminLevel;
    final double area;
    final Geometry geometry;

    ContainerRecord(Map<String, String> names, int adminLevel, double area, Geometry geometry) {
      this.names = names;
      this.adminLevel = adminLevel;
      this.area = area;
      this.geometry = geometry;
    }

    boolean isCountry() {
      return adminLevel == COUNTRY_ADMIN_LEVEL;
    }
  }

  private record Entry(ContainerRecord record, PreparedGeometry prepared) {
  }

  private final STRtree tree = new STRtree();
  private final int loadedCount;
  private final Collection<ContainerRecord> collected = new ConcurrentLinkedQueue<>();

  private ContainerIndex(Collection<ContainerRecord> loaded) {
    for (ContainerRecord record : loaded) {
      tree.insert(record.geometry.getEnvelopeInternal(),
          new Entry(record, PreparedGeometryFactory.prepare(record.geometry)));
    }
    tree.build();
    this.loadedCount = loaded.size();
  }

  /** Loads the previous build's containers for lookup; a missing file yields an empty index. */
  static ContainerIndex load(Path path) throws IOException {
    if (path == null || !Files.exists(path)) {
      LOGGER.info("Container index: none at {} — this build tags no points and will produce it", path);
      return new ContainerIndex(List.of());
    }
    List<ContainerRecord> records = read(path);
    LOGGER.info("Container index: loaded {} containers from {}", records.size(), path);
    return new ContainerIndex(records);
  }

  /** Remembers a container seen in this build, to be persisted by {@link #write}. */
  void add(ContainerRecord record) {
    collected.add(record);
  }

  /** The containers that enclose the given coordinate, in no particular order. */
  List<ContainerRecord> containing(double lat, double lng) {
    if (loadedCount == 0) {
      return List.of();
    }
    Point point = GEOMETRY_FACTORY.createPoint(new Coordinate(lng, lat));
    List<ContainerRecord> hits = new ArrayList<>();
    for (Object candidate : tree.query(point.getEnvelopeInternal())) {
      Entry entry = (Entry) candidate;
      if (entry.prepared().contains(point)) {
        hits.add(entry.record());
      }
    }
    return hits;
  }

  /** Persists the containers collected in this build for the next build to load. */
  void write(Path path) throws IOException {
    List<ContainerRecord> snapshot = new ArrayList<>(collected);
    WKBWriter wkbWriter = new WKBWriter();
    try (DataOutputStream out = new DataOutputStream(
        new BufferedOutputStream(new GZIPOutputStream(Files.newOutputStream(path))))) {
      out.writeInt(snapshot.size());
      for (ContainerRecord record : snapshot) {
        byte[] wkb = wkbWriter.write(record.geometry);
        out.writeInt(wkb.length);
        out.write(wkb);
        out.writeInt(record.adminLevel);
        out.writeDouble(record.area);
        out.writeInt(record.names.size());
        for (Map.Entry<String, String> name : record.names.entrySet()) {
          out.writeUTF(name.getKey());
          out.writeUTF(name.getValue());
        }
      }
    }
    LOGGER.info("Container index: wrote {} containers to {}", snapshot.size(), path);
  }

  private static List<ContainerRecord> read(Path path) throws IOException {
    WKBReader wkbReader = new WKBReader(GEOMETRY_FACTORY);
    List<ContainerRecord> records = new ArrayList<>();
    try (DataInputStream in = new DataInputStream(
        new BufferedInputStream(new GZIPInputStream(Files.newInputStream(path))))) {
      int count = in.readInt();
      for (int i = 0; i < count; i++) {
        byte[] wkb = new byte[in.readInt()];
        in.readFully(wkb);
        Geometry geometry;
        try {
          geometry = wkbReader.read(wkb);
        } catch (ParseException e) {
          throw new IOException("corrupt container geometry at record " + i, e);
        }
        int adminLevel = in.readInt();
        double area = in.readDouble();
        int nameCount = in.readInt();
        Map<String, String> names = new LinkedHashMap<>(nameCount);
        for (int n = 0; n < nameCount; n++) {
          names.put(in.readUTF(), in.readUTF());
        }
        records.add(new ContainerRecord(names, adminLevel, area, geometry));
      }
    }
    return records;
  }
}
