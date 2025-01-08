package com.example;

import com.onthegomap.planetiler.Planetiler;
import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;



/**
 * Hello world!
 */
public class MainClass {

    /** 
     * Main entry point for the application.
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        run(Arguments.fromArgsOrConfigFile(args));
    }
    
    static void run(Arguments args) throws Exception {
        String area = args.getString("area", "geofabrik area to download", "israel-and-palestine");
        // Planetiler is a convenience wrapper around the lower-level API for the most common use-cases.
        // See ToiletsOverlayLowLevelApi for an example using the lower-level API
        Planetiler planetiler = Planetiler.create(args);
        planetiler.setProfile(new GlobalSearchProfile(planetiler.config()))
          // override this default with osm_path="path/to/data.osm.pbf"
          .addOsmSource("osm", Path.of("data", "sources", area + ".osm.pbf"), "geofabrik:" + area)
          // override this default with mbtiles="path/to/output.mbtiles"
          .overwriteOutput(Path.of("data", "global_points.pmtiles"))
          .run();
    }
}
