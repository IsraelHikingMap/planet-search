package il.org.osm.israelhiking;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("e2e")
public class E2ETest {
    @Test
    public void test() throws Exception {
        MainClass.main(new String[]{ "--download", "--external-file-path", "./src/test/resources/extenal.geojson"});
    }
}
