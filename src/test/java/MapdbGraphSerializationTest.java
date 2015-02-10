import com.github.jkschneider.tinkermapdb.graph.MapdbGraph;
import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.Test;
import org.mapdb.Store;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

public class MapdbGraphSerializationTest {
    @Test
    public void vertexCanBeSerializedAndDeserializedCompletely() {
        Map<String, String> confMap = new HashMap<>();
        Configuration c = new MapConfiguration(confMap);
        c.setProperty("storage.cacheSize", 1);
        MapdbGraph g = MapdbGraph.open(c);
        Store s = g.store();

        int testSize = 1000;
        Random r = new Random();

        Vertex prev = null;
        for(int i = 0; i < testSize; i++) {
            byte[] name = new byte[1000];
            r.nextBytes(name);
            Vertex v = g.addVertex("name", name);
            if(prev != null)
                v.addEdge("to", prev);
            prev = v;

            if(i % 1000 == 0) {
                System.out.println(s.getCurrSize());
            }
        }

        assertNotNull(prev);
        for(int i = 1; i < testSize; i++) {
            assertEquals("vertex " + i + " does not have any in vertices",
                    1, prev.out().count().next().intValue());
            prev = prev.out().next();
        }
    }
}
