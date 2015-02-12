package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.Vertex;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.Test;
import org.mapdb.Store;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MapdbGraphSerializationTest {
    @Test
    public void subgraphsCanBeSerializedAndDeserializedCompletely() {
        Map<String, String> confMap = new HashMap<>();
        Configuration c = new MapConfiguration(confMap);
        c.setProperty("storage.cacheSize", 1);
        MapdbGraph g = MapdbGraph.open(c);
        Store s = g.store();

        int testSize = 1000;

        Vertex prev = null;
        for(int i = 0; i < testSize; i++) {
            Vertex v = g.addVertex("name", i);
            if(prev != null)
                v.addEdge("to", prev);
            prev = v;

            if(i % 1000 == 0) {
                System.out.println(s.getCurrSize());
            }
        }

        assertNotNull(prev);
        for(int i = testSize-2; i >= 0; i--) {
            assertEquals(i, (int) prev.out().next().value("name"));
            assertEquals("vertex " + i + " does not have any in vertices",
                    1, prev.out().count().next().intValue());
            prev = prev.out().next();
        }
    }
}
