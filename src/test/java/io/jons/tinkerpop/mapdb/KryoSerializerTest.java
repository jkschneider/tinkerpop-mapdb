package io.jons.tinkerpop.mapdb;

import org.junit.Test;

import java.io.*;
import static org.junit.Assert.*;

public class KryoSerializerTest {
    @Test
    public void roundTripVertex() throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(byteOut);

        KryoSerializer<MapdbVertex> serializer = new KryoSerializer<>(MapdbVertex.class);

        MapdbGraph g = MapdbGraph.open();
        MapdbVertex v = (MapdbVertex) g.addVertex("name", "abc");

        serializer.serialize(out, v);

        ByteArrayInputStream byteIn = new ByteArrayInputStream(byteOut.toByteArray());
        DataInput in = new DataInputStream(byteIn);

        MapdbVertex v2 = serializer.deserialize(in, 0);
        assertEquals("abc", v2.value("name"));
    }
}
