package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;
import java.util.stream.Stream;

public class MapdbHelper {
    protected synchronized static long getNextId(final MapdbGraph graph) {
        return Stream.generate(() -> (++graph.currentId)).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
    }

    public static List<MapdbVertex> queryVertexIndex(final MapdbGraph graph, final String key, final Object value) {
        return graph.vertexIndex.get(key, value);
    }

    public static List<MapdbEdge> queryEdgeIndex(final MapdbGraph graph, final String key, final Object value) {
        return graph.edgeIndex.get(key, value);
    }
}
