package com.github.jkschneider.tinkermapdb.graph;

import java.util.HashMap;
import java.util.Map;

class MapdbGraphRegistry {
    static Map<Integer, MapdbGraph> registry = new HashMap<>();

    public static void addGraph(MapdbGraph g) {
        registry.put(g.instanceId, g);
    }

    public static MapdbGraph find(Integer instanceId) {
        return registry.get(instanceId);
    }
}
