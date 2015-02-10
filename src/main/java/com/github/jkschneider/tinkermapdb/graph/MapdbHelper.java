package com.github.jkschneider.tinkermapdb.graph;

import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.*;
import java.util.stream.Stream;

public class MapdbHelper {
    protected synchronized static long getNextId(final MapdbGraph graph) {
        return Stream.generate(() -> (++graph.currentId)).filter(id -> !graph.vertices.containsKey(id) && !graph.edges.containsKey(id)).findAny().get();
    }

    protected static Edge addEdge(final MapdbGraph graph, final MapdbVertex outVertex, final MapdbVertex inVertex, final String label, final Object... keyValues) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        final Edge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = MapdbHelper.getNextId(graph);
        }

        edge = new MapdbEdge(idValue, outVertex, label, inVertex, graph);
        ElementHelper.attachProperties(edge, keyValues);
        graph.edges.put(edge.id(), edge);
        MapdbHelper.addOutEdge(outVertex, label, edge);
        MapdbHelper.addInEdge(inVertex, label, edge);
        return edge;
    }

    protected static void addOutEdge(final MapdbVertex vertex, final String label, final Edge edge) {
        Set<Edge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.outEdges.put(label, edges);
        }
        edges.add(edge);
    }

    protected static void addInEdge(final MapdbVertex vertex, final String label, final Edge edge) {
        Set<Edge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new HashSet<>();
            vertex.inEdges.put(label, edges);
        }
        edges.add(edge);
    }

    public static List<MapdbVertex> queryVertexIndex(final MapdbGraph graph, final String key, final Object value) {
        return graph.vertexIndex.get(key, value);
    }

    public static List<MapdbEdge> queryEdgeIndex(final MapdbGraph graph, final String key, final Object value) {
        return graph.edgeIndex.get(key, value);
    }

    @SuppressWarnings("unchecked")
    public static final Iterator<MapdbEdge> getEdges(final MapdbVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.outEdges.values().forEach(edges::addAll);
            else if (edgeLabels.length == 1)
                edges.addAll(vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
            else
                Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.inEdges.values().forEach(edges::addAll);
            else if (edgeLabels.length == 1)
                edges.addAll(vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()));
            else
                Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).forEach(edges::addAll);
        }
        return (Iterator) edges.iterator();
    }

    @SuppressWarnings("unchecked")
    public static final Iterator<MapdbVertex> getVertices(final MapdbVertex vertex, final Direction direction, final String... edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.outEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((MapdbEdge) edge).inVertex())));
            else if (edgeLabels.length == 1)
                vertex.outEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((MapdbEdge) edge).inVertex()));
            else
                Stream.of(edgeLabels).map(vertex.outEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((MapdbEdge) edge).inVertex()));
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (edgeLabels.length == 0)
                vertex.inEdges.values().forEach(set -> set.forEach(edge -> vertices.add(((MapdbEdge) edge).outVertex())));
            else if (edgeLabels.length == 1)
                vertex.inEdges.getOrDefault(edgeLabels[0], Collections.emptySet()).forEach(edge -> vertices.add(((MapdbEdge) edge).outVertex()));
            else
                Stream.of(edgeLabels).map(vertex.inEdges::get).filter(Objects::nonNull).flatMap(Set::stream).forEach(edge -> vertices.add(((MapdbEdge) edge).outVertex()));
        }
        return (Iterator) vertices.iterator();
    }
}
