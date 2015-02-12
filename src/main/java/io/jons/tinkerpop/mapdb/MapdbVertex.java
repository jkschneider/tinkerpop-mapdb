package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import static com.tinkerpop.gremlin.structure.Direction.*;

public class MapdbVertex extends MapdbElement implements Vertex, Vertex.Iterators {
    protected Map<String, Map<Object, MapdbEdge>> outEdges = new ConcurrentHashMap<>();
    protected Map<String, Map<Object, MapdbEdge>> inEdges = new ConcurrentHashMap<>();

    private static final Object[] EMPTY_ARGS = new Object[0];

    protected MapdbVertex(final Object id, final String label, final MapdbGraph graph) {
        super(id, label, graph);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        if (this.properties.containsKey(key)) {
            final List<VertexProperty<V>> list = (List) this.properties.get(key);
            if (list.size() > 1)
                throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
            else
                return list.get(0);
        } else
            return VertexProperty.<V>empty();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        return this.property(key, value, EMPTY_ARGS);
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        final Optional<Object> optionalId = ElementHelper.getIdValue(keyValues);

        ElementHelper.validateProperty(key, value);
        final VertexProperty<V> vertexProperty = optionalId.isPresent() ?
                new MapdbVertexProperty<>(optionalId.get(), this, key, value) :
                new MapdbVertexProperty<>(this, key, value);
        final List<Property> list = this.properties.getOrDefault(key, new ArrayList<>());
        list.add(vertexProperty);
        this.properties.put(key, list);
        mapdbGraph().vertexIndex.autoUpdate(key, value, null, this);
        ElementHelper.attachProperties(vertexProperty, keyValues);
        this.mapdbGraph().vertices.put(id, this); // since mapdb values should be immutable
        return vertexProperty;
    }

    @Override
    public Edge addEdge(final String label, final Vertex vertex, final Object... keyValues) {
        if (null == vertex) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);

        MapdbGraph graph = mapdbGraph();
        MapdbVertex inVertex = (MapdbVertex) vertex;
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(keyValues);

        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);

        final MapdbEdge edge;
        if (null != idValue) {
            if (graph.edges.containsKey(idValue))
                throw Graph.Exceptions.edgeWithIdAlreadyExists(idValue);
        } else {
            idValue = MapdbHelper.getNextId(graph);
        }

        edge = new MapdbEdge(idValue, this, label, inVertex, graph);
        ElementHelper.attachProperties(edge, keyValues);
        graph.edges.put(edge.id, edge);
        addOutEdge(this, label, edge);
        addInEdge(inVertex, label, edge);

        graph.vertices.put(this.id, this);
        graph.vertices.put(inVertex.id, inVertex);

        return edge;
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        final List<Edge> edges = new ArrayList<>();
        this.iterators().edgeIterator(BOTH).forEachRemaining(edges::add);
        edges.stream().filter(edge -> !((MapdbEdge) edge).removed).forEach(Edge::remove);
        this.properties.clear();
        mapdbGraph().vertexIndex.removeElement(this);
        mapdbGraph().vertices.remove(this.id);
        this.removed = true;
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

    @Override
    public Vertex.Iterators iterators() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<VertexProperty<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }

    private Stream<MapdbEdge> edgeStream(Direction direction, final String... edgeLabels) {
        switch(direction) {
        case OUT:
            if (edgeLabels.length == 0)
                return outEdges.values().stream().flatMap(edgeMap -> edgeMap.values().stream());
            else if (edgeLabels.length == 1)
                return outEdges.getOrDefault(edgeLabels[0], Collections.emptyMap()).values().stream();
            return Stream.of(edgeLabels).map(outEdges::get).filter(Objects::nonNull).flatMap(edgeMap -> edgeMap.values().stream());
        case IN:
            if (edgeLabels.length == 0)
                return inEdges.values().stream().flatMap(edgeMap -> edgeMap.values().stream());
            else if (edgeLabels.length == 1)
                return inEdges.getOrDefault(edgeLabels[0], Collections.emptyMap()).values().stream();
            return Stream.of(edgeLabels).map(inEdges::get).filter(Objects::nonNull).flatMap(edgeMap -> edgeMap.values().stream());
        }
        throw new IllegalArgumentException("only IN and OUT are supported");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Edge> edgeIterator(Direction direction, final String... edgeLabels) {
        if(direction == null || direction.equals(BOTH))
            return (Iterator) Stream.concat(edgeStream(IN, edgeLabels), edgeStream(OUT, edgeLabels)).iterator();
        return (Iterator) edgeStream(direction, edgeLabels).iterator();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Vertex> vertexIterator(Direction direction, final String... edgeLabels) {
        if(direction == null) direction = BOTH;

        switch(direction) {
        case BOTH:
            return (Iterator) Stream.concat(
                    edgeStream(IN, edgeLabels).map(MapdbEdge::outVertex),
                    edgeStream(OUT, edgeLabels).map(MapdbEdge::inVertex)
            ).iterator();
        case IN:
            return (Iterator) edgeStream(IN, edgeLabels).map(MapdbEdge::outVertex).iterator();
        case OUT:
        default:
            return (Iterator) edgeStream(OUT, edgeLabels).map(MapdbEdge::inVertex).iterator();
        }
    }

    protected static void addOutEdge(final MapdbVertex vertex, final String label, final MapdbEdge edge) {
        Map<Object, MapdbEdge> edges = vertex.outEdges.get(label);
        if (null == edges) {
            edges = new ConcurrentHashMap<>();
            vertex.outEdges.put(label, edges);
        }
        edges.put(edge.id(), edge);
        vertex.mapdbGraph().vertices.put(vertex.id, vertex);
    }

    protected static void addInEdge(final MapdbVertex vertex, final String label, final MapdbEdge edge) {
        Map<Object, MapdbEdge> edges = vertex.inEdges.get(label);
        if (null == edges) {
            edges = new ConcurrentHashMap<>();
            vertex.inEdges.put(label, edges);
        }
        edges.put(edge.id(), edge);
        vertex.mapdbGraph().vertices.put(vertex.id, vertex);
    }
}
