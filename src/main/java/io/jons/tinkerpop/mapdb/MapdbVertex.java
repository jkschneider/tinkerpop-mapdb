package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

public class MapdbVertex extends MapdbElement implements Vertex, Vertex.Iterators {
    protected Map<String, Set<Edge>> outEdges = new HashMap<>();
    protected Map<String, Set<Edge>> inEdges = new HashMap<>();

    private static final Object[] EMPTY_ARGS = new Object[0];

    protected MapdbVertex(final Object id, final String label, final MapdbGraph graph) {
        super(id, label, graph);
    }

    @Override
    public <V> VertexProperty<V> property(final String key) {
        if (removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        if (this.properties.containsKey(key)) {
            final List<VertexProperty> list = (List) this.properties.get(key);
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
        return MapdbHelper.addEdge(mapdbGraph(), this, (MapdbVertex) vertex, label, keyValues);
    }

    @Override
    public void remove() {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(Vertex.class, this.id);
        final List<Edge> edges = new ArrayList<>();
        this.iterators().edgeIterator(Direction.BOTH).forEachRemaining(edges::add);
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

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Edge> edgeIterator(final Direction direction, final String... edgeLabels) {
        return (Iterator) MapdbHelper.getEdges(this, direction, edgeLabels);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction, final String... edgeLabels) {
        return (Iterator) MapdbHelper.getVertices(this, direction, edgeLabels);
    }
}
