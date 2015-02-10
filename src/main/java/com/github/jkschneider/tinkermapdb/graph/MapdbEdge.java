package com.github.jkschneider.tinkermapdb.graph;

import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class MapdbEdge extends MapdbElement implements Edge, Edge.Iterators {
    protected Object inVertexId;
    protected Object outVertexId;

    protected MapdbEdge(final Object id, final Vertex outVertex, final String label, final Vertex inVertex, final MapdbGraph graph) {
        super(id, label, graph);
        this.outVertexId = outVertex.id();
        this.inVertexId = inVertex.id();
        graph.edgeIndex.autoUpdate(T.label.getAccessor(), this.label, null, this);
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        ElementHelper.validateProperty(key, value);
        final Property oldProperty = super.property(key);
        final Property<V> newProperty = new MapdbProperty<>(this, key, value);
        this.properties.put(key, Collections.singletonList(newProperty));
        mapdbGraph().edgeIndex.autoUpdate(key, value, oldProperty.isPresent() ? oldProperty.value() : null, this);
        return newProperty;
    }

    @Override
    public void remove() {
        if (this.removed)
            throw Element.Exceptions.elementAlreadyRemoved(Edge.class, this.id);
        final MapdbVertex outVertex = outVertex();
        final MapdbVertex inVertex = inVertex();

        if (null != outVertex && null != outVertex.outEdges) {
            final Set<Edge> edges = outVertex.outEdges.get(this.label());
            if (null != edges)
                edges.remove(this);
        }
        if (null != inVertex && null != inVertex.inEdges) {
            final Set<Edge> edges = inVertex.inEdges.get(this.label());
            if (null != edges)
                edges.remove(this);
        }

        mapdbGraph().edgeIndex.removeElement(this);
        mapdbGraph().edges.remove(this.id());
        this.properties.clear();
        this.removed = true;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);

    }

    @Override
    public Edge.Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Direction direction) {
        switch (direction) {
            case OUT:
                return IteratorUtils.of(outVertex());
            case IN:
                return IteratorUtils.of(inVertex());
            default:
                return IteratorUtils.of(outVertex(), inVertex());
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<Property<V>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }

    public MapdbVertex outVertex() {
        return (MapdbVertex) mapdbGraph().vertexIterator(this.outVertexId).next();
    }

    public MapdbVertex inVertex() {
        return (MapdbVertex) mapdbGraph().vertexIterator(this.inVertexId).next();
    }
}
