package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.VertexProperty;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class MapdbVertexProperty<V> extends MapdbElement implements VertexProperty<V>, VertexProperty.Iterators {
    protected Object vertexId;
    protected String key;
    protected V value;

    public MapdbVertexProperty(final MapdbVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(MapdbHelper.getNextId(vertex.mapdbGraph()), key, vertex.mapdbGraph());
        this.vertexId = vertex.id;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public MapdbVertexProperty(final Object id, final MapdbVertex vertex, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key, vertex.mapdbGraph());
        this.vertexId = vertex.id;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public Object id() {
        return this.id;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        final Property<U> property = new MapdbProperty<U>(this, key, value);
        this.properties.put(key, Collections.singletonList(property));
        mapdbGraph().vertices.put(vertexId, element());
        return property;
    }

    @Override
    public Vertex element() {
        return mapdbGraph().vertexIterator(vertexId).next();
    }

    @Override
    public void remove() {
        MapdbVertex vertex = (MapdbVertex) element();
        if (vertex.properties.containsKey(this.key)) {
            vertex.properties.get(this.key).remove(this);
            if (vertex.properties.get(this.key).size() == 0) {
                vertex.properties.remove(this.key);
                mapdbGraph().vertexIndex.remove(this.key, this.value, vertex);
            }
            final AtomicBoolean delete = new AtomicBoolean(true);
            vertex.propertyIterator(this.key).forEachRemaining(property -> {
                if (property.value().equals(this.value))
                    delete.set(false);
            });
            if (delete.get()) mapdbGraph().vertexIndex.remove(this.key, this.value, vertex);
            this.properties.clear();
            this.removed = true;
        }
    }

    //////////////////////////////////////////////

    public VertexProperty.Iterators iterators() {
        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <U> Iterator<Property<U>> propertyIterator(final String... propertyKeys) {
        return (Iterator) super.propertyIterator(propertyKeys);
    }
}
