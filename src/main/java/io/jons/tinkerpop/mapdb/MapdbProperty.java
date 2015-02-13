package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public class MapdbProperty<T> implements Property<T> {
    protected String key;
    protected T value;
    protected int graphId;

    // one of the two of these will be non-null
    protected Object edgeId;
    protected Object vertexId;

    protected transient MapdbElement element;

    public MapdbProperty(MapdbElement element, String key, T value) {
        if(element instanceof MapdbEdge)
            edgeId = element.id;
        else
            vertexId = element.id;

        this.element = element;
        graphId = element.graphId;

        this.key = key;
        this.value = value;
    }

    /**
     * Kryo deserializer will call this immediately upon construction so that if the underlying edge or vertex is
     * removed from the graph later, we stil have a reference to it in a detached state
     */
    public MapdbProperty<T> refreshElement() {
        if(vertexId != null)
            element = (MapdbElement) MapdbGraphRegistry.find(graphId).vertexIterator(vertexId).next();
        else if(edgeId != null)
            element = (MapdbElement) MapdbGraphRegistry.find(graphId).edgeIterator(edgeId).next();
        return this;
    }

    @Override
    public Element element() {
        return element;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public T value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return null != this.value;
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        element.properties.remove(this.key);
        if (element instanceof Edge)
            element.mapdbGraph().edgeIndex.remove(key, value, (MapdbEdge) element);
    }
}
