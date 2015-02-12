package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

public class MapdbProperty<V> implements Property<V> {
    protected String key;
    protected V value;
    protected int graphId;

    // one of the two of these will be non-null
    protected Object edgeId;
    protected Object vertexId;

    public MapdbProperty(MapdbElement element, String key, V value) {
        if(element instanceof MapdbEdge)
            edgeId = element.id;
        else
            vertexId = element.id;

        graphId = element.graphId;

        this.key = key;
        this.value = value;
    }

    @Override
    public Element element() {
        return this.mapdbElement();
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
        mapdbElement().properties.remove(this.key);
        if (mapdbElement() instanceof Edge)
            mapdbElement().mapdbGraph().edgeIndex.remove(key, value, (MapdbEdge) mapdbElement());
    }

    private MapdbElement mapdbElement() {
        Iterator<? extends Element> iter = vertexId != null ?
            MapdbGraphRegistry.find(graphId).vertexIterator(vertexId) :
            MapdbGraphRegistry.find(graphId).edgeIterator(edgeId);
        return iter.hasNext() ? (MapdbElement) iter.next() : null;
    }
}
