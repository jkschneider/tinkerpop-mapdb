package io.jons.tinkerpop.mapdb;

import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.Property;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;
import java.util.stream.Collectors;

public abstract class MapdbElement implements Element, Element.Iterators {
    protected Map<String, List<Property>> properties = new HashMap<>();
    protected Object id;
    protected int graphId;
    protected String label;
    protected boolean removed = false;

    protected MapdbElement(final Object id, final String label, final MapdbGraph graph) {
        this.graphId = graph.instanceId;
        this.label = label;
        this.id = id;
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() { return MapdbGraphRegistry.find(graphId); }

    protected MapdbGraph mapdbGraph() { return MapdbGraphRegistry.find(graphId); }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw Element.Exceptions.elementAlreadyRemoved(this.getClass(), this.id);
        return this.properties.containsKey(key) ? this.properties.get(key).get(0) : Property.<V>empty();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V> Iterator<? extends Property<V>> propertyIterator(String... propertyKeys) {
        if (propertyKeys.length == 1) {
            final List<Property> properties = this.properties.getOrDefault(propertyKeys[0], Collections.emptyList());
            if (properties.size() == 1) {
                return IteratorUtils.of(properties.get(0));
            } else if (properties.isEmpty()) {
                return Collections.emptyIterator();
            } else {
                return (Iterator) new ArrayList<>(properties).iterator();
            }
        }

        // TODO we need to run .collect(..) here in order to prevent ConcurrentModificationExceptions, but is there a better way?
        return (Iterator) this.properties.entrySet().stream()
                .filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys))
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toList()).iterator();
    }
}
