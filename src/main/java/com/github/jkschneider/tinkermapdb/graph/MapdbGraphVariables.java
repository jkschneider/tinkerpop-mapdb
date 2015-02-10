package com.github.jkschneider.tinkermapdb.graph;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.GraphVariableHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import org.mapdb.DB;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MapdbGraphVariables implements Graph.Variables {
    private final Map<String, Object> variables;

    public MapdbGraphVariables(MapdbGraph graph, DB db) {
        variables = db.createHashMap(graph.getName() + "-variables").make();
    }

    @Override
    public Set<String> keys() {
        return this.variables.keySet();
    }

    @Override
    public <R> Optional<R> get(final String key) {
        return Optional.ofNullable((R) this.variables.get(key));
    }

    @Override
    public void remove(final String key) {
        this.variables.remove(key);
    }

    @Override
    public void set(final String key, final Object value) {
        GraphVariableHelper.validateVariable(key, value);
        this.variables.put(key, value);
    }

    public String toString() {
        return StringFactory.graphVariablesString(this);
    }
}
