package com.github.jkschneider.tinkermapdb.graph;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.UnsafeInput;
import com.esotericsoftware.kryo.io.UnsafeOutput;
import com.github.jkschneider.tinkermapdb.graph.traversal.strategy.MapdbElementStepStrategy;
import com.github.jkschneider.tinkermapdb.graph.traversal.strategy.MapdbGraphStepStrategy;
import com.tinkerpop.gremlin.process.TraversalStrategies;
import com.tinkerpop.gremlin.process.computer.GraphComputer;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.ElementHelper;
import com.tinkerpop.gremlin.structure.util.StringFactory;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.Store;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
public class MapdbGraph implements Graph, Graph.Iterators {
    private static AtomicInteger instanceCounter = new AtomicInteger();

    static {
        try {
            TraversalStrategies.GlobalCache.registerStrategies(MapdbGraph.class, TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(MapdbGraphStepStrategy.instance()));
            TraversalStrategies.GlobalCache.registerStrategies(MapdbVertex.class, TraversalStrategies.GlobalCache.getStrategies(Vertex.class).clone().addStrategies(MapdbElementStepStrategy.instance()));
            TraversalStrategies.GlobalCache.registerStrategies(MapdbEdge.class, TraversalStrategies.GlobalCache.getStrategies(Edge.class).clone().addStrategies(MapdbElementStepStrategy.instance()));
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private static final Configuration DEFAULT_CONFIGURATION = new BaseConfiguration() {{
        this.setProperty(Graph.GRAPH, MapdbGraph.class.getName());
    }};

    private Configuration configuration = DEFAULT_CONFIGURATION;

    DB db = openDB();

    protected int instanceId = instanceCounter.incrementAndGet();

    protected Long currentId = -1l;

    private static class KryoSerializer<T> implements Serializer<T>, Serializable {
        transient Class<T> tClass;
        transient Kryo kryo = new Kryo() {{
            register(MapdbVertex.class, 0);
            register(MapdbEdge.class, 1);
            register(MapdbProperty.class, 2);
            setReferences(false);
        }};

        public KryoSerializer(Class<T> tClass) {
            this.tClass = tClass;
        }

        @Override
        public void serialize(DataOutput out, T t) throws IOException {
            UnsafeOutput o = new UnsafeOutput(new ByteArrayOutputStream());
            kryo.writeObject(o, t);
            byte[] buf = o.getBuffer();
            out.writeInt(buf.length);
            out.write(o.getBuffer());
            o.close();
        }

        @Override
        public T deserialize(DataInput in, int available) throws IOException {
            final int bufLength = in.readInt();
            UnsafeInput i = new UnsafeInput(new ByteArrayInputStream(new byte[bufLength]));
            T t = kryo.readObject(i, tClass);
            i.close();
            return t;
        }

        @Override
        public int fixedSize() {
            return -1;
        }
    }

    protected Map<Object, Vertex> vertices = db.createHashMap(getName() + "-vertices")
//            .keySerializer(Serializer.LONG)
            .valueSerializer(new KryoSerializer<>(MapdbVertex.class))
            .make();

    protected Map<Object, Edge> edges = db.createHashMap(getName() + "-edges")
//            .keySerializer(Serializer.LONG)
            .valueSerializer(new KryoSerializer<>(MapdbEdge.class))
            .make();

    protected MapdbIndex<MapdbVertex> vertexIndex = new MapdbIndex<>(this, MapdbVertex.class);
    protected MapdbIndex<MapdbEdge> edgeIndex = new MapdbIndex<>(this, MapdbEdge.class);

    protected MapdbGraphVariables variables = new MapdbGraphVariables(this, db);

    public static MapdbGraph open() {
        MapdbGraph g = new MapdbGraph();
        MapdbGraphRegistry.addGraph(g);
        return g;
    }

    public static MapdbGraph open(final Configuration configuration) {
        MapdbGraph g = new MapdbGraph();
        MapdbGraphRegistry.addGraph(g);

        CompositeConfiguration c = new CompositeConfiguration();
        c.addConfiguration(configuration);
        c.addConfiguration(DEFAULT_CONFIGURATION);
        g.configuration = c;

        return g;
    }

    AtomicInteger addCount = new AtomicInteger();

    @Override
    public Vertex addVertex(final Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Object idValue = ElementHelper.getIdValue(keyValues).orElse(null);
        final String label = ElementHelper.getLabelValue(keyValues).orElse(Vertex.DEFAULT_LABEL);

        if (null != idValue) {
            if (this.vertices.containsKey(idValue))
                throw Exceptions.vertexWithIdAlreadyExists(idValue);
        } else {
            idValue = MapdbHelper.getNextId(this);
        }

        final MapdbVertex vertex = new MapdbVertex(idValue, label, this);
        this.vertices.put(vertex.id(), vertex);
        ElementHelper.attachProperties(vertex, keyValues);

        if(addCount.incrementAndGet() % configuration.getInteger("storage.compactInterval", 1000) == 0) {
            db.compact();
            addCount.set(0);
        }

        return vertex;
    }

    @Override
    public GraphComputer compute(final Class... graphComputerClass) {
        throw Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(graphComputerClass[0]);
    }

    @Override
    public Variables variables() {
        return this.variables;
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + this.vertices.size() + " edges:" + this.edges.size());
    }

    private DB openDB() {
        return DBMaker
            .newMemoryDirectDB()
            .transactionDisable()
            .cacheLRUEnable()
            .cacheSize(configuration.getInt("storage.cacheSize", 1000))
            .make();
    }

    public void clear() {
        this.db.close();
        this.db = openDB();

        this.vertices.clear();
        this.edges.clear();
        this.variables = new MapdbGraphVariables(this, db);
        this.currentId = 0l;
        this.vertexIndex = new MapdbIndex<>(this, MapdbVertex.class);
        this.edgeIndex = new MapdbIndex<>(this, MapdbEdge.class);
    }

    @Override
    public void close() {
        db.close();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Configuration configuration() {
        return configuration;
    }

    @Override
    public Iterators iterators() {
        return this;
    }

    @Override
    public Iterator<Vertex> vertexIterator(final Object... vertexIds) {
        if (0 == vertexIds.length) {
            return this.vertices.values().iterator();
        } else if (1 == vertexIds.length) {
            final Vertex vertex = this.vertices.get(vertexIds[0]);
            return null == vertex ? Collections.emptyIterator() : IteratorUtils.of(vertex);
        } else
            return Stream.of(vertexIds).map(this.vertices::get).filter(Objects::nonNull).iterator();
    }

    @Override
    public Iterator<Edge> edgeIterator(final Object... edgeIds) {
        if (0 == edgeIds.length) {
            return this.edges.values().iterator();
        } else if (1 == edgeIds.length) {
            final Edge edge = this.edges.get(edgeIds[0]);
            return null == edge ? Collections.emptyIterator() : IteratorUtils.of(edge);
        } else
            return Stream.of(edgeIds).map(this.edges::get).filter(Objects::nonNull).iterator();
    }

    @Override
    public Features features() {
        return MapdbGraphFeatures.INSTANCE;
    }

    public static class MapdbGraphFeatures implements Features {

        static final MapdbGraphFeatures INSTANCE = new MapdbGraphFeatures();

        private MapdbGraphFeatures() {}

        @Override
        public GraphFeatures graph() {
            return MapdbGraphGraphFeatures.INSTANCE;
        }

        @Override
        public EdgeFeatures edge() {
            return MapdbGraphEdgeFeatures.INSTANCE;
        }

        @Override
        public VertexFeatures vertex() {
            return MapdbGraphVertexFeatures.INSTANCE;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public static class MapdbGraphVertexFeatures implements Features.VertexFeatures {
        static final MapdbGraphVertexFeatures INSTANCE = new MapdbGraphVertexFeatures();

        private MapdbGraphVertexFeatures() {}

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class MapdbGraphEdgeFeatures implements Features.EdgeFeatures {
        static final MapdbGraphEdgeFeatures INSTANCE = new MapdbGraphEdgeFeatures();

        private MapdbGraphEdgeFeatures(){}

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
    }

    public static class MapdbGraphGraphFeatures implements Features.GraphFeatures {
        static final MapdbGraphGraphFeatures INSTANCE = new MapdbGraphGraphFeatures();

        private MapdbGraphGraphFeatures() {}

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    ///////////// GRAPH SPECIFIC INDEXING METHODS ///////////////

    /**
     * Create an index for said element class ({@link com.tinkerpop.gremlin.structure.Vertex} or {@link com.tinkerpop.gremlin.structure.Edge}) and said property key.
     * Whenever an element has the specified key mutated, the index is updated.
     * When the index is created, all existing elements are indexed to ensure that they are captured by the index.
     *
     * @param key          the property key to index
     * @param elementClass the element class to index
     * @param <E>          The type of the element class
     */
    public <E extends Element> void createIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexIndex.createKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeIndex.createKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    /**
     * Drop the index for the specified element class ({@link com.tinkerpop.gremlin.structure.Vertex} or {@link com.tinkerpop.gremlin.structure.Edge}) and key.
     *
     * @param key          the property key to stop indexing
     * @param elementClass the element class of the index to drop
     * @param <E>          The type of the element class
     */
    public <E extends Element> void dropIndex(final String key, final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            this.vertexIndex.dropKeyIndex(key);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            this.edgeIndex.dropKeyIndex(key);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    public Store store() {
        return Store.forDB(db);
    }

    /**
     * Return all the keys currently being index for said element class  ({@link com.tinkerpop.gremlin.structure.Vertex} or {@link com.tinkerpop.gremlin.structure.Edge}).
     *
     * @param elementClass the element class to get the indexed keys for
     * @param <E>          The type of the element class
     * @return the set of keys currently being indexed
     */
    // FIXME this is not an efficient way to retrieve matching index keys
    public <E extends Element> Set<String> getIndexedKeys(final Class<E> elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return this.vertexIndex.getIndexedKeys();
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return this.edgeIndex.getIndexedKeys();
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    public String getName() {
        return "mapdbgraph-" + instanceId;
    }
}
