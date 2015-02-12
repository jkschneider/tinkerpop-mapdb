package io.jons.tinkerpop.mapdb;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.mapdb.Serializer;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

class KryoSerializer<T> extends Serializer<T> implements Serializable {
    transient Class<T> tClass;

    private transient static ThreadLocal<Kryo> kryoPool = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            return new Kryo() {{
                register(MapdbVertex.class, 100);
                register(MapdbEdge.class, 101);
                register(MapdbProperty.class, 102);
                register(MapdbVertexProperty.class, 103);
                register(ConcurrentHashMap.class, 104);
                register(HashMap.class, 105);
                setReferences(false);
            }};
        }
    };

    public KryoSerializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    @Override
    public void serialize(DataOutput out, T t) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream(4096);
        Output o = new Output(os);
        kryoPool.get().writeObject(o, t);
        o.flush();
        out.writeInt((int) o.total());
        out.write(os.toByteArray(), 0, (int) o.total());
        o.close();
    }

    @Override
    public T deserialize(DataInput in, int available) throws IOException {
        final int bufLength = in.readInt();
        byte[] buf = new byte[bufLength];
        in.readFully(buf);
        ByteArrayInputStream is = new ByteArrayInputStream(buf);
        Input i = new Input(is);
        T t = kryoPool.get().readObject(i, tClass);
        i.close();
        return t;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}